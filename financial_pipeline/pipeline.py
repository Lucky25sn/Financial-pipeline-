"""
Financial Data Automation Pipeline
===================================
Run:  python pipeline.py
      python pipeline.py --file data/input/trades.csv
      python pipeline.py --watch   (auto-process new files in data/input/)
"""

import os
import sys
import json
import time
import shutil
import hashlib
import argparse
import logging
from pathlib import Path
from datetime import datetime, date

import pandas as pd
import numpy as np

# ─── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent
INPUT_DIR   = BASE_DIR / "data" / "input"
OUTPUT_DIR  = BASE_DIR / "data" / "output"
REPORT_DIR  = BASE_DIR / "data" / "output"
TEMPLATE    = BASE_DIR / "templates" / "dashboard.html"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pipeline")

# ─── Column normalisation map ─────────────────────────────────────────────────
COLUMN_MAP = {
    "id":          ["transaction_id", "trade_id", "payment_id", "id", "txn_id"],
    "account_id":  ["account_id", "account", "payer_id", "client_id", "acc"],
    "date":        ["date", "trade_date", "payment_date", "transaction_date", "value_date"],
    "amount":      ["amount", "value", "notional", "net_amount", "gross_amount"],
    "currency":    ["currency", "ccy", "curr"],
    "type":        ["type", "transaction_type", "trade_type", "payment_type", "category"],
    "description": ["description", "desc", "reference", "notes", "instrument"],
}

VALID_CURRENCIES = {
    "ZAR","USD","EUR","GBP","JPY","CHF","AUD","CAD","CNY","HKD",
    "SGD","NZD","SEK","NOK","DKK","MXN","BRL","INR","KRW","TRY",
}

VALID_TYPES = {"debit","credit","transfer","trade","payment","buy","sell","fee"}

# ─── Validation rules ─────────────────────────────────────────────────────────
def amount_positive(row):
    if pd.isna(row["amount_clean"]) or row["amount_clean"] <= 0:
        return "Amount must be positive"

def account_id_present(row):
    val = str(row.get("account_id_clean", "")).strip()
    if not val or val.lower() in ("nan", "none", ""):
        return "Missing account ID"

def valid_date(row):
    if pd.isna(row["date_clean"]):
        return "Invalid or unparseable date"
    if row["date_clean"] > date.today():
        return "Date is in the future"

def valid_currency(row):
    c = str(row.get("currency_clean", "")).strip().upper()
    if c not in VALID_CURRENCIES:
        return f"Invalid currency code '{c}'"

def valid_type(row):
    t = str(row.get("type_clean", "")).strip().lower()
    if t not in VALID_TYPES:
        return f"Invalid transaction type '{t}'"

RULES = [
    ("amount_positive",    amount_positive),
    ("account_id_present", account_id_present),
    ("valid_date",         valid_date),
    ("valid_currency",     valid_currency),
    ("valid_type",         valid_type),
]

# ─── Ingestion ────────────────────────────────────────────────────────────────
def ingest(filepath: Path) -> pd.DataFrame:
    log.info(f"Ingesting  →  {filepath.name}")
    ext = filepath.suffix.lower()
    if ext == ".csv":
        df = pd.read_csv(filepath, dtype=str)
    elif ext in (".xlsx", ".xls"):
        df = pd.read_excel(filepath, dtype=str)
    elif ext == ".json":
        df = pd.read_json(filepath, dtype=str)
    else:
        raise ValueError(f"Unsupported file type: {ext}")
    log.info(f"  Loaded {len(df)} rows, {len(df.columns)} columns")
    return df

# ─── Cleaning ─────────────────────────────────────────────────────────────────
def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Map varied column names to standard names."""
    col_lower = {c.lower().strip(): c for c in df.columns}
    mapping = {}
    for std_name, candidates in COLUMN_MAP.items():
        for candidate in candidates:
            if candidate in col_lower:
                mapping[col_lower[candidate]] = std_name
                break
    df = df.rename(columns=mapping)
    log.info(f"  Column mapping: {mapping}")
    return df

def clean_amounts(df: pd.DataFrame) -> pd.DataFrame:
    if "amount" in df.columns:
        df["amount_clean"] = (
            df["amount"].astype(str)
            .str.replace(r"[,\s$£€¥R]", "", regex=True)
            .str.replace(r"\((.+)\)", r"-\1", regex=True)
        )
        df["amount_clean"] = pd.to_numeric(df["amount_clean"], errors="coerce")
    else:
        df["amount_clean"] = np.nan
    return df

def clean_dates(df: pd.DataFrame) -> pd.DataFrame:
    if "date" in df.columns:
        df["date_clean"] = pd.to_datetime(
            df["date"].astype(str).str.strip(),
            dayfirst=False, errors="coerce"
        ).dt.date
    else:
        df["date_clean"] = pd.NaT
    return df

def clean_currencies(df: pd.DataFrame) -> pd.DataFrame:
    if "currency" in df.columns:
        df["currency_clean"] = df["currency"].astype(str).str.strip().str.upper()
    else:
        df["currency_clean"] = "UNKNOWN"
    return df

def clean_types(df: pd.DataFrame) -> pd.DataFrame:
    if "type" in df.columns:
        df["type_clean"] = df["type"].astype(str).str.strip().str.lower()
    else:
        df["type_clean"] = "unknown"
    return df

def clean_account_ids(df: pd.DataFrame) -> pd.DataFrame:
    if "account_id" in df.columns:
        df["account_id_clean"] = df["account_id"].astype(str).str.strip()
        df.loc[df["account_id_clean"].isin(["nan","None",""]), "account_id_clean"] = ""
    else:
        df["account_id_clean"] = ""
    return df

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    if "id" in df.columns:
        before = len(df)
        df["_is_duplicate"] = df.duplicated(subset=["id"], keep="first")
        dupes = df["_is_duplicate"].sum()
        if dupes:
            log.warning(f"  Found {dupes} duplicate transaction IDs")
        else:
            log.info("  No duplicates found")
    else:
        df["_is_duplicate"] = False
    return df

def clean(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Cleaning data...")
    df = normalise_columns(df)
    df = clean_amounts(df)
    df = clean_dates(df)
    df = clean_currencies(df)
    df = clean_types(df)
    df = clean_account_ids(df)
    df = remove_duplicates(df)
    log.info(f"  Cleaning complete")
    return df

# ─── Validation ───────────────────────────────────────────────────────────────
def validate(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Running validation rules...")
    df["_errors"] = [[] for _ in range(len(df))]

    # Duplicate check (uses flag set during clean)
    dup_mask = df.get("_is_duplicate", pd.Series(False, index=df.index))
    for idx in df[dup_mask].index:
        df.at[idx, "_errors"] = df.at[idx, "_errors"] + ["Duplicate transaction ID"]

    # Rule-based checks
    for rule_name, rule_fn in RULES:
        passes = fails = 0
        for idx, row in df.iterrows():
            error = rule_fn(row)
            if error:
                df.at[idx, "_errors"] = df.at[idx, "_errors"] + [error]
                fails += 1
            else:
                passes += 1
        log.info(f"  [{rule_name}]  pass={passes}  fail={fails}")

    df["_status"] = df["_errors"].apply(lambda e: "rejected" if e else "valid")
    df["_error_summary"] = df["_errors"].apply(lambda e: "; ".join(e) if e else "")

    valid   = (df["_status"] == "valid").sum()
    rejected = (df["_status"] == "rejected").sum()
    log.info(f"  Validation complete → {valid} valid, {rejected} rejected")
    return df

# ─── Output ───────────────────────────────────────────────────────────────────
def write_outputs(df: pd.DataFrame, source_name: str) -> dict:
    ts    = datetime.now().strftime("%Y%m%d_%H%M%S")
    stem  = f"{source_name}_{ts}"

    clean_cols = [c for c in df.columns if not c.startswith("_")]

    valid_df    = df[df["_status"] == "valid"][clean_cols + ["_status"]]
    rejected_df = df[df["_status"] == "rejected"][clean_cols + ["_status", "_error_summary"]]
    full_df     = df[clean_cols + ["_status", "_error_summary"]]

    valid_path    = OUTPUT_DIR / f"{stem}_valid.csv"
    rejected_path = OUTPUT_DIR / f"{stem}_rejected.csv"
    full_path     = OUTPUT_DIR / f"{stem}_full.csv"

    valid_df.to_csv(valid_path, index=False)
    rejected_df.to_csv(rejected_path, index=False)
    full_df.to_csv(full_path, index=False)

    log.info(f"  Saved: {valid_path.name}")
    log.info(f"  Saved: {rejected_path.name}")
    log.info(f"  Saved: {full_path.name}")

    return {
        "valid":    str(valid_path),
        "rejected": str(rejected_path),
        "full":     str(full_path),
    }

# ─── Report ───────────────────────────────────────────────────────────────────
def build_report(df: pd.DataFrame, source_name: str, output_paths: dict) -> dict:
    total    = len(df)
    valid    = int((df["_status"] == "valid").sum())
    rejected = int((df["_status"] == "rejected").sum())
    quality  = round(valid / total * 100, 1) if total else 0

    err_counts = {}
    for errs in df["_errors"]:
        for e in errs:
            err_counts[e] = err_counts.get(e, 0) + 1

    currency_totals = {}
    if "currency_clean" in df.columns and "amount_clean" in df.columns:
        for _, row in df[df["_status"] == "valid"].iterrows():
            ccy = str(row.get("currency_clean", "UNKNOWN"))
            amt = row.get("amount_clean", 0)
            if pd.notna(amt):
                currency_totals[ccy] = round(currency_totals.get(ccy, 0) + float(amt), 2)

    report = {
        "source":          source_name,
        "run_at":          datetime.now().isoformat(),
        "total_records":   total,
        "valid_records":   valid,
        "rejected_records": rejected,
        "quality_rate_pct": quality,
        "error_breakdown": err_counts,
        "currency_totals": currency_totals,
        "output_files":    output_paths,
    }

    report_path = OUTPUT_DIR / f"{source_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_report.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)
    log.info(f"  Report: {report_path.name}")

    return report

def print_summary(report: dict):
    print("\n" + "═"*52)
    print(f"  PIPELINE REPORT  —  {report['source']}")
    print("═"*52)
    print(f"  Total records   : {report['total_records']}")
    print(f"  ✓  Valid        : {report['valid_records']}")
    print(f"  ✗  Rejected     : {report['rejected_records']}")
    print(f"  Quality rate    : {report['quality_rate_pct']}%")
    if report["error_breakdown"]:
        print("\n  Rejection reasons:")
        for reason, count in sorted(report["error_breakdown"].items(), key=lambda x: -x[1]):
            print(f"    [{count:>3}]  {reason}")
    if report["currency_totals"]:
        print("\n  Valid totals by currency:")
        for ccy, total in sorted(report["currency_totals"].items()):
            print(f"    {ccy:<5}  {total:>14,.2f}")
    print("═"*52 + "\n")

# ─── Dashboard HTML ───────────────────────────────────────────────────────────
def generate_dashboard(reports: list):
    """Write a self-contained HTML dashboard from all run reports."""
    html_path = OUTPUT_DIR / "dashboard.html"
    template_path = TEMPLATE

    with open(template_path) as f:
        template = f.read()

    html = template.replace("__REPORTS_JSON__", json.dumps(reports, default=str))
    with open(html_path, "w") as f:
        f.write(html)
    log.info(f"  Dashboard → {html_path}")
    return html_path

# ─── Main pipeline ────────────────────────────────────────────────────────────
def run_file(filepath: Path) -> dict:
    log.info(f"\n{'─'*50}")
    log.info(f"Processing: {filepath.name}")
    df        = ingest(filepath)
    df        = clean(df)
    df        = validate(df)
    paths     = write_outputs(df, filepath.stem)
    report    = build_report(df, filepath.stem, paths)
    print_summary(report)
    return report

def run_all(input_dir: Path) -> list:
    files   = list(input_dir.glob("*.csv")) + list(input_dir.glob("*.xlsx")) + list(input_dir.glob("*.json"))
    if not files:
        log.warning(f"No files found in {input_dir}")
        return []
    reports = [run_file(f) for f in files]
    html    = generate_dashboard(reports)
    print(f"\n  Open your dashboard:  {html}\n")
    return reports

def watch_mode(input_dir: Path):
    """Watch input folder and process new files automatically."""
    log.info(f"Watch mode active — monitoring {input_dir}")
    log.info("Drop any CSV/XLSX/JSON file into data/input/ to trigger the pipeline.")
    log.info("Press Ctrl+C to stop.\n")
    seen = set(f.name for f in input_dir.iterdir())
    try:
        while True:
            time.sleep(2)
            current = set(f.name for f in input_dir.iterdir())
            new_files = current - seen
            for fname in new_files:
                fp = input_dir / fname
                if fp.suffix.lower() in (".csv", ".xlsx", ".json"):
                    log.info(f"New file detected: {fname}")
                    time.sleep(0.5)
                    try:
                        report = run_file(fp)
                        generate_dashboard([report])
                    except Exception as e:
                        log.error(f"Failed to process {fname}: {e}")
            seen = current
    except KeyboardInterrupt:
        log.info("Watch mode stopped.")

# ─── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Financial Data Pipeline")
    parser.add_argument("--file",  type=str, help="Process a single file")
    parser.add_argument("--watch", action="store_true", help="Watch input folder for new files")
    args = parser.parse_args()

    if args.watch:
        watch_mode(INPUT_DIR)
    elif args.file:
        fp = Path(args.file)
        if not fp.exists():
            log.error(f"File not found: {fp}")
            sys.exit(1)
        report = run_file(fp)
        generate_dashboard([report])
    else:
        run_all(INPUT_DIR)
