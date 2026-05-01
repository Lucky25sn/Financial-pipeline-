# Financial Data Automation Pipeline

A Python pipeline that ingests messy financial data (CSV, Excel, JSON),
cleans it, validates it, and outputs a clean dataset + HTML dashboard.

---

## Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the pipeline (processes all files in data/input/)
```bash
python pipeline.py
```

### 3. Process a single file
```bash
python pipeline.py --file data/input/transactions.csv
```

### 4. Watch mode — auto-processes new files dropped into data/input/
```bash
python pipeline.py --watch
```

---

## Project Structure

```
financial_pipeline/
├── pipeline.py               ← Main script
├── requirements.txt
├── README.md
├── data/
│   ├── input/
│   │   ├── transactions.csv  ← Sample messy transaction data
│   │   ├── trades.csv        ← Sample trade records
│   │   └── payments.csv      ← Sample payment data
│   └── output/
│       ├── *_valid.csv       ← Clean, valid records
│       ├── *_rejected.csv    ← Rejected records with reasons
│       ├── *_full.csv        ← All records with status
│       ├── *_report.json     ← Run summary
│       └── dashboard.html    ← Open this in your browser!
└── templates/
    └── dashboard.html        ← Dashboard template
```

---

## Validation Rules

| Rule                  | Description                                      |
|-----------------------|--------------------------------------------------|
| amount_positive       | Transaction amount must be > 0                   |
| account_id_present    | No missing or blank account IDs                  |
| valid_date            | Date must parse correctly and not be in future   |
| valid_currency        | Currency must be a known 3-letter ISO code       |
| valid_type            | Type must be: debit/credit/transfer/trade/etc.   |
| no_duplicate          | Transaction ID must be unique within the file    |

---

## Supported Column Names

The pipeline auto-detects and normalises these column names:

| Standard    | Accepted variants                                    |
|-------------|------------------------------------------------------|
| id          | transaction_id, trade_id, payment_id, txn_id         |
| account_id  | account_id, account, payer_id, client_id             |
| date        | date, trade_date, payment_date, value_date           |
| amount      | amount, value, notional, net_amount, gross_amount    |
| currency    | currency, ccy, curr                                  |
| type        | type, transaction_type, trade_type, payment_type     |
| description | description, desc, reference, notes, instrument      |

---

## Adding Your Own Data

Drop any `.csv`, `.xlsx`, or `.json` file into `data/input/` and run the pipeline.
The pipeline handles:
- Mixed date formats (2024-01-15, 2024/01/15, 01/15/2024)
- Mixed-case currencies (usd → USD)
- Comma-formatted numbers ("1,500.00")
- Currency symbols in amounts ($1500, R1500)
- Blank/NaN values
- Duplicate IDs

---

## Viewing the Dashboard

After running, open the generated file in your browser:
```
data/output/dashboard.html
```

Or use VS Code's Live Server extension to serve it:
Right-click `dashboard.html` → "Open with Live Server"
