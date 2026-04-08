# Gatwick Affiliate Reconciliation

Automates monthly Rakuten affiliate reconciliation for Gatwick Airport parking.

## What it does

1. Accepts pasted Rakuten inquiry data (full sheet or raw IDs)
2. Cleans and normalises Order IDs using known patterns
3. Pushes cleaned IDs to BigQuery (dated table per month)
4. Runs reconciliation SQL against GHP + Main site GA4 tables
5. Writes matched/unmatched results to a new Google Sheets tab
6. Shows per-run summary in the browser

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Credentials

Place your GCP service account key at:
```
~/gatwick-reconciliation/credentials.json
```

The service account needs:
- BigQuery Data Editor + Job User on `spa-link-audit-305308`
- Google Sheets API read/write access
- Drive API read/write (for creating new sheet tabs)

### 3. Run locally

```bash
python app.py
```

App starts at http://localhost:5000

---

## ID cleaning rules

| Input | Output | Rule |
|---|---|---|
| `GHP-DW-1628713_1` | `GHP-DW-1628713` | Strip `_N` suffix |
| `DW09926523ZAN_1` | `DW09926523ZAN` | Strip `_N` suffix |
| `LGW-DW-11176152ANN` | `DW11176152ANN` | LGW + trailing 3 letters → main site |
| `LGW-DW-08614442` | `GHP-DW-08614442` | LGW + no trailing letters → GHP |
| `LG-DW-10905890` | `GHP-DW-10905890` | LG + no trailing letters → GHP |
| `GHP-DW-1662491` | `GHP-DW-1662491` | Pass through |
| `DW10316811TIM` | `DW10316811TIM` | Pass through |
| `DFP183373420` | — | Flagged for manual review |

---

## Project structure

```
gatwick-reconciliation/
├── app.py              # Flask app + API routes
├── cleaner.py          # ID cleaning logic
├── bq_runner.py        # BigQuery push + reconciliation queries
├── sheets_writer.py    # Google Sheets output
├── credentials.json    # GCP service account (gitignored)
├── requirements.txt
├── templates/
│   └── index.html      # Web UI
├── tests/
│   └── test_cleaner.py
└── runs.log            # Auto-generated run history
```

---

## BigQuery tables

| Table | Description |
|---|---|
| `analytics_343350290` | GHP (Gatwick Holiday Parking) |
| `analytics_256226149` | Main site |
| `inquiry_YYYYMM_monthname` | Cleaned IDs uploaded each run |

Each monthly run creates a new dated table (e.g. `inquiry_202603_march`) — old tables are preserved for dispute resolution.

---

## Google Sheets output

- Sheet ID: `1ir7kqEqlsnWyDkIEQGoCVqzXjMRhdd8PCAVl9rYXJEY`
- New tab created per run: `Output_March2026`, `Output_April2026` etc.
- Status column populated: `Matched` / `Unmatched`
- If Sheets write fails: CSV saved locally as `output_march2026.csv`

---

## Running tests

```bash
python -m pytest tests/ -v
```

---

## Deploy to Cloud Run (future)

```bash
# Build image
gcloud builds submit --tag gcr.io/spa-link-audit-305308/gatwick-recon

# Deploy
gcloud run deploy gatwick-recon \
  --image gcr.io/spa-link-audit-305308/gatwick-recon \
  --platform managed \
  --region europe-west2 \
  --allow-unauthenticated
```

Add `credentials.json` as a Cloud Run secret and update the path in `bq_runner.py` and `sheets_writer.py`.
