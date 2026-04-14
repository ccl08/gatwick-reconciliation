"""
Flask app for Gatwick affiliate reconciliation.
"""

import csv
import io
import json
import logging
import os
from datetime import datetime, date

from flask import Flask, jsonify, render_template, request

from cleaner import clean_order_ids, format_for_bq, summarise, CleanStatus

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("runs.log"),
    ],
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.urandom(24)

REQUIRED_FIELDS = {"Order ID"}

# Possible names for the Rakuten transaction-status column, checked in order.
# The first match (case-insensitive) wins.
_STATUS_COLUMN_CANDIDATES = [
    "Status",
    "Transaction Status",
    "Approval Status",
    "Txn Status",
    "Order Status",
]


def _find_status_key(row: dict) -> str | None:
    """Return the actual key in *row* that represents the Rakuten status column.

    Tries each candidate name with a case-insensitive match against the row's
    keys.  Returns the key as it appears in the dict, or None.
    """
    # Build a lowercase → original-key lookup (once per call, rows are small)
    lower_map = {k.lower().strip(): k for k in row if not k.startswith("_")}
    for candidate in _STATUS_COLUMN_CANDIDATES:
        real_key = lower_map.get(candidate.lower())
        if real_key is not None:
            return real_key
    return None


def _get_status_value(row: dict) -> str:
    """Extract the Rakuten transaction-status value from *row*, tolerating
    column-name variations and case differences.  Returns '' if not found."""
    key = _find_status_key(row)
    return row.get(key, "") if key else ""


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

EXPECTED_COLUMNS = [
    "Status",
    "Inquiry #",
    "Date of Inquiry",
    "Site ID",
    "Publisher",
    "Transaction Date",
    "Order ID",
    "Requested Order Amount",
    "Publisher Comments",
    "Member ID",
    "SKU",
    "Resolution",
    "Comment",
]


def parse_tsv_or_csv(text: str) -> list[dict]:
    """
    Parse pasted tab-separated or comma-separated data.
    First row is treated as headers.
    """
    # Strip BOM (common in Excel CSV exports) so the first column header is clean
    text = text.lstrip("\ufeff").strip()
    if not text:
        return []

    # Detect delimiter: more tabs than commas → TSV
    first_line = text.splitlines()[0]
    delimiter = "\t" if first_line.count("\t") >= first_line.count(",") else ","

    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    rows = []
    for row in reader:
        # Normalise keys: strip whitespace
        cleaned = {k.strip(): v.strip() if v else "" for k, v in row.items()}
        rows.append(cleaned)
    return rows


def parse_raw_ids(text: str) -> list[dict]:
    """
    Parse raw IDs (one per line, or comma/space separated).
    Returns minimal rows with only Order ID set.
    """
    text = text.strip()
    ids = []
    for line in text.splitlines():
        for part in line.replace(",", " ").split():
            part = part.strip()
            if part:
                ids.append(part)
    return [{"Order ID": id_} for id_ in ids]


def detect_date_range(rows: list[dict]) -> tuple[str | None, str | None]:
    """
    Extract min and max Transaction Date from rows.
    Returns (date_from, date_to) as YYYYMMDD strings, or (None, None).
    """
    dates = []
    for row in rows:
        raw = row.get("Transaction Date", "").strip()
        if not raw:
            continue
        # Try common date formats
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%Y%m%d"):
            try:
                d = datetime.strptime(raw, fmt).date()
                dates.append(d)
                break
            except ValueError:
                continue

    if not dates:
        return None, None

    return min(dates).strftime("%Y%m%d"), max(dates).strftime("%Y%m%d")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/preview", methods=["POST"])
def preview():
    """
    Parse and clean submitted data. Returns preview without running BQ.
    """
    data = request.get_json(force=True)
    raw_text = data.get("raw_text", "").strip()
    input_mode = data.get("input_mode", "auto")  # 'auto', 'tsv', 'raw_ids'

    if not raw_text:
        return jsonify({"error": "No data provided"}), 400

    # Parse rows
    rows = []
    if input_mode == "raw_ids":
        rows = parse_raw_ids(raw_text)
    else:
        # Try structured first; fall back to raw IDs
        rows = parse_tsv_or_csv(raw_text)
        if not rows or "Order ID" not in rows[0]:
            rows = parse_raw_ids(raw_text)

    if not rows:
        return jsonify({"error": "Could not parse any rows from input"}), 400

    # Clean IDs
    raw_ids = [row.get("Order ID", "") for row in rows]
    clean_results = clean_order_ids(raw_ids)
    summary = summarise(clean_results)

    # Detect date range
    date_from, date_to = detect_date_range(rows)
    needs_manual_dates = date_from is None

    # Attach cleaned IDs back to rows
    for row, result in zip(rows, clean_results):
        row["_original_id"] = result.original
        row["_cleaned_id"] = result.cleaned
        row["_clean_status"] = result.status
        row["_clean_note"] = result.note

    return jsonify({
        "rows": rows,
        "summary": {
            "total": summary["total"],
            "clean": summary["clean"],
            "auto_fixed": summary["auto_fixed"],
            "flagged": summary["flagged"],
        },
        "auto_fixed_items": [
            {"original": r.original, "cleaned": r.cleaned, "note": r.note}
            for r in summary["auto_fixed_items"]
        ],
        "flagged_items": [
            {"original": r.original, "cleaned": r.cleaned, "note": r.note}
            for r in summary["flagged_items"]
        ],
        "date_from": date_from,
        "date_to": date_to,
        "needs_manual_dates": needs_manual_dates,
    })


@app.route("/api/submit", methods=["POST"])
def submit():
    """
    Run the full pipeline: push to BQ, run reconciliation, write to Sheets.
    """
    data = request.get_json(force=True)

    rows = data.get("rows", [])
    date_from = data.get("date_from", "").strip()
    date_to = data.get("date_to", "").strip()
    manual_overrides = data.get("manual_overrides", {})  # {original_id: corrected_id}
    lookback_days = int(data.get("lookback_days", 90))

    if not rows:
        return jsonify({"error": "No rows to process"}), 400

    if not date_from or not date_to:
        return jsonify({"error": "Date range is required"}), 400

    # Apply manual overrides to flagged IDs
    for row in rows:
        orig = row.get("_original_id", "")
        if orig in manual_overrides:
            row["_cleaned_id"] = manual_overrides[orig]
            row["_clean_status"] = "manual"

    # Collect final cleaned IDs (skip still-flagged if desired)
    transaction_ids = [
        row["_cleaned_id"]
        for row in rows
        if row.get("_cleaned_id") and row.get("_clean_status") != CleanStatus.FLAGGED
    ]

    # Also include flagged IDs that were overridden manually
    for row in rows:
        if row.get("_clean_status") == "manual" and row.get("_cleaned_id"):
            if row["_cleaned_id"] not in transaction_ids:
                transaction_ids.append(row["_cleaned_id"])

    if not transaction_ids:
        return jsonify({"error": "No valid IDs to submit after cleaning"}), 400

    run_date = datetime.utcnow()
    logger.info(
        f"Run started at {run_date.isoformat()} | "
        f"IDs: {len(transaction_ids)} | "
        f"Date range: {date_from} – {date_to}"
    )

    # Import here to avoid loading heavy deps during tests
    try:
        from bq_runner import push_ids_to_bq, run_reconciliation
        from sheets_writer import write_results
    except ImportError as e:
        return jsonify({"error": f"Missing dependency: {e}"}), 500

    # Step 1: Push IDs to BQ
    try:
        bq_table = push_ids_to_bq(transaction_ids, run_date=run_date)
    except Exception as e:
        logger.error(f"BQ push failed: {e}")
        return jsonify({"error": f"Failed to push IDs to BigQuery: {e}"}), 500

    # Step 2: Run reconciliation
    try:
        matched_rows = run_reconciliation(date_from, date_to, run_date=run_date, lookback_days=lookback_days)
    except Exception as e:
        logger.error(f"BQ reconciliation failed: {e}")
        return jsonify({"error": f"BigQuery reconciliation failed: {e}"}), 500

    matched_ids = {r["transaction_id"] for r in matched_rows}

    # --- Diagnostic logging: dump the first row so we can see every column ---
    if rows:
        first = rows[0]
        visible_keys = {k: v for k, v in first.items() if not k.startswith("_")}
        logger.info("DIAG first row keys+values: %s", visible_keys)
        status_key = _find_status_key(first)
        logger.info(
            "DIAG detected status column: %s (value=%r)",
            status_key,
            first.get(status_key, "<N/A>") if status_key else "<NOT FOUND>",
        )

    # Build lookup from cleaned ID → original Rakuten transaction status.
    # Key on BOTH the cleaned ID and the original Order ID so lookups work
    # regardless of which form is used.
    id_to_txn_status = {}
    for row in rows:
        rakuten_status = _get_status_value(row)
        cleaned = row.get("_cleaned_id", "")
        original = row.get("Order ID", "").strip()
        if cleaned:
            id_to_txn_status[cleaned] = rakuten_status
        if original and original != cleaned:
            id_to_txn_status[original] = rakuten_status

    logger.info(
        "DIAG id_to_txn_status first 5: %s",
        list(id_to_txn_status.items())[:5],
    )
    logger.info(
        "DIAG matched_rows first 5 transaction_ids: %s",
        [r["transaction_id"] for r in matched_rows[:5]],
    )

    # Enrich matched_rows with the Rakuten transaction status
    for mr in matched_rows:
        mr["transaction_status"] = id_to_txn_status.get(mr["transaction_id"], "")

    # Step 3: Write to Sheets
    sheets_error = None
    tab_name = None
    sheet_url = None
    try:
        # Strip internal fields before writing
        clean_rows = [
            {k: v for k, v in row.items() if not k.startswith("_")}
            for row in rows
        ]

        # Build a mapping from Order ID → cleaned ID so write_results can
        # match original Order IDs against the cleaned matched_ids set,
        # and from Order ID → Rakuten transaction status.
        order_id_to_cleaned = {}
        order_id_to_status = {}
        for row in rows:
            orig = row.get("Order ID", "").strip()
            if orig:
                order_id_to_cleaned[orig] = row.get("_cleaned_id", orig)
                order_id_to_status[orig] = _get_status_value(row)

        logger.info("DIAG order_id_to_status first 5: %s", list(order_id_to_status.items())[:5])

        tab_name, sheet_url = write_results(
            clean_rows,
            matched_ids,
            run_date=run_date,
            order_id_to_cleaned=order_id_to_cleaned,
            order_id_to_status=order_id_to_status,
        )
    except Exception as e:
        sheets_error = str(e)
        logger.error(f"Sheets write failed: {e}")

    matched_count = len(matched_ids)
    unmatched_count = len(transaction_ids) - matched_count

    logger.info(
        f"Run complete | Matched: {matched_count} | Unmatched: {unmatched_count} | "
        f"Sheet tab: {tab_name or 'FAILED'}"
    )

    result = {
        "success": True,
        "submitted": len(transaction_ids),
        "matched": matched_count,
        "unmatched": unmatched_count,
        "tab_name": tab_name,
        "sheet_url": sheet_url,
        "bq_table": bq_table,
        "matched_rows": matched_rows[:500],  # cap payload size
    }

    if sheets_error:
        result["sheets_warning"] = sheets_error

    return jsonify(result)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    app.run(debug=False, host="0.0.0.0", port=port)
