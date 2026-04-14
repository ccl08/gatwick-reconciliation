"""
Google Sheets writer for Gatwick reconciliation output.
Creates a new tab named Output_[Month][Year] and populates Status column.
"""

import csv
import io
import logging
import os
from datetime import datetime

import gspread
from google.oauth2 import service_account

logger = logging.getLogger(__name__)

SHEET_ID = "1ir7kqEqlsnWyDkIEQGoCVqzXjMRhdd8PCAVl9rYXJEY"
CREDENTIALS_PATH = "/Users/chriscespedes/gatwick-reconciliation/credentials.json"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _get_client() -> gspread.Client:
    if os.path.exists(CREDENTIALS_PATH):
        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIALS_PATH, scopes=SCOPES
        )
    else:
        # On Cloud Run the service account is attached to the instance — use ADC
        import google.auth
        credentials, _ = google.auth.default(scopes=SCOPES)
    return gspread.authorize(credentials)


def _tab_name(run_date: datetime | None = None) -> str:
    """Return tab name like Output_20260330_1322."""
    d = run_date or datetime.utcnow()
    return d.strftime("Output_%Y%m%d_%H%M")


def write_results(
    input_rows: list[dict],
    matched_ids: set[str],
    run_date: datetime | None = None,
) -> str:
    """
    Write reconciliation results to Google Sheets.

    - Creates a new tab named Output_[Month][Year]
    - Copies all original Rakuten columns
    - Populates the Status column: Matched / Unmatched

    Returns the tab name written to, or raises RuntimeError on failure.
    Falls back to CSV if Sheets write fails.
    """
    tab = _tab_name(run_date)

    # Build output rows — preserve original Rakuten "Status" as
    # "Transaction Status" and add a separate "Match Status" column.
    output_rows = []
    for row in input_rows:
        order_id = row.get("Order ID", "").strip()
        match_status = "Matched" if order_id in matched_ids else "Unmatched"
        out = dict(row)
        # Preserve original Rakuten status (e.g. "Live Transaction", "On Hold", "Cancellation")
        out["Transaction Status"] = row.get("Status", "")
        out["Match Status"] = match_status
        # Remove the original "Status" key to avoid ambiguity
        out.pop("Status", None)
        output_rows.append(out)

    logger.info(f"write_results: sheet_id={SHEET_ID} tab={tab} rows={len(output_rows)}")
    logger.info(f"write_results: credentials={CREDENTIALS_PATH}")

    try:
        logger.info("write_results: [1/4] authenticating with service account")
        client = _get_client()

        logger.info(f"write_results: [2/4] opening spreadsheet by key: {SHEET_ID}")
        spreadsheet = client.open_by_key(SHEET_ID)
        logger.info(f"write_results: spreadsheet opened — title='{spreadsheet.title}'")

        # Delete existing tab with same name if it exists (re-run protection)
        logger.info(f"write_results: [3/4] checking for existing tab '{tab}'")
        try:
            existing = spreadsheet.worksheet(tab)
            spreadsheet.del_worksheet(existing)
            logger.info(f"write_results: deleted existing tab '{tab}'")
        except gspread.exceptions.WorksheetNotFound:
            logger.info(f"write_results: tab '{tab}' does not exist yet — will create")

        logger.info(f"write_results: [4/4] creating tab '{tab}' with {len(output_rows)+10} rows")
        worksheet = spreadsheet.add_worksheet(title=tab, rows=len(output_rows) + 10, cols=20)

        if not output_rows:
            worksheet.update("A1", [["No data to write"]])
            logger.info("write_results: no output rows — wrote placeholder")
            sheet_url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit#gid={worksheet.id}"
            return tab, sheet_url

        headers = list(output_rows[0].keys())
        # Ensure Match Status and Transaction Status are the first two columns
        for col in ("Transaction Status", "Match Status"):
            if col in headers:
                headers.remove(col)
        headers = ["Match Status", "Transaction Status"] + headers

        data = [headers]
        for row in output_rows:
            data.append([str(row.get(h, "")) for h in headers])

        logger.info(f"write_results: writing {len(data)} rows (incl. header) to A1")
        worksheet.update("A1", data)
        sheet_url = (
            f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
            f"/edit#gid={worksheet.id}"
        )
        logger.info(f"write_results: success — {len(output_rows)} rows written to tab '{tab}' ({sheet_url})")
        return tab, sheet_url

    except gspread.exceptions.SpreadsheetNotFound:
        logger.error(f"write_results: FAILED — spreadsheet not found: {SHEET_ID}")
        logger.error("  → Share the sheet with the service account as Editor:")
        logger.error(f"  → gatwick-reconciliation@spa-link-audit-305308.iam.gserviceaccount.com")
        csv_path = _save_csv_fallback(output_rows, run_date)
        raise RuntimeError(
            f"Spreadsheet not found or service account has no access (sheet_id={SHEET_ID}). "
            f"Share it with gatwick-reconciliation@spa-link-audit-305308.iam.gserviceaccount.com as Editor. "
            f"Fallback CSV: {csv_path}"
        ) from None
    except gspread.exceptions.APIError as e:
        status = e.response.status_code if hasattr(e, 'response') else '?'
        logger.error(f"write_results: FAILED — Sheets API error {status}: {e}")
        csv_path = _save_csv_fallback(output_rows, run_date)
        raise RuntimeError(f"Sheets API error {status}: {e}. Fallback CSV: {csv_path}") from e
    except Exception as e:
        logger.error(f"write_results: FAILED — {type(e).__name__}: {e}", exc_info=True)
        csv_path = _save_csv_fallback(output_rows, run_date)
        raise RuntimeError(
            f"Google Sheets write failed ({type(e).__name__}: {e}). Fallback CSV: {csv_path}"
        ) from e


def _save_csv_fallback(rows: list[dict], run_date: datetime | None = None) -> str:
    """Save results as CSV when Sheets write fails."""
    d = run_date or datetime.utcnow()
    filename = d.strftime("output_%B%Y.csv").lower()
    path = os.path.join(os.path.dirname(__file__), filename)

    if not rows:
        return path

    headers = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    logger.info(f"CSV fallback written to {path}")
    return path
