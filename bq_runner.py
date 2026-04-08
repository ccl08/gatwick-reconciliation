"""
BigQuery runner for Gatwick reconciliation.
Pushes cleaned IDs to BQ and runs reconciliation queries against both tables.
"""

import logging
import os
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

logger = logging.getLogger(__name__)

PROJECT = "spa-link-audit-305308"
DATASET = "gatwick_reconciliationIDs_Rakuten"
RECON_DATASET = "gatwick_affiliate_reconciliation"
CREDENTIALS_PATH = "/Users/chriscespedes/gatwick-reconciliation/credentials.json"


def _get_client() -> bigquery.Client:
    if os.path.exists(CREDENTIALS_PATH):
        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIALS_PATH,
            scopes=["https://www.googleapis.com/auth/bigquery"],
        )
        return bigquery.Client(project=PROJECT, credentials=credentials)
    # On Cloud Run the service account is attached to the instance — use ADC
    return bigquery.Client(project=PROJECT)


def _destination_table_id(run_date: datetime | None = None) -> str:
    """Return the timestamped table ID for cleaned IDs, e.g. inquiry_20260330_1322."""
    d = run_date or datetime.utcnow()
    return f"{PROJECT}.{DATASET}.inquiry_{d.strftime('%Y%m%d_%H%M')}"


def push_ids_to_bq(
    transaction_ids: list[str],
    run_date: datetime | None = None,
) -> str:
    """
    Write cleaned transaction IDs to a dated BQ table.
    Creates the table if it doesn't exist, then streams rows via insert_rows_json.
    Clears existing rows first (delete+recreate) to honour WRITE_TRUNCATE semantics.
    Returns the full table ID written to.
    """
    from google.api_core.exceptions import NotFound, Conflict, GoogleAPICallError

    client = _get_client()
    table_id = _destination_table_id(run_date)
    table_name = table_id.split(".")[-1]
    table_ref = client.dataset(DATASET).table(table_name)

    logger.info(f"push_ids_to_bq: target table = {table_id}")
    logger.info(f"push_ids_to_bq: {len(transaction_ids)} raw IDs received")

    rows = [{"transaction_id": tid} for tid in transaction_ids if tid]
    if not rows:
        logger.warning("push_ids_to_bq: no valid IDs after filtering — nothing written")
        return table_id

    schema = [bigquery.SchemaField("transaction_id", "STRING", mode="REQUIRED")]

    # --- Step 1: ensure the table exists -----------------------------------
    try:
        existing = client.get_table(table_ref)
        logger.info(f"push_ids_to_bq: table already exists ({existing.num_rows} rows), deleting and recreating for TRUNCATE semantics")
        try:
            client.delete_table(table_ref)
            logger.info("push_ids_to_bq: existing table deleted")
        except GoogleAPICallError as e:
            logger.error(f"push_ids_to_bq: failed to delete existing table — {type(e).__name__}: {e}")
            raise
    except NotFound:
        logger.info("push_ids_to_bq: table does not exist, will create it")

    try:
        bq_table = bigquery.Table(table_ref, schema=schema)
        client.create_table(bq_table)
        logger.info("push_ids_to_bq: table created successfully")
    except Conflict:
        # Race condition: another process created it between our check and create
        logger.warning("push_ids_to_bq: table creation conflict (already exists) — proceeding with insert")
    except GoogleAPICallError as e:
        logger.error(f"push_ids_to_bq: FAILED to create table — {type(e).__name__}: {e}")
        logger.error(f"  table_ref = {table_ref}")
        logger.error(f"  project   = {PROJECT}")
        logger.error(f"  dataset   = {DATASET}")
        raise

    # --- Step 2: stream rows via insert_rows_json --------------------------
    # Newly created tables need a brief propagation period before streaming inserts work.
    import time
    errors = None
    for attempt in range(1, 6):
        try:
            logger.info(f"push_ids_to_bq: insert attempt {attempt}/5 — {len(rows)} rows")
            errors = client.insert_rows_json(table_ref, rows)
            break  # success
        except GoogleAPICallError as e:
            if "not found" in str(e).lower() and attempt < 5:
                logger.warning(f"push_ids_to_bq: table not yet ready, retrying in {attempt * 2}s…")
                time.sleep(attempt * 2)
            else:
                logger.error(f"push_ids_to_bq: insert_rows_json API call failed — {type(e).__name__}: {e}")
                raise

    if errors:
        # insert_rows_json returns per-row errors, not exceptions
        logger.error(f"push_ids_to_bq: {len(errors)} row-level insert errors:")
        for err in errors[:10]:  # cap log noise to first 10
            logger.error(f"  {err}")
        raise RuntimeError(f"BigQuery insert_rows_json returned {len(errors)} errors — first: {errors[0]}")

    logger.info(f"push_ids_to_bq: success — {len(rows)} IDs written to {table_id}")
    return table_id


def _build_recon_query(bq_table_id: str, date_from: str, date_to: str) -> str:
    """Build the reconciliation SQL against the consolidated parking data table."""
    parking_table = f"{PROJECT}.{RECON_DATASET}.gatwick_parking_data"
    return f"""
SELECT
    pd.event_date,
    pd.entry_date,
    pd.transaction_id,
    pd.item_category
FROM
    `{parking_table}` AS pd
INNER JOIN
    `{bq_table_id}` AS validation_ids
ON
    pd.transaction_id = validation_ids.transaction_id
WHERE
    pd.event_date BETWEEN "{date_from}" AND "{date_to}"
"""


def run_reconciliation(
    date_from: str,
    date_to: str,
    run_date: datetime | None = None,
) -> list[dict]:
    """
    Run reconciliation against both GHP and Main site GA4 tables.
    Returns combined list of matched rows as dicts.

    date_from / date_to: YYYYMMDD strings
    """
    client = _get_client()
    bq_table_id = _destination_table_id(run_date)

    sql = _build_recon_query(bq_table_id, date_from, date_to)
    logger.info("Running reconciliation against gatwick_parking_data")

    try:
        query_job = client.query(sql, location="europe-west2")
        rows = query_job.result()
        all_rows = []
        for row in rows:
            source = "GHP" if row.transaction_id.startswith("GHP-") else "Main"
            all_rows.append({
                "source": source,
                "event_date": row.event_date,
                "entry_date": row.entry_date,
                "transaction_id": row.transaction_id,
                "item_category": row.item_category,
            })
    except Exception as e:
        logger.error(f"BQ reconciliation query failed: {e}")
        raise RuntimeError(f"BigQuery reconciliation query failed: {e}") from e

    logger.info(f"Reconciliation complete: {len(all_rows)} matches found")
    return all_rows
