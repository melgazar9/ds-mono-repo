import os
import shutil
import sys
import tempfile
from io import StringIO
from time import time

import polars as pl
import psycopg2
from google.cloud import bigquery, storage
from psycopg2 import sql
from tqdm import tqdm

GCP_CREDENTIALS_PATH = os.getenv("GCP_CREDENTIALS_PATH")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET")

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_DB = os.getenv("POSTGRES_DB")

POSTGRES_CONN_PARAMS = {
    "dbname": POSTGRES_DB,
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "host": POSTGRES_HOST,
    "port": POSTGRES_PORT,
}
PG_SCHEMA = "bigquery_migration"

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_DESTINATION_PREFIX = "bq_exports"

already_loaded = {
    "actions",
    "analyst_price_targets",
    "balance_sheet",
    "calendar",
    "cash_flow",
    "crypto_prices_1d",
    "crypto_prices_1h",
    "crypto_prices_1m",
    "crypto_prices_2m",
    "crypto_prices_5m",
    "crypto_tickers",
    "crypto_tickers_top_250",
    "dividends",
    "earnings_dates",
    "earnings_estimate",
    "earnings_history",
    "eps_revisions",
    "eps_trend",
    "fast_info",
    "financials",
    "forex_prices_1d",
    "forex_prices_1h",
    "forex_prices_1m",
    "forex_prices_2m",
    "forex_prices_5m",
    "forex_tickers",
    "futures_prices_1d",
    "futures_prices_1h",
    "futures_prices_1m",
    "futures_prices_2m",
    "futures_prices_5m",
    "futures_tickers",
    "growth_estimates",
    "history_metadata",
    "income_stmt",
    "info",
    "insider_purchases",
    "insider_roster_holders",
    "insider_transactions",
    "institutional_holders",
    "isin",
    "major_holders",
    "mutualfund_holders",
    "options",
    "option_chain",
    "quarterly_balance_sheet",
    "quarterly_cash_flow",
    "quarterly_financials",
    "quarterly_income_stmt",
    "recommendations",
    "recommendations_summary",
    "revenue_estimate",
    "sec_filings",
    "shares_full",
    "splits",
    "forex_tickers_old",
    "crypto_tickers_top_250_old",
    "crypto_tickers_old",
    "futures_tickers_old",
    "stock_tickers_old",
    "stock_prices_1d",
    "stock_prices_1h",
    "stock_prices_1m",
    "stock_prices_2m",
    "stock_prices_5m",
    "stock_tickers",
    "sustainability",
    "test_timescale",
    "ttm_cash_flow",
    "ttm_financials",
    "ttm_income_stmt",
    "upgrades_downgrades",
    # "news"  # error loading news table
}

KNOWN_LARGE_TABLES = {
    "stock_prices_1m",
    "stock_prices_2m",
    "stock_prices_5m",
    "stock_prices_1h",
    "stock_prices_1d",
    "forex_prices_1m",
    "forex_prices_2m",
    "forex_prices_5m",
    "forex_prices_1h",
    "forex_prices_1d",
    "futures_prices_1m",
    "futures_prices_2m",
    "futures_prices_5m",
    "futures_prices_1h",
    "futures_prices_1d",
    "crypto_prices_1m",
    "crypto_prices_2m",
    "crypto_prices_5m",
    "crypto_prices_1h",
    "crypto_prices_1d",
}


bq_client = bigquery.Client(project=GCP_PROJECT_ID)
storage_client = storage.Client(project=GCP_PROJECT_ID)


def get_tables_to_process():
    query = f"""
        SELECT table_name
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}`.INFORMATION_SCHEMA.TABLES
        WHERE table_type = 'BASE TABLE'
    """
    try:
        print(f"Listing tables in BigQuery dataset: {GCP_PROJECT_ID}.{BQ_DATASET}")
        all_tables = {row.table_name for row in bq_client.query(query).result()}
        print(f"Found {len(all_tables)} tables in BigQuery.")

        tables_to_process = list(all_tables - already_loaded)

        print(f"Filtered down to {len(tables_to_process)} tables for processing.")
        return tables_to_process

    except Exception as e:
        print(f"❌ Error listing BigQuery tables: {e}", file=sys.stderr)
        return []


def get_bq_table_schema(table_name: str):
    try:
        table_ref = bigquery.Table(f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}")
        table = bq_client.get_table(table_ref)
        print(f"Fetched schema for BigQuery table: {table_name}")
        return table.schema
    except Exception as e:
        print(f"❌ Error fetching BigQuery schema for {table_name}: {e}", file=sys.stderr)
        raise


def has_complex_types(schema, level=0):
    """Checks if a schema contains ARRAY, STRUCT, or REPEATED types."""
    indent = "  " * level
    # print(f"{indent}Checking schema level {level}...") # Debug print level
    for field in schema:
        # print(f"{indent}  Field: {field.name}, Type: {field.field_type.upper()}, Mode: {field.mode.upper()}") # Debug print field details
        # --- CORRECTED CHECK HERE ---
        if (
            field.field_type.upper() in ["ARRAY", "STRUCT"]
            or field.mode.upper() == "REPEATED"
        ):
            # --- END CORRECTED CHECK ---
            print(
                f"{indent}  DEBUG: Detected complex/repeated type '{field.field_type.upper()}'/'{field.mode.upper()}' for field '{field.name}'."
            )  # Debug print detection
            return True
        # For STRUCTs, recursively check nested fields
        if field.field_type.upper() == "STRUCT" and field.fields:
            # print(f"{indent}  DEBUG: Entering nested STRUCT for field '{field.name}'...") # Debug print entering struct
            if has_complex_types(field.fields, level + 1):
                return True
    # print(f"{indent}Finished checking schema level {level}.") # Debug print level finished
    return False


def map_bq_type_to_postgres(bq_type: str, mode: str) -> str:
    bq_type = bq_type.upper()
    if bq_type in ["STRING", "DATE", "DATETIME", "TIMESTAMP"]:
        return "TEXT"
    elif bq_type in ["INTEGER", "INT64"]:
        return "BIGINT"
    elif bq_type in ["FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC", "DECIMAL"]:
        return "DOUBLE PRECISION"
    elif bq_type == "BOOLEAN":
        return "BOOLEAN"
    elif bq_type == "BYTES":
        return "BYTEA"
    elif bq_type in ["ARRAY", "STRUCT"]:
        print(
            f"Warning: BigQuery type {bq_type} ({mode}) needs careful handling for PostgreSQL. Mapping to JSONB.",
            file=sys.stderr,
        )
        return "JSONB"
    else:
        print(
            f"Warning: Unhandled BigQuery type {bq_type} ({mode}). Mapping to TEXT.",
            file=sys.stderr,
        )
        return "TEXT"


def create_pg_table_from_bq_schema(table_name: str):
    schema = get_bq_table_schema(table_name)

    cols = []
    for field in schema:
        pg_type = map_bq_type_to_postgres(field.field_type, field.mode)
        cols.append(
            sql.SQL("{col_name} {pg_type}").format(
                col_name=sql.Identifier(field.name), pg_type=sql.SQL(pg_type)
            )
        )

    ddl = sql.SQL("CREATE TABLE IF NOT EXISTS {schema}.{table} (\n  {cols}\n);").format(
        schema=sql.Identifier(PG_SCHEMA),
        table=sql.Identifier(table_name),
        cols=sql.SQL(",\n  ").join(cols),
    )

    conn = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONN_PARAMS)
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(
                    sql.Identifier(PG_SCHEMA)
                )
            )
            cur.execute(ddl)
        conn.commit()
        print(f"PostgreSQL table {PG_SCHEMA}.{table_name} created or already exists.")
    except Exception as e:
        print(
            f"❌ Error creating PostgreSQL table {PG_SCHEMA}.{table_name}: {e}",
            file=sys.stderr,
        )
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def export_bq_to_gcs(table_name: str, bucket_name: str, destination_prefix: str):
    source_table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}"
    destination_uri = f"gs://{bucket_name}/{destination_prefix}/{table_name}/*.csv"

    job_config = bigquery.job.ExtractJobConfig()
    job_config.destination_format = bigquery.DestinationFormat.CSV
    job_config.field_delimiter = ","
    job_config.print_header = False

    print(f"Exporting {source_table_id} to {destination_uri}...")

    # Fetch table_ref explicitly to get location
    try:
        source_table_ref = bq_client.get_table(source_table_id)
    except Exception as e:
        print(
            f"❌ Error getting source table reference {source_table_id}: {e}",
            file=sys.stderr,
        )
        raise

    try:
        extract_job = bq_client.extract_table(
            source_table_ref,
            destination_uri,
            job_config=job_config,
            location=source_table_ref.location,
        )

        extract_job.result()  # Wait for the job to complete

        print(f"Export job completed for {table_name}.")

    except Exception as e:
        # Catch and reraise BigQuery errors specifically during the extract
        print(f"❌ BigQuery Export Error for {table_name}: {e}", file=sys.stderr)
        raise e

    bucket = storage_client.get_bucket(bucket_name)
    gcs_files = [
        blob.name
        for blob in bucket.list_blobs(prefix=f"{destination_prefix}/{table_name}/")
        if blob.name.endswith(".csv")
    ]
    print(f"Found {len(gcs_files)} exported files in GCS for {table_name}.")
    return gcs_files


def download_gcs_files(bucket_name: str, gcs_file_names: list, local_dir: str):
    os.makedirs(local_dir, exist_ok=True)
    bucket = storage_client.get_bucket(bucket_name)
    local_paths = []
    print(f"Downloading {len(gcs_file_names)} files from GCS to {local_dir}...")
    for gcs_file_name in tqdm(gcs_file_names, desc="Downloading GCS files"):
        blob = bucket.blob(gcs_file_name)
        local_file_path = os.path.join(local_dir, os.path.basename(gcs_file_name))
        try:
            blob.download_to_filename(local_file_path)
            local_paths.append(local_file_path)
        except Exception as e:
            print(f"❌ Error downloading {gcs_file_name}: {e}", file=sys.stderr)
            raise e
    print("Download complete.")
    return local_paths


def upload_csv_to_postgres(
    local_file_path: str, table_name: str, schema_name: str = "bigquery_migration"
):
    print(
        f"Uploading {os.path.basename(local_file_path)} to {schema_name}.{table_name}..."
    )
    conn = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONN_PARAMS)
        with conn.cursor() as cur:
            # Use NULL '' for empty strings in COPY
            copy_command = sql.SQL(
                "COPY {schema}.{table} FROM STDIN WITH (FORMAT CSV, NULL '')"
            ).format(
                schema=sql.Identifier(schema_name),
                table=sql.Identifier(table_name),
            )
            with open(local_file_path, "r") as f:
                # Use copy_expert with a file-like object
                cur.copy_expert(copy_command, f)
        conn.commit()
        print(f"✅ Uploaded {os.path.basename(local_file_path)}.")
    except Exception as e:
        print(
            f"❌ Error uploading {os.path.basename(local_file_path)} to {schema_name}.{table_name}: {e}",
            file=sys.stderr,
        )
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def delete_gcs_files(bucket_name: str, gcs_file_names: list):
    if not gcs_file_names:
        return
    print(f"Deleting {len(gcs_file_names)} files from GCS...")
    bucket = storage_client.get_bucket(bucket_name)
    try:
        batch_size = 1000  # Delete in batches
        for i in range(0, len(gcs_file_names), batch_size):
            batch_file_names = gcs_file_names[i : i + batch_size]
            bucket.delete_blobs(batch_file_names)
            print(f"  Deleted batch {int(i/batch_size) + 1}...")
        print("GCS file deletion complete.")
    except Exception as e:
        print(f"⚠️ Warning: Error deleting GCS files: {e}", file=sys.stderr)


def delete_local_files(local_file_paths: list):
    if not local_file_paths:
        return
    print(f"Deleting {len(local_file_paths)} local files...")
    for local_path in local_file_paths:
        try:
            os.remove(local_path)
        except Exception as e:
            print(
                f"⚠️ Warning: Error deleting local file {local_path}: {e}",
                file=sys.stderr,
            )
    print("Local file deletion complete.")


def get_bq_row_count(table_name: str) -> int:
    query = f"SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}`"
    try:
        # Use a BigQuery job for the count query
        query_job = bq_client.query(query)
        count_result = query_job.result()  # Wait for the job to complete
        count = list(count_result)[0][0]
        print(f"BigQuery row count for {table_name}: {count}")
        return count
    except Exception as e:
        print(f"❌ Error getting row count for {table_name}: {e}", file=sys.stderr)
        return 0


def process_table(table_name: str):
    print(f"✨ Starting processing for table: {table_name}")
    start_time_process = time()

    local_temp_dir = None
    gcs_exported_files = []
    downloaded_local_files = []

    try:
        bq_schema = get_bq_table_schema(table_name)

        if has_complex_types(bq_schema):
            print(
                f"⚠️ Skipping table {table_name}: Contains complex types (ARRAY or STRUCT) which cannot be exported directly to CSV.",
                file=sys.stderr,
            )
            print(
                f"Please handle {table_name} separately (e.g., export to JSON/Avro, flatten in BQ, or use a different tool).",
                file=sys.stderr,
            )
            return  # Skip this table
        elif table_name == "news":
            # If we reach here for 'news', it means has_complex_types *didn't* detect it
            print(
                f"❌ ERROR: Table 'news' schema did NOT trigger complex type detection, but BQ export failed due to ARRAY/STRUCT.",
                file=sys.stderr,
            )
            print("Dumping schema structure for debugging:", file=sys.stderr)
            for field in bq_schema:
                print(
                    f"  - Field Name: {field.name}, Type: {field.field_type}, Mode: {field.mode}, Fields: {field.fields}",
                    file=sys.stderr,
                )
            # Optionally raise an error here to stop and investigate the schema
            # raise ValueError("Schema detection failure for 'news' table")

        create_pg_table_from_bq_schema(table_name)

        total_rows_bq = get_bq_row_count(table_name)
        if total_rows_bq == 0:
            print(f"Skipping table {table_name} as it has 0 rows or count failed.")
            return

        # Add print statement for the temporary directory being used
        print(f"Script using temporary directory: {tempfile.gettempdir()}")
        local_temp_dir = tempfile.mkdtemp(prefix="bq_pg_export_")
        print(f"Table specific temporary directory: {local_temp_dir}")

        gcs_exported_files = export_bq_to_gcs(
            table_name, GCS_BUCKET_NAME, GCS_DESTINATION_PREFIX
        )
        if not gcs_exported_files:
            print(
                f"❌ No files exported to GCS for {table_name}. Aborting table processing.",
                file=sys.stderr,
            )
            return

        downloaded_local_files = download_gcs_files(
            GCS_BUCKET_NAME, gcs_exported_files, local_temp_dir
        )
        if not downloaded_local_files:
            print(
                f"❌ No files downloaded locally for {table_name}. Aborting table processing.",
                file=sys.stderr,
            )
            return

        with tqdm(
            total=len(downloaded_local_files),
            unit="files",
            desc=f"Uploading {table_name} files",
        ) as pbar:
            for local_file_path in downloaded_local_files:
                try:
                    upload_csv_to_postgres(local_file_path, table_name)
                    pbar.update(1)
                except Exception as upload_e:
                    print(
                        f"❌ Error uploading file {local_file_path}. Aborting table processing.",
                        file=sys.stderr,
                    )
                    raise upload_e  # Stop processing this table on upload failure

        print(
            f"Finished uploading all files for {table_name}. Verifying row count in PostgreSQL..."
        )
        conn = None
        rows_in_pg = -1  # Initialize to -1 in case verification fails
        try:
            conn = psycopg2.connect(**POSTGRES_CONN_PARAMS)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("SELECT COUNT(*) FROM {schema}.{table}").format(
                        schema=sql.Identifier(PG_SCHEMA), table=sql.Identifier(table_name)
                    )
                )
                rows_in_pg = cur.fetchone()[0]

        except Exception as count_e:
            print(
                f"⚠️ Warning: Could not get final row count from PostgreSQL for {table_name}: {count_e}",
                file=sys.stderr,
            )
            rows_in_pg = -1  # Ensure it's -1 if count fails

        finally:
            if conn:
                conn.close()

        end_time_process = time()

        if rows_in_pg != -1 and rows_in_pg != total_rows_bq:
            print(
                f"\n⚠️ Warning: Loaded {rows_in_pg} rows for {table_name}, but BigQuery has {total_rows_bq} rows. Data may be incomplete.",
                file=sys.stderr,
            )
        elif rows_in_pg != -1:  # Only print match if we successfully got the PG count
            print(
                f"✅ PostgreSQL row count ({rows_in_pg}) matches BigQuery count ({total_rows_bq})."
            )
        # else: if rows_in_pg is -1, the warning above is sufficient

        print(
            f"✅ Finished processing {table_name}. Total rows loaded into PG: {rows_in_pg if rows_in_pg != -1 else 'unknown'}. "
            f"Took {round((end_time_process - start_time_process) / 60, 3)} minutes."
        )

    except Exception as e:
        print(f"\n❌ Failed to process table {table_name}: {e}", file=sys.stderr)
        # This 'raise e' stops the loop on the first failure.
        # If you want to continue processing other tables even if one fails,
        # comment out the 'raise e' below.
        raise e
        # Add print to stderr if we catch an exception and continue
        # print(f"Continuing to next table after failure for {table_name}.", file=sys.stderr)

    finally:
        if downloaded_local_files:
            delete_local_files(downloaded_local_files)
        if local_temp_dir and os.path.exists(local_temp_dir):
            try:
                # Use shutil.rmtree to remove directory and its contents
                shutil.rmtree(local_temp_dir, ignore_errors=True)
            except Exception as e:  # Catch any exception during removal
                print(
                    f"⚠️ Warning: Error removing temp directory {local_temp_dir}: {e}",
                    file=sys.stderr,
                )

        # Keeping GCS files for manual cleanup as per previous decision
        if gcs_exported_files:
            print(
                f"Leaving exported files for {table_name} in GCS bucket '{GCS_BUCKET_NAME}' under '{GCS_DESTINATION_PREFIX}/{table_name}/' for manual review/cleanup.",
                file=sys.stderr,
            )


def main():
    # Add checks for required environment variables or defined constants
    if not all([GCP_PROJECT_ID, BQ_DATASET, POSTGRES_CONN_PARAMS.get("password")]):
        print(
            "‼️ ERROR: Missing required configuration (GCP_PROJECT_ID, BQ_DATASET, POSTGRES_PASSWORD).",
            file=sys.stderr,
        )
        print(
            "Ensure environment variables are set or constants are defined.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Add a check for the correct GCS bucket name
    # Assuming 'meltano_tap_yfinance' is the expected bucket name
    if GCS_BUCKET_NAME != "meltano_tap_yfinance":
        print(
            f"⚠️ Warning: GCS_BUCKET_NAME is set to '{GCS_BUCKET_NAME}'. Expected 'meltano_tap_yfinance'.",
            file=sys.stderr,
        )
        # You might want to add a sys.exit(1) here if the bucket name is critical
        # sys.exit(1)

    print(f"Using GCS bucket: {GCS_BUCKET_NAME}")

    tables_to_process = get_tables_to_process()

    if not tables_to_process:
        print("No tables found to process or list retrieval failed. Exiting.")
        return

    print(
        f"Initiating migration for {len(tables_to_process)} table(s) using GCS export method..."
    )

    # Added a comment to allow the loop to continue after failure
    # To make the script continue processing other tables even if one fails,
    # uncomment the 'continue' line and comment out the 'break' line below.
    for table_name in tables_to_process:
        try:
            process_table(table_name)
        except Exception as main_e:
            print(f"Failure processing table {table_name}: {main_e}", file=sys.stderr)
            # If the failure was the expected BQ export error, perhaps continue?
            # If it was a different error (like upload failure), maybe break?
            # For now, keep break to stop on any failure.
            break


if __name__ == "__main__":
    start_time = time()
    # Add a print statement for the overall temporary directory configuration
    print(f"Python's default temporary directory setting: {tempfile.gettempdir()}")
    main()
    print(f"Script execution completed in {round(time() - start_time, 3)} seconds.")
