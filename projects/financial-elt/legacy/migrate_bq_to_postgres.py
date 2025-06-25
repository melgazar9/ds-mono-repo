import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from io import StringIO
from time import time

import pandas as pd
import polars as pl
import psycopg2
from google.cloud import bigquery
from psycopg2 import sql
from sqlalchemy import create_engine
from tqdm import tqdm

# === CONFIG ===

POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET")

POSTGRES_CONN = {
    "dbname": POSTGRES_DB,
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "host": POSTGRES_HOST,
    "port": POSTGRES_PORT,
}
PG_SCHEMA = "bigquery_migration"
LOCAL_TMP = "/tmp/bq_pg"
os.makedirs(LOCAL_TMP, exist_ok=True)
pg_engine = create_engine(
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# === INIT ===

bq = bigquery.Client(project=GCP_PROJECT_ID)


def get_tables():
    query = f"""
        SELECT table_name
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.INFORMATION_SCHEMA.TABLES`
        WHERE table_type = 'BASE TABLE'
    """

    tables = [row.table_name for row in bq.query(query).result()]

    """ ❌ Failed to process history_metadata: CSV format does not support nested data """

    already_loaded = [
        "actions",
        "balance_sheet",
        "calendar",
        "crypto_prices_1h",
        "crypto_tickers",
        "crypto_tickers_old",
        "crypto_tickers_top_250",
        "forex_prices_1d",
        "forex_prices_5m",
        "forex_tickers",
        "forex_tickers_old",
        "futures_prices_1h",
        "futures_prices_1m",
        "futures_prices_5m",
        "futures_tickers",
        "history_metadata",
        "insider_roster_holders",
        "insider_transactions",
        "institutional_holders",
        "option_chain",
        "quarterly_balance_sheet",
        "quarterly_cash_flow",
        "quarterly_income_stmt",
        "splits",
        "stock_prices_1m",
        "stock_prices_2m",
        "stock_prices_5m",
        "stock_tickers",
        "upgrades_downgrades",
    ]

    tables = [i for i in tables if i not in already_loaded]

    return tables


def fetch_bq_table_to_polars_df(
    table_name, project_id="financial-elt-908001", dataset_id="meltano_yfinance_dev"
) -> pl.DataFrame:
    table_ref = bq.dataset(dataset_id, project=project_id).table(table_name)

    print(f"Reading data from BigQuery table: {project_id}.{dataset_id}.{table_name}")

    try:
        query = f"SELECT COUNT(*) FROM `{project_id}.{dataset_id}.{table_name}`"
        total_rows = list(bq.query(query).result())[0][0]

        rows = bq.list_rows(table_ref)
        arrow_reader = rows.to_arrow_iterable()
        all_chunks = []
        loaded_rows = 0

        with tqdm(total=total_rows, desc="Rows loaded") as pbar:
            for batch in arrow_reader:
                df = pl.from_arrow(batch)
                loaded_rows += df.height
                pbar.update(df.height)
                all_chunks.append(df)
        df = pl.concat(all_chunks)

        return df

    except Exception as e:
        print(f"❌ An error occurred while fetching data from BigQuery: {e}")
        raise


def fetch_data_from_bigquery(query):
    """Fetch data from BigQuery and return it as a pandas DataFrame"""
    df = bq.query(query).to_dataframe(progress_bar_type="tqdm")
    return df


def create_pg_table_from_bq_schema(table_name):
    """Create table in Postgres based on BigQuery schema"""
    table_ref = bq.dataset(BQ_DATASET).table(table_name)
    table = bq.get_table(table_ref)

    cols = []
    for field in table.schema:
        # Simple BQ to PG type mapping
        bq_type = field.field_type.upper()
        if bq_type in ["STRING", "DATE", "DATETIME", "TIMESTAMP"]:
            pg_type = "TEXT"
        elif bq_type in ["INTEGER", "INT64"]:
            pg_type = "BIGINT"
        elif bq_type in ["FLOAT", "FLOAT64", "NUMERIC", "DECIMAL"]:
            pg_type = "DOUBLE PRECISION"
        elif bq_type == "BOOLEAN":
            pg_type = "BOOLEAN"
        else:
            pg_type = "TEXT"  # fallback

        cols.append(f'"{field.name}" {pg_type}')

    ddl = f'CREATE TABLE IF NOT EXISTS {PG_SCHEMA}."{table_name}" (\n  {", ".join(cols)}\n);'

    with psycopg2.connect(**POSTGRES_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()


def map_polars_type_to_postgres(polars_type: pl.DataType) -> str:
    if polars_type in {pl.Int8, pl.Int16, pl.Int32}:
        return "INTEGER"
    elif polars_type in {pl.Int64}:
        return "BIGINT"
    elif polars_type in {pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64}:
        return "BIGINT"
    elif polars_type in {pl.Float32, pl.Float64}:
        return "DOUBLE PRECISION"
    elif polars_type in {pl.Boolean}:
        return "BOOLEAN"
    elif polars_type in {pl.Utf8}:
        return "TEXT"
    elif polars_type in {pl.Date}:
        return "DATE"
    elif isinstance(polars_type, pl.Datetime):
        return "TIMESTAMP WITH TIME ZONE"
    elif polars_type in {pl.Duration}:
        return "INTERVAL"
    elif polars_type in {pl.Categorical, pl.Enum}:
        return "TEXT"
    elif polars_type in {pl.List}:
        return "TEXT"
    elif polars_type in {pl.Struct}:
        return "JSONB"
    elif polars_type in {pl.Binary}:
        return "BYTEA"
    # elif isinstance(polars_type, pl.Decimal):
    #     return f"NUMERIC({polars_type.precision}, {polars_type.scale})"
    else:
        print(f"Warning: Unhandled Polars type {polars_type}. Mapping to TEXT.")
        return "TEXT"


def create_pg_table_from_polars_schema(df: pl.DataFrame, table_name: str, schema_name="bigquery_migration"):
    """
    Creates a table in PostgreSQL based on the schema of a Polars DataFrame.

    Args:
        df: The Polars DataFrame whose schema will be used.
        table_name: The name of the table to create.
        schema_name: The name of the schema in PostgreSQL.
        conn_params: Dictionary of parameters for psycopg2.connect().
    """
    cols = []
    for col_name, col_type in df.schema.items():
        pg_type = map_polars_type_to_postgres(col_type)
        # Construct each column definition as a sql.SQL object
        cols.append(sql.SQL("{col_name} {pg_type}").format(col_name=sql.Identifier(col_name), pg_type=sql.SQL(pg_type)))

    ddl = sql.SQL("CREATE TABLE IF NOT EXISTS {schema}.{table} (\n  {cols}\n);").format(
        schema=sql.Identifier(schema_name), table=sql.Identifier(table_name), cols=sql.SQL(",\n  ").join(cols)
    )

    try:
        with psycopg2.connect(**POSTGRES_CONN) as conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(sql.Identifier(schema_name)))
                cur.execute(ddl)
            conn.commit()
        print(f"Table {schema_name}.{table_name} created or already exists.")
    except Exception as e:
        print(f"❌ Error creating table {schema_name}.{table_name}: {e}")
        raise


def upload_polars_df_to_postgres(
    df: pl.DataFrame, table_name: str, schema_name: str = "bigquery_migration", chunk_size: int = 100000, debug_save_csv=False
):
    try:
        with psycopg2.connect(**POSTGRES_CONN) as conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(sql.Identifier(schema_name)))
                create_pg_table_from_polars_schema(df, table_name)
                for i in tqdm(range(0, df.height, chunk_size), desc=f"Uploading {table_name} chunks", leave=False):
                    chunk = df.slice(i, chunk_size)
                    csv_buf = StringIO()
                    chunk.write_csv(csv_buf, include_header=False, null_value="")
                    csv_buf.seek(0)

                    if debug_save_csv:
                        chunk_file_path = os.path.join(LOCAL_TMP, f"{table_name}_chunk_{int(i / chunk_size)}.csv")
                        with open(chunk_file_path, "w") as f:
                            f.write(csv_buf.getvalue())
                        csv_buf.seek(0)

                    copy_command = sql.SQL("COPY {schema}.{table} FROM STDIN WITH (FORMAT CSV, NULL '')").format(
                        schema=sql.Identifier(schema_name), table=sql.Identifier(table_name)
                    )

                    try:
                        cur.copy_expert(copy_command, csv_buf)
                        print(f"✅ {table_name} loaded.")
                    except Exception as chunk_e:
                        print(f"❌ Error uploading chunk {int(i / chunk_size) + 1} for {table_name}: {chunk_e}")
                        raise chunk_e

            conn.commit()
    except Exception:
        raise


def upload_chunk_to_postgres(df, table_name):
    """Efficiently upload chunk to Postgres using COPY command"""
    csv_buf = StringIO()
    df.to_csv(csv_buf, index=False, na_rep="NULL")  # Change '\N' to NULL for PostgreSQL
    csv_buf.seek(0)

    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()

    try:
        cur.execute("BEGIN;")
        df.head(0).to_sql(table_name.lower(), pg_engine, if_exists="append", index=False, schema="bigquery_migration")
        cur.copy_expert(f"COPY {PG_SCHEMA}.\"{table_name}\" FROM STDIN WITH CSV HEADER NULL AS 'NULL'", csv_buf)
        conn.commit()
    except Exception as e:
        print(f"❌ Error uploading {table_name}: {e}")
        conn.rollback()  # Rollback the transaction on error
    finally:
        cur.close()
        conn.close()


def process_table(table_name, method="polars"):
    """Process each table and upload to Postgres"""
    try:
        start_time_process = time()
        print(f"Processing table: {table_name}")
        if method == "csv":
            df = fetch_data_from_bigquery(f"select * from `{GCP_PROJECT_ID}.meltano_yfinance_dev.{table_name}`")
            create_pg_table_from_bq_schema(table_name)
            chunk_size = 1000000  # 1 million rows per chunk for large tables
            tmp = pd.DataFrame()
            for start in range(0, len(df), chunk_size):
                end = start + chunk_size
                chunk = df.iloc[start:end]
                upload_chunk_to_postgres(chunk, table_name)
                tmp = pd.concat([chunk, tmp])

            end_time = time()
            print(f"✅ {table_name} loaded. Took {round((end_time - start_time_process) / 60, 3)} minutes.")
        elif method == "polars":
            df = fetch_bq_table_to_polars_df(table_name)
            upload_polars_df_to_postgres(df, table_name)
    except Exception as e:
        print(f"❌ Failed to process {table_name}: {e}")


def main():
    tables = get_tables()
    print(f"Found {len(tables)} tables. Starting parallel processing...")

    large_tables = [t for t in tables if "500M" in t]  # Example: detect large tables by name or size logic
    small_tables = [t for t in tables if "500M" not in t]

    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.map(process_table, small_tables)

    with ProcessPoolExecutor(max_workers=1) as executor:
        executor.map(process_table, large_tables)


if __name__ == "__main__":
    start_time = time()
    main()
    print(f"Execution completed in {round(time() - start_time, 3)} seconds.")
