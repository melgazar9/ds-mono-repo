import psycopg2
from ds_core.db_connectors import *

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DIALECT = os.getenv("POSTGRES_DIALECT")

conn = PostgresConnect()


def insert_from_source_to_target(db_params, source_schema, source_table, target_schema, target_table):
    conn = PostgresConnect()
    cur = conn.cursor()

    query = f"""
    INSERT INTO {target_schema}.{target_table} ({columns})
    SELECT {columns}
    FROM {source_schema}.{source_table}
    """

    # You can get columns via separate query or hardcode them if stable
    # Example hardcoded columns for demo:
    columns = "timestamp, timestamp_tz_aware, timezone, ticker, open, high, low, close, volume, repaired, replication_key, _sdc_batched_at, _sdc_received_at, _sdc_deleted_at, _sdc_extracted_at, _sdc_table_version, _sdc_sequence"

    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    insert_from_source_to_target()
