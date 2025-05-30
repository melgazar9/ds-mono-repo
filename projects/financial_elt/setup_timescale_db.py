import os
from ds_core.db_connectors import PostgresConnect

def ensure_primary_key(db, schema, table, pk_cols):
    sql = f"""
    SELECT COUNT(*) FROM information_schema.table_constraints
    WHERE table_schema = '{schema}'
      AND table_name = '{table}'
      AND constraint_type = 'PRIMARY KEY';
    """
    res = db.run_sql(sql, df_type="pandas")
    if res.iloc[0,0] == 0:
        pk_cols_str = ", ".join(pk_cols)
        db.run_sql(f"ALTER TABLE {schema}.{table} ADD PRIMARY KEY ({pk_cols_str});", df_type=None)

BAR_TABLES = {
    "bars_1_second":  ("1 day",    "7 days",   8),
    "bars_1_minute":  ("7 days",   "30 days",  8),
    "bars_1_hour":    ("30 days",  "90 days",  8),
    "bars_1_day":     ("90 days",  "365 days", 8),
    "bars_1_week":    ("180 days", "730 days", 8),
    "bars_1_month":   ("1 year",   "1095 days",8),
}

ENV = os.getenv('ENVIRONMENT', 'dev')  # fallback to 'dev' if not set
SCHEMA = f"tap_polygon_{ENV}"

CREATE_SCHEMA = f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};"
GRANT_SCHEMA = f"GRANT USAGE, CREATE ON SCHEMA {SCHEMA} TO data;"
GRANT_TABLE = f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {SCHEMA} TO data;"

TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
    timestamp TIMESTAMP NOT NULL,
    ticker TEXT NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    vwap NUMERIC,
    transactions NUMERIC,
    otc BOOLEAN,
    PRIMARY KEY (timestamp, ticker)
);
"""

CREATE_HYPERTABLE = """
SELECT create_hypertable(
    '{full_table}',
    'timestamp',
    chunk_time_interval => INTERVAL '{chunk_interval}'
);
"""

ADD_HASH_DIMENSION = """
SELECT add_dimension('{full_table}', 'ticker', number_partitions => {hash_partitions});
"""

ENABLE_COMPRESSION = """
ALTER TABLE {full_table} SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'ticker'
);
"""

ADD_COMPRESSION_POLICY = """
SELECT add_compression_policy('{full_table}', INTERVAL '{compress_interval}');
"""

with PostgresConnect(database="financial_elt") as db:
    db.con = db.con.execution_options(isolation_level="AUTOCOMMIT")
    print("Creating schema and granting access...")
    db.run_sql(CREATE_SCHEMA, df_type=None)
    db.run_sql(GRANT_SCHEMA, df_type=None)

    for table, (chunk_interval, compress_interval, hash_partitions) in BAR_TABLES.items():
        full_table = f"{SCHEMA}.{table}"
        print(f"\nSetting up {full_table}...")
        db.run_sql(TABLE_DDL.format(full_table=full_table), df_type=None)
        db.run_sql(GRANT_TABLE, df_type=None)
        ensure_primary_key(db, SCHEMA, table, ["timestamp", "ticker"])
        db.run_sql(CREATE_HYPERTABLE.format(full_table=full_table, chunk_interval=chunk_interval), df_type=None)
        db.run_sql(ADD_HASH_DIMENSION.format(full_table=full_table, hash_partitions=hash_partitions), df_type=None)
        db.run_sql(ENABLE_COMPRESSION.format(full_table=full_table), df_type=None)
        db.run_sql(ADD_COMPRESSION_POLICY.format(full_table=full_table, compress_interval=compress_interval), df_type=None)

    print("\nAll bar tables created and optimized!")

    # Show hypertables
    print("\nHypertables in database (basic):")
    df = db.run_sql(
        "SELECT hypertable_schema, hypertable_name, num_dimensions, num_chunks, compression_enabled "
        "FROM timescaledb_information.hypertables ORDER BY hypertable_schema, hypertable_name;",
        df_type="pandas"
    )
    print(df)
    print("\nHypertable chunk intervals (time dimension):")
    df = db.run_sql(
        """
        SELECT
          h.hypertable_schema,
          h.hypertable_name,
          d.column_name AS time_column,
          (d.time_interval || ' seconds')::interval AS chunk_interval
        FROM timescaledb_information.hypertables h
        JOIN timescaledb_information.dimensions d
          ON h.hypertable_schema = d.hypertable_schema
         AND h.hypertable_name = d.hypertable_name
        WHERE d.dimension_number = 1
        ORDER BY h.hypertable_schema, h.hypertable_name;
        """,
        df_type="pandas"
    )
    print(df)