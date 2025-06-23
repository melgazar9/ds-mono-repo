"""
Production-ready TimescaleDB + Postgres setup for all Polygon.io tables,
with primary keys and hypertables matching exactly the Meltano tap-polygon stream primary_keys.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from ds_core.db_connectors import PostgresConnect  # noqa: E402

#########################
###### tap-polygon ######
#########################


# ==============================
# Hypertable time-series config (only streams that are time-series)
# ==============================

TIMESERIES_TABLES = {
    "bars_1_second": (["timestamp", "ticker"], "timestamp", "1 day", "7 days", 8),
    "bars_30_second": (["timestamp", "ticker"], "timestamp", "3 days", "14 days", 8),
    "bars_1_minute": (["timestamp", "ticker"], "timestamp", "7 days", "30 days", 8),
    "bars_5_minute": (["timestamp", "ticker"], "timestamp", "21 days", "60 days", 8),
    "bars_30_minute": (["timestamp", "ticker"], "timestamp", "30 days", "90 days", 8),
    "bars_1_hour": (["timestamp", "ticker"], "timestamp", "30 days", "90 days", 8),
    "bars_1_day": (["timestamp", "ticker"], "timestamp", "90 days", "365 days", 8),
    "bars_1_week": (["timestamp", "ticker"], "timestamp", "180 days", "730 days", 8),
    "bars_1_month": (["timestamp", "ticker"], "timestamp", "1 year", "1095 days", 8),
    "daily_market_summary": (["timestamp", "ticker"], "timestamp", "90 days", "365 days", 8),
    "daily_ticker_summary": (["from", "symbol"], "from", "90 days", "365 days", 8),
    "previous_day_bar": (["timestamp", "ticker"], "timestamp", "90 days", "365 days", 8),
    "top_market_movers": (["updated", "ticker"], "updated", "90 days", "365 days", 8),
    "trades": (["ticker", "exchange", "id"], "sip_timestamp", "1 day", "7 days", 16),
    "quotes": (["ticker", "sip_timestamp", "sequence_number"], "sip_timestamp", "1 day", "7 days", 16),
    "sma": (["timestamp", "ticker", "indicator", "series_window_timespan"], "timestamp", "3 days", "21 days", 8),
    "ema": (["timestamp", "ticker", "indicator", "series_window_timespan"], "timestamp", "3 days", "21 days", 8),
    "macd": (["timestamp", "ticker", "indicator", "series_window_timespan"], "timestamp", "3 days", "21 days", 8),
    "rsi": (["timestamp", "ticker", "indicator", "series_window_timespan"], "timestamp", "3 days", "21 days", 8),
    "short_interest": (["settlement_date", "ticker"], "settlement_date", "90 days", "365 days", 8),
    "short_volume": (["date", "ticker"], "date", "90 days", "365 days", 8),
}

# ==============================
# DDL templates for hypertables
# ==============================
BAR_DDL = """
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

DAILY_MARKET_SUMMARY_DDL = BAR_DDL

DAILY_TICKER_SUMMARY_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
    "from" DATE NOT NULL,
    symbol TEXT NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    otc BOOLEAN,
    pre_market NUMERIC,
    after_hours NUMERIC,
    status TEXT,
    PRIMARY KEY ("from", symbol)
);
"""

PREVIOUS_DAY_BAR_DDL = BAR_DDL

TOP_MARKET_MOVERS_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
    updated TIMESTAMP NOT NULL,
    ticker TEXT NOT NULL,
    day JSONB,
    last_quote JSONB,
    last_trade JSONB,
    min JSONB,
    prev_day JSONB,
    todays_change NUMERIC,
    todays_change_percent NUMERIC,
    fair_market_value BOOLEAN,
    PRIMARY KEY (updated, ticker)
);
"""

TRADES_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
    ticker TEXT NOT NULL,
    exchange INTEGER NOT NULL,
    id TEXT NOT NULL,
    sip_timestamp TIMESTAMP NOT NULL,
    participant_timestamp BIGINT,
    price NUMERIC,
    size NUMERIC,
    tape INTEGER,
    sequence_number INTEGER,
    conditions JSONB,
    correction JSONB,
    trf_id INTEGER,
    trf_timestamp BIGINT,
    PRIMARY KEY (ticker, exchange, id, sip_timestamp)
);
"""

QUOTES_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
    ticker TEXT NOT NULL,
    sip_timestamp TIMESTAMP NOT NULL,
    sequence_number INTEGER NOT NULL,
    ask_exchange INTEGER,
    ask_price NUMERIC,
    ask_size NUMERIC,
    bid_exchange INTEGER,
    bid_price NUMERIC,
    bid_size NUMERIC,
    conditions JSONB,
    indicators JSONB,
    participant_timestamp TIMESTAMP,
    tape INTEGER,
    trf_timestamp TIMESTAMP,
    PRIMARY KEY (ticker, sip_timestamp, sequence_number)
);
"""

INDICATOR_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
    timestamp TIMESTAMP NOT NULL,
    ticker TEXT NOT NULL,
    indicator TEXT NOT NULL,
    series_window_timespan TEXT NOT NULL,
    value NUMERIC,
    underlying_timestamp TIMESTAMP,
    underlying_ticker TEXT,
    underlying_open NUMERIC,
    underlying_high NUMERIC,
    underlying_low NUMERIC,
    underlying_close NUMERIC,
    underlying_volume NUMERIC,
    underlying_vwap NUMERIC,
    underlying_transactions INTEGER,
    PRIMARY KEY (timestamp, ticker, indicator, series_window_timespan)
);
"""

SHORT_INTEREST_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
    settlement_date DATE NOT NULL,
    ticker TEXT NOT NULL,
    short_interest BIGINT,
    avg_daily_volume BIGINT,
    days_to_cover NUMERIC,
    PRIMARY KEY (settlement_date, ticker)
);
"""

SHORT_VOLUME_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
    date DATE NOT NULL,
    ticker TEXT NOT NULL,
    short_volume BIGINT,
    total_volume BIGINT,
    short_volume_ratio NUMERIC,
    adf_short_volume INTEGER,
    adf_short_volume_exempt INTEGER,
    exempt_volume INTEGER,
    nasdaq_carteret_short_volume INTEGER,
    nasdaq_carteret_short_volume_exempt INTEGER,
    nasdaq_chicago_short_volume INTEGER,
    nasdaq_chicago_short_volume_exempt INTEGER,
    non_exempt_volume INTEGER,
    nyse_short_volume INTEGER,
    nyse_short_volume_exempt INTEGER,
    PRIMARY KEY (date, ticker)
);
"""

DDL_MAP = {
    "bars_1_second": BAR_DDL,
    "bars_30_second": BAR_DDL,
    "bars_1_minute": BAR_DDL,
    "bars_5_minute": BAR_DDL,
    "bars_30_minute": BAR_DDL,
    "bars_1_hour": BAR_DDL,
    "bars_1_day": BAR_DDL,
    "bars_1_week": BAR_DDL,
    "bars_1_month": BAR_DDL,
    "daily_market_summary": DAILY_MARKET_SUMMARY_DDL,
    "daily_ticker_summary": DAILY_TICKER_SUMMARY_DDL,
    "previous_day_bar": PREVIOUS_DAY_BAR_DDL,
    "top_market_movers": TOP_MARKET_MOVERS_DDL,
    "trades": TRADES_DDL,
    "quotes": QUOTES_DDL,
    "sma": INDICATOR_DDL,
    "ema": INDICATOR_DDL,
    "macd": INDICATOR_DDL,
    "rsi": INDICATOR_DDL,
    "short_interest": SHORT_INTEREST_DDL,
    "short_volume": SHORT_VOLUME_DDL,
}

ENV = os.getenv("ENVIRONMENT", "dev")
SCHEMA = f"tap_polygon_{ENV}"

CREATE_SCHEMA = f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};"
GRANT_SCHEMA = f"GRANT USAGE, CREATE ON SCHEMA {SCHEMA} TO data;"
GRANT_TABLE = f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {SCHEMA} TO data;"

CREATE_HYPERTABLE = """
SELECT create_hypertable(
    '{full_table}',
    '{time_col}',
    chunk_time_interval => INTERVAL '{chunk_interval}'
);
"""

ADD_HASH_DIMENSION = """
SELECT add_dimension('{full_table}', 'ticker', number_partitions => {hash_partitions});
"""

ADD_COMPRESSION_POLICY = """
SELECT add_compression_policy('{full_table}', INTERVAL '{compress_interval}');
"""


def get_segment_col(pk_cols, time_col):
    # Prefer a key that is not the time column
    for col in pk_cols:
        if col != time_col:
            return col
    return pk_cols[0]  # fallback


def safe_run_sql(db, sql, df_type=None, ignore_error_codes=None):
    # Optionally ignore certain postgres error codes for idempotent ops
    ignore_error_codes = ignore_error_codes or []
    try:
        return db.run_sql(sql, df_type=df_type)
    except Exception as e:
        # Optionally skip errors like 'already hypertable', 'already compressed', etc.
        # You can expand this logic based on your needs.
        if hasattr(e, "orig") and hasattr(e.orig, "pgcode") and e.orig.pgcode in ignore_error_codes:
            print(f"Warning: {e}")
        else:
            print(f"Error executing query:\n{sql}\n{e}")
            raise


with PostgresConnect(database="financial_elt") as db:
    db.con = db.con.execution_options(isolation_level="AUTOCOMMIT")
    print("Creating schema and granting access...")
    safe_run_sql(db, CREATE_SCHEMA, df_type=None)
    safe_run_sql(db, GRANT_SCHEMA, df_type=None)

    # Create hypertables with PKs matching Meltano tap-polygon
    for table, (pk_cols, time_col, chunk_interval, compress_interval, hash_partitions) in TIMESERIES_TABLES.items():
        full_table = f"{SCHEMA}.{table}"
        table_ddl = DDL_MAP[table]
        print(f"Setting up hypertable: {full_table}...")
        safe_run_sql(db, table_ddl.format(full_table=full_table), df_type=None)
        safe_run_sql(db, GRANT_TABLE, df_type=None)
        safe_run_sql(
            db,
            CREATE_HYPERTABLE.format(full_table=full_table, time_col=time_col, chunk_interval=chunk_interval),
            df_type=None,
            ignore_error_codes=["TS101"],  # Already a hypertable
        )
        if "ticker" in pk_cols and hash_partitions > 1:
            safe_run_sql(
                db,
                ADD_HASH_DIMENSION.format(full_table=full_table, hash_partitions=hash_partitions),
                df_type=None,
                ignore_error_codes=["TS202"],  # Already has hash dimension
            )
        # Use the correct segmentby column for compression
        segment_col = get_segment_col(pk_cols, time_col)
        safe_run_sql(
            db,
            f"""
            ALTER TABLE {full_table} SET (
                timescaledb.compress,
                timescaledb.compress_segmentby = '{segment_col}'
            );
            """,
            df_type=None,
            ignore_error_codes=["42701"],  # Already compressed
        )
        safe_run_sql(
            db,
            ADD_COMPRESSION_POLICY.format(full_table=full_table, compress_interval=compress_interval),
            df_type=None,
            ignore_error_codes=["TS201"],  # Already has compression policy
        )

    print("\nAll hypertables created and optimized!")

    # Show hypertables
    print("\nHypertables in database (basic):")
    df = safe_run_sql(
        db,
        """
        SELECT hypertable_schema, hypertable_name, num_dimensions, num_chunks, compression_enabled
        FROM timescaledb_information.hypertables ORDER BY hypertable_schema, hypertable_name;
        """,
        df_type="pandas",
    )
    print(df)
    print("\nHypertable chunk intervals (time dimension):")
    df = safe_run_sql(
        db,
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
        df_type="pandas",
    )
    print(df)


##########################
###### tap-yfinance ######
##########################


YF_TIMESERIES_TABLES = {
    # Prices
    "prices_1m": (["timestamp", "ticker"], "timestamp", "7 days", "30 days", 8),
    "prices_2m": (["timestamp", "ticker"], "timestamp", "7 days", "30 days", 8),
    "prices_5m": (["timestamp", "ticker"], "timestamp", "21 days", "60 days", 8),
    "prices_1h": (["timestamp", "ticker"], "timestamp", "90 days", "365 days", 8),
    "prices_1d": (["timestamp", "ticker"], "timestamp", "365 days", "1095 days", 8),
    # Corporate actions/events
    "actions": (["timestamp", "ticker"], "timestamp", "365 days", "1095 days", 4),
    "dividends": (["timestamp", "ticker"], "timestamp", "365 days", "1095 days", 4),
    "splits": (["timestamp", "ticker"], "timestamp", "365 days", "1095 days", 4),
    # Earnings, news
    "earnings_dates": (["timestamp", "ticker"], "timestamp", "365 days", "1095 days", 2),
    "news": (["timestamp_extracted", "ticker"], "timestamp_extracted", "30 days", "180 days", 2),
}

YF_PRICE_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
    timestamp TIMESTAMPTZ NOT NULL,
    ticker TEXT NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    adjclose NUMERIC,
    PRIMARY KEY (timestamp, ticker)
);
"""

YF_ACTIONS_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
    timestamp TIMESTAMPTZ NOT NULL,
    ticker TEXT NOT NULL,
    dividends NUMERIC,
    stock_splits NUMERIC,
    timezone TEXT,
    timestamp_tz_aware TEXT,
    PRIMARY KEY (timestamp, ticker)
);
"""

YF_DIVIDENDS_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
    timestamp TIMESTAMPTZ NOT NULL,
    ticker TEXT NOT NULL,
    dividends NUMERIC,
    timezone TEXT,
    timestamp_tz_aware TEXT,
    PRIMARY KEY (timestamp, ticker)
);
"""

YF_SPLITS_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
    timestamp TIMESTAMPTZ NOT NULL,
    ticker TEXT NOT NULL,
    stock_splits NUMERIC,
    timezone TEXT,
    timestamp_tz_aware TEXT,
    PRIMARY KEY (timestamp, ticker)
);
"""

YF_EARNINGS_DATES_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
    timestamp TIMESTAMPTZ NOT NULL,
    ticker TEXT NOT NULL,
    eps_estimate NUMERIC,
    reported_eps NUMERIC,
    pct_surprise NUMERIC,
    timezone TEXT,
    timestamp_tz_aware TEXT,
    PRIMARY KEY (timestamp, ticker)
);
"""

YF_NEWS_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
    timestamp_extracted TIMESTAMPTZ NOT NULL,
    ticker TEXT NOT NULL,
    id TEXT,
    content TEXT,
    PRIMARY KEY (timestamp_extracted, ticker)
);
"""

YF_DDL_MAP = {
    "prices_1m": YF_PRICE_DDL,
    "prices_2m": YF_PRICE_DDL,
    "prices_5m": YF_PRICE_DDL,
    "prices_1h": YF_PRICE_DDL,
    "prices_1d": YF_PRICE_DDL,
    "actions": YF_ACTIONS_DDL,
    "dividends": YF_DIVIDENDS_DDL,
    "splits": YF_SPLITS_DDL,
    "earnings_dates": YF_EARNINGS_DATES_DDL,
    "news": YF_NEWS_DDL,
}

YF_ENV = os.getenv("ENVIRONMENT", "dev")
YF_SCHEMA = f"tap_yfinance_{YF_ENV}"

YF_CREATE_SCHEMA = f"CREATE SCHEMA IF NOT EXISTS {YF_SCHEMA};"
YF_GRANT_SCHEMA = f"GRANT USAGE, CREATE ON SCHEMA {YF_SCHEMA} TO data;"
YF_GRANT_TABLE = f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {YF_SCHEMA} TO data;"

YF_CREATE_HYPERTABLE = """
SELECT create_hypertable(
    '{full_table}',
    '{time_col}',
    chunk_time_interval => INTERVAL '{chunk_interval}'
);
"""

YF_ADD_HASH_DIMENSION = """
SELECT add_dimension('{full_table}', 'ticker', number_partitions => {hash_partitions});
"""

YF_ADD_COMPRESSION_POLICY = """
SELECT add_compression_policy('{full_table}', INTERVAL '{compress_interval}');
"""


def get_segment_col(pk_cols, time_col):
    for col in pk_cols:
        if col != time_col:
            return col
    return pk_cols[0]


with PostgresConnect(database="financial_elt") as db:
    db.con = db.con.execution_options(isolation_level="AUTOCOMMIT")
    print("\nCreating tap-yfinance schema and granting access...")
    safe_run_sql(db, YF_CREATE_SCHEMA, df_type=None)
    safe_run_sql(db, YF_GRANT_SCHEMA, df_type=None)

    # Create hypertables with PKs matching Meltano tap-yfinance
    for table, (pk_cols, time_col, chunk_interval, compress_interval, hash_partitions) in YF_TIMESERIES_TABLES.items():
        full_table = f"{YF_SCHEMA}.{table}"
        table_ddl = YF_DDL_MAP[table]
        print(f"Setting up hypertable: {full_table}...")
        safe_run_sql(db, table_ddl.format(full_table=full_table), df_type=None)
        safe_run_sql(db, YF_GRANT_TABLE, df_type=None)
        safe_run_sql(
            db,
            YF_CREATE_HYPERTABLE.format(full_table=full_table, time_col=time_col, chunk_interval=chunk_interval),
            df_type=None,
            ignore_error_codes=["TS101"],
        )
        if "ticker" in pk_cols and hash_partitions > 1:
            safe_run_sql(
                db,
                YF_ADD_HASH_DIMENSION.format(full_table=full_table, hash_partitions=hash_partitions),
                df_type=None,
                ignore_error_codes=["TS202"],
            )
        segment_col = get_segment_col(pk_cols, time_col)
        safe_run_sql(
            db,
            f"""
            ALTER TABLE {full_table} SET (
                timescaledb.compress,
                timescaledb.compress_segmentby = '{segment_col}'
            );
            """,
            df_type=None,
            ignore_error_codes=["42701"],
        )
        safe_run_sql(
            db,
            YF_ADD_COMPRESSION_POLICY.format(full_table=full_table, compress_interval=compress_interval),
            df_type=None,
            ignore_error_codes=["TS201"],
        )

    print("\nAll tap-yfinance hypertables created and optimized!")

    # Show hypertables
    print("\nHypertables in database (basic):")
    df = safe_run_sql(
        db,
        """
        SELECT hypertable_schema, hypertable_name, num_dimensions, num_chunks, compression_enabled
        FROM timescaledb_information.hypertables ORDER BY hypertable_schema, hypertable_name;
        """,
        df_type="pandas",
    )
    print(df)
    print("\nHypertable chunk intervals (time dimension):")
    df = safe_run_sql(
        db,
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
        df_type="pandas",
    )
    print(df)
