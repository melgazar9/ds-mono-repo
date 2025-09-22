"""
Production-ready TimescaleDB + Postgres setup for all FMP (Financial Modeling Prep) tables,
optimized for 3+ billion API calls and terabytes of financial time-series data.

Based on analysis of tap-fmp streams and FMP API documentation.
Designed for 250+ endpoints, 52k+ tickers, 35+ years of historical data.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from ds_core.db_connectors import PostgresConnect  # noqa: E402

#########################
###### tap-fmp #########
#########################


# ==============================
# Hypertable time-series config (only streams that are time-series)
# ==============================

FMP_TIMESERIES_TABLES = {
    # High-Volume Price & Market Data - Highest Priority
    "unadjusted_price": (["symbol", "date"], "date", "7 days", "30 days", 16),
    "chart_light": (["symbol", "date"], "date", "7 days", "30 days", 16),
    "chart_full": (["symbol", "date"], "date", "7 days", "30 days", 16),
    "eod_bulk": (["symbol", "date"], "date", "7 days", "30 days", 16),
    # Intraday Data - Extremely High Volume
    "prices_1min": (["symbol", "datetime"], "datetime", "1 day", "7 days", 32),
    "prices_5min": (["symbol", "datetime"], "datetime", "1 day", "7 days", 32),
    "prices_15min": (["symbol", "datetime"], "datetime", "3 days", "14 days", 32),
    "prices_30min": (["symbol", "datetime"], "datetime", "3 days", "14 days", 16),
    "prices_1hr": (["symbol", "datetime"], "datetime", "7 days", "30 days", 16),
    "prices_4h": (["symbol", "datetime"], "datetime", "14 days", "60 days", 8),
    "dividend_adjusted_prices": (["symbol", "date"], "date", "14 days", "60 days", 8),
    # Market Performance - High Priority
    "historical_market_cap": (["symbol", "date"], "date", "30 days", "180 days", 8),
    "company_market_cap": (["symbol", "date"], "date", "30 days", "180 days", 8),
    "historical_sector_performance": (["sector", "date"], "date", "30 days", "180 days", 4),
    "historical_industry_performance": (["industry", "date"], "date", "30 days", "180 days", 4),
    "historical_sector_pe": (["sector", "date"], "date", "30 days", "180 days", 4),
    "historical_industry_pe": (["industry", "date"], "date", "30 days", "180 days", 4),
    # Index Data
    "historical_index_full_chart": (["symbol", "date"], "date", "30 days", "180 days", 4),
    "index_1min": (["symbol", "datetime"], "datetime", "1 day", "7 days", 8),
    "index_1hr": (["symbol", "datetime"], "datetime", "7 days", "30 days", 4),
    # Forex & Crypto & Commodities
    "forex_full_chart": (["symbol", "date"], "date", "30 days", "180 days", 4),
    "forex_1min": (["symbol", "datetime"], "datetime", "1 day", "7 days", 8),
    "historical_crypto_full_chart": (["symbol", "date"], "date", "30 days", "180 days", 8),
    "commodities_full_chart": (["symbol", "date"], "date", "30 days", "180 days", 4),
    "commodities_1min": (["symbol", "datetime"], "datetime", "1 day", "7 days", 4),
    "commodities_1hr": (["symbol", "datetime"], "datetime", "7 days", "30 days", 4),
    # Financial Statements - Medium Priority (quarterly/annual frequency)
    "income_statement": (["symbol", "date", "fiscal_year"], "date", "365 days", "1095 days", 4),
    "balance_sheet": (["symbol", "date", "fiscal_year"], "date", "365 days", "1095 days", 4),
    "cash_flow": (["symbol", "date", "fiscal_year"], "date", "365 days", "1095 days", 4),
    "key_metrics": (["symbol", "date", "fiscal_year"], "date", "365 days", "1095 days", 4),
    "financial_ratios": (["symbol", "date", "fiscal_year"], "date", "365 days", "1095 days", 4),
    "enterprise_values": (["symbol", "date", "fiscal_year"], "date", "365 days", "1095 days", 4),
    "owner_earnings": (["symbol", "date", "fiscal_year"], "date", "365 days", "1095 days", 4),
    # Growth Metrics
    "income_statement_growth": (["symbol", "date", "fiscal_year"], "date", "365 days", "1095 days", 4),
    "balance_sheet_growth": (["symbol", "date", "fiscal_year"], "date", "365 days", "1095 days", 4),
    "cash_flow_growth": (["symbol", "date", "fiscal_year"], "date", "365 days", "1095 days", 4),
    "financial_statement_growth": (["symbol", "date", "fiscal_year"], "date", "365 days", "1095 days", 4),
    # TTM Statements (Ultimate and above)
    "income_statement_ttm": (["symbol", "date"], "date", "365 days", "1095 days", 4),
    "balance_sheet_ttm": (["symbol", "date"], "date", "365 days", "1095 days", 4),
    "cash_flow_ttm": (["symbol", "date"], "date", "365 days", "1095 days", 4),
    "key_metrics_ttm": (["symbol", "date"], "date", "365 days", "1095 days", 4),
    "financial_ratios_ttm": (["symbol", "date"], "date", "365 days", "1095 days", 4),
    # As-Reported Statements
    "as_reported_income_statement": (["symbol", "date", "fiscal_year"], "date", "365 days", "1095 days", 4),
    "as_reported_balance_statement": (["symbol", "date", "fiscal_year"], "date", "365 days", "1095 days", 4),
    "as_reported_cashflow_statement": (["symbol", "date", "fiscal_year"], "date", "365 days", "1095 days", 4),
    "as_reported_financial_statement": (["symbol", "date", "fiscal_year"], "date", "365 days", "1095 days", 4),
    # Calendar & Events - Event-driven data
    "earnings_report": (["symbol", "date"], "date", "180 days", "730 days", 2),
    "earnings_calendar": (["symbol", "date"], "date", "180 days", "730 days", 2),
    "dividends_company": (["symbol", "date"], "date", "365 days", "1095 days", 2),
    "dividends_calendar": (["symbol", "date"], "date", "365 days", "1095 days", 2),
    "stock_splits_calendar": (["symbol", "date"], "date", "365 days", "1095 days", 2),
    "ipos_calendar": (["symbol", "date"], "date", "365 days", "1095 days", 2),
    # Insider Trading & Corporate Actions
    "latest_insider_trading": (["symbol", "filing_date", "transaction_date"], "filing_date", "30 days", "180 days", 4),
    "insider_trades_search": (["symbol", "filing_date", "transaction_date"], "filing_date", "30 days", "180 days", 4),
    "insider_trades_by_reporting_name_search": (["symbol", "filing_date"], "filing_date", "30 days", "180 days", 2),
    # Analyst Ratings & Estimates - Time-based
    "historical_ratings": (["symbol", "date"], "date", "365 days", "1095 days", 2),
    "analyst_estimates": (["symbol", "date"], "date", "365 days", "1095 days", 2),
    "historical_stock_grades": (["symbol", "date"], "date", "365 days", "1095 days", 2),
    # Technical Indicators - High Volume
    "simple_moving_average": (["symbol", "date"], "date", "30 days", "180 days", 4),
    "exponential_moving_average": (["symbol", "date"], "date", "30 days", "180 days", 4),
    "weighted_moving_average": (["symbol", "date"], "date", "30 days", "180 days", 4),
    "double_exponential_moving_average": (["symbol", "date"], "date", "30 days", "180 days", 4),
    "triple_exponential_moving_average": (["symbol", "date"], "date", "30 days", "180 days", 4),
    "williams": (["symbol", "date"], "date", "30 days", "180 days", 4),
    "relative_strength_index": (["symbol", "date"], "date", "30 days", "180 days", 4),
    "average_directional_index": (["symbol", "date"], "date", "30 days", "180 days", 4),
    "standard_deviation": (["symbol", "date"], "date", "30 days", "180 days", 4),
    # News & Sentiment - Time-based
    "stock_news": (["published_date", "symbol"], "published_date", "7 days", "30 days", 4),
    "general_news": (["published_date"], "published_date", "7 days", "30 days", 2),
    "crypto_news": (["published_date"], "published_date", "14 days", "60 days", 2),
    "forex_news": (["published_date"], "published_date", "14 days", "60 days", 2),
    "press_releases": (["date", "symbol"], "date", "30 days", "180 days", 2),
    "fmp_articles": (["date"], "date", "30 days", "180 days", 2),
    "search_stock_news": (["published_date", "symbol"], "published_date", "7 days", "30 days", 2),
    "search_crypto_news": (["published_date"], "published_date", "14 days", "60 days", 2),
    "search_forex_news": (["published_date"], "published_date", "14 days", "60 days", 2),
    # 13F Holdings - Quarterly filings
    "institutional_ownership_filings": (["date", "cik"], "date", "365 days", "1095 days", 2),
    "form_13f_filings_extracts_with_analytics": (["date", "cik"], "date", "365 days", "1095 days", 2),
    "filings_extract": (["date", "cik"], "date", "365 days", "1095 days", 2),
    # SEC Filings
    "sec_filings_by_symbol": (["accepted_date", "symbol"], "accepted_date", "30 days", "180 days", 4),
    "sec_filings_by_cik": (["accepted_date", "cik"], "accepted_date", "30 days", "180 days", 4),
    "sec_filings_company_search_by_cik": (["accepted_date", "cik"], "accepted_date", "30 days", "180 days", 4),
    # Financial Reports
    "financial_reports_form_10k_json": (["accepted_date", "symbol"], "accepted_date", "365 days", "1095 days", 2),
    # Crowdfunding & IPOs
    "latest_crowdfunding_campaigns": (["date"], "date", "365 days", "1095 days", 1),
    "crowdfunding_by_cik": (["date", "cik"], "date", "365 days", "1095 days", 1),
    "ipos_disclosure": (["date"], "date", "365 days", "1095 days", 1),
    "ipos_prospectus": (["date"], "date", "365 days", "1095 days", 1),
    "equity_offering_by_cik": (["date", "cik"], "date", "365 days", "1095 days", 1),
    "equity_offering_updates": (["date"], "date", "365 days", "1095 days", 1),
    # Earnings Transcripts (Ultimate and above)
    "earnings_transcripts": (["date", "symbol"], "date", "180 days", "730 days", 2),
}


# ==============================
# DDL templates for hypertables
# ==============================

# Price/Chart Data Tables
PRICE_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,
    adjusted_close NUMERIC,
    unadjusted_volume BIGINT,
    change NUMERIC,
    change_percent NUMERIC,
    vwap NUMERIC,
    label TEXT,
    change_over_time NUMERIC,
    PRIMARY KEY (date, symbol)
);
"""

# Intraday Price Tables
INTRADAY_PRICE_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
    datetime TIMESTAMP NOT NULL,
    symbol TEXT NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,
    PRIMARY KEY (datetime, symbol)
);
"""

# Financial Statement Tables
FINANCIAL_STATEMENT_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    fiscal_year INTEGER,
    period TEXT,
    reported_currency TEXT,
    cik TEXT,
    filling_date DATE,
    accepted_date DATE,
    calendar_year INTEGER,
    link TEXT,
    final_link TEXT,
    -- Financial metrics as JSONB for flexibility (FMP has 100+ metrics per statement)
    metrics JSONB NOT NULL,
    surrogate_key TEXT GENERATED ALWAYS AS (symbol || '_' || date || '_' || COALESCE(period, 'annual')) STORED,
    PRIMARY KEY (date, symbol, fiscal_year)
);
"""

# Market Performance Tables
MARKET_PERFORMANCE_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
    date DATE NOT NULL,
    sector TEXT,
    industry TEXT,
    symbol TEXT,
    performance_metrics JSONB NOT NULL,
    PRIMARY KEY (date, COALESCE(sector, industry, symbol, 'market'))
);
"""

# News Tables
NEWS_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
    published_date TIMESTAMP NOT NULL,
    symbol TEXT,
    title TEXT,
    text TEXT,
    image TEXT,
    url TEXT UNIQUE,
    site TEXT,
    PRIMARY KEY (published_date, COALESCE(symbol, 'general'), url)
);
"""

# Calendar Events Tables
CALENDAR_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    event_type TEXT,
    event_data JSONB NOT NULL,
    PRIMARY KEY (date, symbol, event_type)
);
"""

# Insider Trading Tables
INSIDER_TRADING_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
    filing_date DATE NOT NULL,
    transaction_date DATE,
    symbol TEXT NOT NULL,
    reporting_cik TEXT,
    reporting_name TEXT,
    type_of_owner TEXT,
    transaction_type TEXT,
    securities_owned BIGINT,
    securities_transacted BIGINT,
    price NUMERIC,
    acquisition_or_disposition TEXT,
    ownership_nature TEXT,
    link TEXT,
    PRIMARY KEY (filing_date, transaction_date, symbol, reporting_cik, transaction_type)
);
"""

# Technical Indicators Tables
TECHNICAL_INDICATOR_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    indicator_name TEXT NOT NULL,
    value NUMERIC,
    period INTEGER,
    PRIMARY KEY (date, symbol, indicator_name)
);
"""

# SEC Filings Tables
SEC_FILINGS_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
    accepted_date TIMESTAMP NOT NULL,
    symbol TEXT,
    cik TEXT NOT NULL,
    type TEXT,
    link TEXT,
    filing_data JSONB,
    PRIMARY KEY (accepted_date, cik, type)
);
"""

# Holdings/13F Tables
HOLDINGS_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
    date DATE NOT NULL,
    cik TEXT NOT NULL,
    manager_name TEXT,
    holdings JSONB NOT NULL,
    total_value NUMERIC,
    PRIMARY KEY (date, cik)
);
"""

# Generic table for remaining streams
GENERIC_TIME_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
    date DATE NOT NULL,
    symbol TEXT,
    data JSONB NOT NULL,
    PRIMARY KEY (date, COALESCE(symbol, 'N/A'))
);
"""


FMP_DDL_MAP = {
    # Price & Market Data
    "unadjusted_price": PRICE_DDL,
    "chart_light": PRICE_DDL,
    "chart_full": PRICE_DDL,
    "eod_bulk": PRICE_DDL,
    "dividend_adjusted_prices": PRICE_DDL,
    "historical_market_cap": PRICE_DDL,
    "company_market_cap": PRICE_DDL,
    # Intraday
    "prices_1min": INTRADAY_PRICE_DDL,
    "prices_5min": INTRADAY_PRICE_DDL,
    "prices_15min": INTRADAY_PRICE_DDL,
    "prices_30min": INTRADAY_PRICE_DDL,
    "prices_1hr": INTRADAY_PRICE_DDL,
    "prices_4h": INTRADAY_PRICE_DDL,
    "index_1min": INTRADAY_PRICE_DDL,
    "index_1hr": INTRADAY_PRICE_DDL,
    "forex_1min": INTRADAY_PRICE_DDL,
    "commodities_1min": INTRADAY_PRICE_DDL,
    "commodities_1hr": INTRADAY_PRICE_DDL,
    # Financial Statements
    "income_statement": FINANCIAL_STATEMENT_DDL,
    "balance_sheet": FINANCIAL_STATEMENT_DDL,
    "cash_flow": FINANCIAL_STATEMENT_DDL,
    "key_metrics": FINANCIAL_STATEMENT_DDL,
    "financial_ratios": FINANCIAL_STATEMENT_DDL,
    "enterprise_values": FINANCIAL_STATEMENT_DDL,
    "owner_earnings": FINANCIAL_STATEMENT_DDL,
    "income_statement_growth": FINANCIAL_STATEMENT_DDL,
    "balance_sheet_growth": FINANCIAL_STATEMENT_DDL,
    "cash_flow_growth": FINANCIAL_STATEMENT_DDL,
    "financial_statement_growth": FINANCIAL_STATEMENT_DDL,
    "income_statement_ttm": FINANCIAL_STATEMENT_DDL,
    "balance_sheet_ttm": FINANCIAL_STATEMENT_DDL,
    "cash_flow_ttm": FINANCIAL_STATEMENT_DDL,
    "key_metrics_ttm": FINANCIAL_STATEMENT_DDL,
    "financial_ratios_ttm": FINANCIAL_STATEMENT_DDL,
    "as_reported_income_statement": FINANCIAL_STATEMENT_DDL,
    "as_reported_balance_statement": FINANCIAL_STATEMENT_DDL,
    "as_reported_cashflow_statement": FINANCIAL_STATEMENT_DDL,
    "as_reported_financial_statement": FINANCIAL_STATEMENT_DDL,
    # Market Performance
    "historical_sector_performance": MARKET_PERFORMANCE_DDL,
    "historical_industry_performance": MARKET_PERFORMANCE_DDL,
    "historical_sector_pe": MARKET_PERFORMANCE_DDL,
    "historical_industry_pe": MARKET_PERFORMANCE_DDL,
    "historical_index_full_chart": PRICE_DDL,
    "forex_full_chart": PRICE_DDL,
    "historical_crypto_full_chart": PRICE_DDL,
    "commodities_full_chart": PRICE_DDL,
    # News
    "stock_news": NEWS_DDL,
    "general_news": NEWS_DDL,
    "crypto_news": NEWS_DDL,
    "forex_news": NEWS_DDL,
    "press_releases": NEWS_DDL,
    "fmp_articles": NEWS_DDL,
    "search_stock_news": NEWS_DDL,
    "search_crypto_news": NEWS_DDL,
    "search_forex_news": NEWS_DDL,
    # Calendar Events
    "earnings_report": CALENDAR_DDL,
    "earnings_calendar": CALENDAR_DDL,
    "dividends_company": CALENDAR_DDL,
    "dividends_calendar": CALENDAR_DDL,
    "stock_splits_calendar": CALENDAR_DDL,
    "ipos_calendar": CALENDAR_DDL,
    "earnings_transcripts": CALENDAR_DDL,
    # Insider Trading
    "latest_insider_trading": INSIDER_TRADING_DDL,
    "insider_trades_search": INSIDER_TRADING_DDL,
    "insider_trades_by_reporting_name_search": INSIDER_TRADING_DDL,
    # Technical Indicators
    "simple_moving_average": TECHNICAL_INDICATOR_DDL,
    "exponential_moving_average": TECHNICAL_INDICATOR_DDL,
    "weighted_moving_average": TECHNICAL_INDICATOR_DDL,
    "double_exponential_moving_average": TECHNICAL_INDICATOR_DDL,
    "triple_exponential_moving_average": TECHNICAL_INDICATOR_DDL,
    "williams": TECHNICAL_INDICATOR_DDL,
    "relative_strength_index": TECHNICAL_INDICATOR_DDL,
    "average_directional_index": TECHNICAL_INDICATOR_DDL,
    "standard_deviation": TECHNICAL_INDICATOR_DDL,
    # SEC Filings
    "sec_filings_by_symbol": SEC_FILINGS_DDL,
    "sec_filings_by_cik": SEC_FILINGS_DDL,
    "sec_filings_company_search_by_cik": SEC_FILINGS_DDL,
    "financial_reports_form_10k_json": SEC_FILINGS_DDL,
    # 13F Holdings
    "institutional_ownership_filings": HOLDINGS_DDL,
    "form_13f_filings_extracts_with_analytics": HOLDINGS_DDL,
    "filings_extract": HOLDINGS_DDL,
    # Analyst Data
    "historical_ratings": GENERIC_TIME_DDL,
    "analyst_estimates": GENERIC_TIME_DDL,
    "historical_stock_grades": GENERIC_TIME_DDL,
    # Crowdfunding & IPOs
    "latest_crowdfunding_campaigns": GENERIC_TIME_DDL,
    "crowdfunding_by_cik": GENERIC_TIME_DDL,
    "ipos_disclosure": GENERIC_TIME_DDL,
    "ipos_prospectus": GENERIC_TIME_DDL,
    "equity_offering_by_cik": GENERIC_TIME_DDL,
    "equity_offering_updates": GENERIC_TIME_DDL,
}


ENV = os.getenv("ENVIRONMENT", "dev")
FMP_SCHEMA = f"tap_fmp_{ENV}"

CREATE_SCHEMA = f"CREATE SCHEMA IF NOT EXISTS {FMP_SCHEMA};"
GRANT_SCHEMA = f"GRANT USAGE, CREATE ON SCHEMA {FMP_SCHEMA} TO data;"
GRANT_TABLE = f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {FMP_SCHEMA} TO data;"

CREATE_HYPERTABLE = """
SELECT create_hypertable(
    '{full_table}',
    '{time_col}',
    chunk_time_interval => INTERVAL '{chunk_interval}'
);
"""

ADD_HASH_DIMENSION = """
SELECT add_dimension('{full_table}', 'symbol', number_partitions => {hash_partitions});
"""

ADD_COMPRESSION_POLICY = """
SELECT add_compression_policy('{full_table}', INTERVAL '{compress_interval}');
"""


def get_segment_col(pk_cols, time_col):
    # Prefer a key that is not the time column for compression segmentation
    for col in pk_cols:
        if col != time_col and col in ["symbol", "ticker", "sector", "industry"]:
            return col
    return pk_cols[0] if pk_cols else "symbol"  # fallback


def safe_run_sql(db, sql, df_type=None, ignore_error_codes=None):
    # Optionally ignore certain postgres error codes for idempotent ops
    ignore_error_codes = ignore_error_codes or []
    try:
        return db.run_sql(sql, df_type=df_type)
    except Exception as e:
        # Skip errors like 'already hypertable', 'already compressed', etc.
        if hasattr(e, "orig") and hasattr(e.orig, "pgcode") and e.orig.pgcode in ignore_error_codes:
            print(f"Warning (ignored): {e}")
        else:
            print(f"Error executing query:\n{sql}\n{e}")
            raise


def main():
    with PostgresConnect(database="financial_elt") as db:
        db.con = db.con.execution_options(isolation_level="AUTOCOMMIT")
        print("Creating FMP schema and granting access...")
        safe_run_sql(db, CREATE_SCHEMA, df_type=None)
        safe_run_sql(db, GRANT_SCHEMA, df_type=None)

        # Create hypertables optimized for FMP data patterns
        for table, (pk_cols, time_col, chunk_interval, compress_interval, hash_partitions) in FMP_TIMESERIES_TABLES.items():
            full_table = f"{FMP_SCHEMA}.{table}"
            table_ddl = FMP_DDL_MAP.get(table, GENERIC_TIME_DDL)
            print(f"Setting up hypertable: {full_table}...")

            # Create table
            safe_run_sql(db, table_ddl.format(full_table=full_table), df_type=None)
            safe_run_sql(db, GRANT_TABLE, df_type=None)

            # Convert to hypertable
            safe_run_sql(
                db,
                CREATE_HYPERTABLE.format(full_table=full_table, time_col=time_col, chunk_interval=chunk_interval),
                df_type=None,
                ignore_error_codes=["TS101"],  # Already a hypertable
            )

            # Add space partitioning for high-volume symbol-based data
            if "symbol" in pk_cols and hash_partitions > 1:
                safe_run_sql(
                    db,
                    ADD_HASH_DIMENSION.format(full_table=full_table, hash_partitions=hash_partitions),
                    df_type=None,
                    ignore_error_codes=["TS202"],  # Already has hash dimension
                )

            # Configure compression
            segment_col = get_segment_col(pk_cols, time_col)
            safe_run_sql(
                db,
                f"""
                ALTER TABLE {full_table} SET (
                    timescaledb.compress,
                    timescaledb.compress_segmentby = '{segment_col}',
                    timescaledb.compress_orderby = '{time_col} DESC'
                );
                """,
                df_type=None,
                ignore_error_codes=["42701"],  # Already compressed
            )

            # Add compression policy
            safe_run_sql(
                db,
                ADD_COMPRESSION_POLICY.format(full_table=full_table, compress_interval=compress_interval),
                df_type=None,
                ignore_error_codes=["TS201"],  # Already has compression policy
            )

        print("\n✅ All FMP hypertables created and optimized!")
        print(f"📊 Total tables configured: {len(FMP_TIMESERIES_TABLES)}")

        # Show hypertables
        print("\nHypertables in database (overview):")
        df = safe_run_sql(
            db,
            f"""
            SELECT hypertable_schema, hypertable_name, num_dimensions, num_chunks, compression_enabled
            FROM timescaledb_information.hypertables
            WHERE hypertable_schema = '{FMP_SCHEMA}'
            ORDER BY hypertable_name;
            """,
            df_type="pandas",
        )
        print(df.to_string(index=False))

        print("\nFMP hypertable chunk intervals (time dimension):")
        df = safe_run_sql(
            db,
            f"""
            SELECT
              h.hypertable_name,
              d.column_name AS time_column,
              (d.time_interval || ' seconds')::interval AS chunk_interval,
              h.num_dimensions,
              h.compression_enabled
            FROM timescaledb_information.hypertables h
            JOIN timescaledb_information.dimensions d
              ON h.hypertable_schema = d.hypertable_schema
             AND h.hypertable_name = d.hypertable_name
            WHERE h.hypertable_schema = '{FMP_SCHEMA}' AND d.dimension_number = 1
            ORDER BY h.hypertable_name;
            """,
            df_type="pandas",
        )
        print(df.to_string(index=False))

        print(f"\n🚀 FMP TimescaleDB setup complete for environment: {ENV}")
        print(f"Schema: {FMP_SCHEMA}")
        print("Optimized for 3+ billion API calls and terabytes of time-series data!")


if __name__ == "__main__":
    main()
