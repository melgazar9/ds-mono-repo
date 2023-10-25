from singer_sdk import typing as th

def get_schema(schema_category):
    if schema_category in ['stock_prices', 'forex_prices', 'crypto_prices']:
        schema = th.PropertiesList(
            th.Property("timestamp", th.DateTimeType, required=True),
            th.Property("timestamp_tz_aware", th.StringType),
            th.Property("timezone", th.StringType),
            th.Property("ticker", th.StringType, required=True),
            th.Property("open", th.NumberType),
            th.Property("high", th.NumberType),
            th.Property("low", th.NumberType),
            th.Property("close", th.NumberType),
            th.Property("volume", th.NumberType),
            th.Property("dividends", th.NumberType),
            th.Property("stock_splits", th.NumberType),
            th.Property("repaired", th.BooleanType),
            th.Property("replication_key", th.StringType)
        ).to_dict()

    elif schema_category in ['stock_prices_wide', 'forex_prices_wide', 'crypto_prices_wide']:
        schema = th.PropertiesList(  # potentially a dynamic number of columns
            th.Property("timestamp", th.DateTimeType, required=True),
            th.Property("data", th.AnyType, required=True)
        ).to_dict()

    elif schema_category == 'stock_tickers':
        schema = th.PropertiesList(
            th.Property("yahoo_ticker", th.StringType, required=True),
            th.Property("google_ticker", th.StringType),
            th.Property("bloomberg_ticker", th.StringType),
            th.Property("numerai_ticker", th.StringType),
            th.Property("yahoo_ticker_old", th.StringType),
            th.Property("yahoo_valid_pts", th.BooleanType),
            th.Property("yahoo_valid_numerai", th.BooleanType)
        ).to_dict()

    elif schema_category == 'forex_tickers':
        schema = th.PropertiesList(
            th.Property("yahoo_ticker", th.StringType, required=True),
            th.Property("yahoo_name", th.StringType),
            th.Property("bloomberg_ticker", th.StringType)
        ).to_dict()

    elif schema_category == 'crypto_tickers':
        schema = th.PropertiesList(
            th.Property("yahoo_ticker", th.StringType, required=True),
            th.Property("yahoo_name", th.StringType),
            th.Property("price_intraday", th.NumberType),
            th.Property("change", th.NumberType),
            th.Property("pct_change", th.StringType),
            th.Property("market_cap", th.StringType),
            th.Property("volume_in_currency_since_0_00_utc", th.StringType),
            th.Property("volume_in_currency_24_hr", th.StringType),
            th.Property("total_volume_all_currencies_24_hr", th.StringType),
            th.Property("circulating_supply", th.StringType)
        ).to_dict()

    else:
        raise NotImplementedError('Only price streams are currently supported.')

    return schema