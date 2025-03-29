{{ config(schema='yfinance_clean', materialized='table') }}

select distinct
  ticker,
  sec_ticker,
  yahoo_ticker_pts,
  google_ticker_pts,
  sec_cik_str,
  sec_title,
  google_ticker,
  bloomberg_ticker,
  numerai_ticker,
  yahoo_ticker_old,
  yahoo_valid_pts,
  yahoo_valid_numerai
from
  {{ source('tap_yfinance_dev', 'stock_tickers') }}