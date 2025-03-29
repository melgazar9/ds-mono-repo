{{ config(schema='yfinance_clean', materialized='table') }}

select distinct
  timestamp,
  timestamp_tz_aware,
  timezone,
  ticker,
  stock_splits
from
  {{ source('tap_yfinance_dev', 'splits') }}