{{ config(schema='yfinance_clean', materialized='table') }}

select distinct
  timestamp,
  timestamp_tz_aware,
  timezone,
  ticker,
  amount
from
  {{ source('tap_yfinance_dev', 'shares_full') }}