{{ config(schema='yfinance_clean', materialized='table') }}

select distinct
  ticker,
  timestamp_extracted,
  timestamp_tz_aware,
  currency,
  day_high,
  day_low,
  exchange,
  fifty_day_average,
  last_price,
  last_volume,
  market_cap,
  open,
  previous_close,
  quote_type,
  regular_market_previous_close,
  shares,
  ten_day_average_volume,
  three_month_average_volume,
  extracted_timezone,
  two_hundred_day_average,
  year_change,
  year_high,
  year_low
from
  {{ source('tap_yfinance_dev', 'fast_info') }}
