{{ config(materialized='incremental', schema='prices', unique_key=['timestamp', 'yahoo_ticker']) }}

with cte_most_recent as (
  select
    timestamp,
    timestamp_tz_aware,
    timezone,
    yahoo_ticker,
    bloomberg_ticker,
    numerai_ticker,
    open,
    high,
    low,
    close,
    volume,
    dividends,
    stock_splits,
    batch_timestamp
  from
    {{ source('yfinance_el_production', 'stock_prices_1m') }}
  qualify
    row_number() over(partition by timestamp, yahoo_ticker order by batch_timestamp desc) = 1
)

select
  timestamp,
  timestamp_tz_aware,
  timezone,
  yahoo_ticker,
  bloomberg_ticker,
  numerai_ticker,
  open,
  high,
  low,
  close,
  volume,
  dividends,
  stock_splits,
  batch_timestamp
from
  cte_most_recent
{% if is_incremental() %}
  where timestamp >= (select max(timestamp) from {{this}})
{% endif %}
