{{
  config(
    schema='yf_prices',
    materialized='incremental',
    unique_key=['timestamp', 'ticker'],
    on_schema_change='append_new_columns'
  )
}}

-- deduped stock prices by timestamp, ticker

with cte as (
  select
    timestamp,
    timestamp_tz_aware,
    timezone,
    ticker,
    open,
    high,
    low,
    close,
    volume,
    dividends,
    stock_splits,
    repaired,
    replication_key,
    row_number() over (partition by timestamp, ticker order by _sdc_batched_at desc) as rn
  from
    {{ source('tap_yfinance_dev', 'stock_prices_1d') }}
)

select
  timestamp,
  timestamp_tz_aware,
  timezone,
  ticker,
  open,
  high,
  low,
  close,
  volume,
  dividends,
  stock_splits,
  repaired,
  replication_key
from
  cte
where
  rn = 1
  {% if is_incremental() %}
    and timestamp >= (select max(timestamp) from {{ this }})
  {% endif %}

order by 1