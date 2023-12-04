{{ config(materialized='incremental', schema='yfinance_prices', unique_key=['timestamp', 'ticker']) }}

with cte_most_recent as (
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
    _sdc_batched_at
  from
    {{ source('meltano_yfinance_dev', 'stock_prices_1d') }}
  qualify
    row_number() over(partition by timestamp, ticker order by _sdc_batched_at desc) = 1
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
  _sdc_batched_at
from
  cte_most_recent
{% if is_incremental() %}
  where timestamp >= (select max(timestamp) from {{this}})
{% endif %}