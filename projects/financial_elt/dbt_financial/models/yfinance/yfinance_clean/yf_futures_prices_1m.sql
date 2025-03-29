{{ config(schema='yfinance_clean', materialized='incremental', unique_key=['timestamp', 'ticker'] ) }}

-- deduped futures prices by timestamp, ticker

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
    repaired,
    replication_key,
    row_number() over (partition by timestamp, ticker order by _sdc_batched_at desc) as rn
  from
    {{ source('tap_yfinance_dev', 'futures_prices_1m') }}
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
  repaired,
  replication_key
from
  cte
where
  rn = 1
  {% if is_incremental() %}
    and timestamp >= (select max(date(timestamp)) - interval '3 days' from {{ this }})
  {% endif %}

order by 1