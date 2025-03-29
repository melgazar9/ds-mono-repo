{{ config(schema='yfinance_clean', materialized='incremental', unique_key=['timestamp', 'ticker']) }}

with cte as (
  select
    timestamp,
    timestamp_tz_aware,
    timezone,
    ticker,
    dividends,
    row_number() over (partition by ticker, dividends order by _sdc_batched_at desc) as rn
  from
    {{ source('tap_yfinance_dev', 'dividends') }}
)

select
  timestamp,
  timestamp_tz_aware,
  timezone,
  ticker,
  dividends
from
  cte
where
  rn = 1
  {% if is_incremental() %}
    and timestamp >= (select max(date(timestamp)) - interval '3 days' from {{ this }})
  {% endif %}

order by 1