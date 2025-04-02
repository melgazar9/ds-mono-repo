{{ config(schema='yfinance_clean', materialized='incremental', unique_key=['timestamp', 'ticker']) }}

with cte as (
  select distinct
    timestamp,
    timestamp_tz_aware,
    timezone,
    ticker,
    eps_estimate,
    reported_eps,
    pct_surprise,
    _sdc_batched_at,
    row_number() over (partition by timestamp, ticker order by _sdc_batched_at desc) as rn
  from
    {{ source('tap_yfinance_dev', 'earnings_dates') }}
)

select distinct
  timestamp,
  timestamp_tz_aware,
  timezone,
  ticker,
  eps_estimate,
  reported_eps,
  pct_surprise,
  _sdc_batched_at
from
  cte
where
  rn = 1
  {% if is_incremental() %}
    and timestamp >= (select max(date(timestamp)) - interval '3 days' from {{ this }})
  {% endif %}

order by 1