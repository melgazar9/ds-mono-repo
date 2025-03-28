{{ config(schema='yfinance_clean', materialized='incremental', unique_key=['timestamp', 'ticker']) }}

with cte as (
  select
    timestamp,
    timestamp_tz_aware,
    timezone,
    ticker,
    eps_estimate,
    reported_eps,
    pct_surprise,
    row_number() over (partition by ticker, eps_estimate, reported_eps, pct_surprise order by _sdc_batched_at desc) as rn
  from
    {{ source('tap_yfinance_dev', 'earnings_dates') }}
)

select
  timestamp,
  timestamp_tz_aware,
  timezone,
  ticker,
  eps_estimate,
  reported_eps,
  pct_surprise,
from
  cte
where
  rn = 1
  {% if is_incremental() %}
    and timestamp >= (select max(timestamp) from {{ this }})
  {% endif %}

order by 1