{{ config(schema='yfinance_clean', materialized='incremental', unique_key='surrogate_key') }}

with cte as (
  select
    {{ dbt_utils.generate_surrogate_key([
      'timestamp',
      'ticker',
      'dividends',
      'stock_splits'
    ]) }} as surrogate_key,
    timestamp,
    timestamp_tz_aware,
    timezone,
    ticker,
    dividends,
    stock_splits,
    row_number() over (partition by timestamp, ticker, dividends, stock_splits order by _sdc_batched_at desc) as rn
  from
    {{ source('tap_yfinance_dev', 'actions') }}
)

select
  surrogate_key,
  timestamp,
  timestamp_tz_aware,
  timezone,
  ticker,
  dividends,
  stock_splits
from
  cte
where
  rn = 1
  {% if is_incremental() %}
    and timestamp >= (select max(timestamp) - interval '3 days' from {{ this }})
  {% endif %}

order by 1