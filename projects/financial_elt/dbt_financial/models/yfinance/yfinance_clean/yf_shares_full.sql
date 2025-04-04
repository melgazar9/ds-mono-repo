{{ config(schema='yfinance_clean', materialized='incremental', unique_key='surrogate_key') }}

with cte_surrogate as (
  select
    {{ dbt_utils.generate_surrogate_key([
      'timestamp',
      'timestamp_tz_aware',
      'timezone',
      'ticker',
      'amount'
    ]) }} as surrogate_key,
    *
  from
    {{ source('tap_yfinance_dev', 'shares_full') }}
),

cte as (
  select
    *,
    row_number() over(partition by timestamp, ticker order by _sdc_batched_at desc) as rn
  from
    cte_surrogate
)

select
  surrogate_key,
  timestamp,
  timestamp_tz_aware,
  timezone,
  ticker,
  amount,
  _sdc_batched_at
from
  cte
where
  rn = 1
  {% if is_incremental() %}
    and timestamp >= (select max(timestamp) - interval '3 days' from {{ this }})
  {% endif %}

order by 1