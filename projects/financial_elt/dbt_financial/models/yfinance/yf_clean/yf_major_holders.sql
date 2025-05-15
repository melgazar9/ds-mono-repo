{{ config(schema='yf_clean', materialized='incremental', unique_key='surrogate_key') }}

with cte_surrogate as (
  select
    {{ dbt_utils.generate_surrogate_key([
      'timestamp_extracted',
      'ticker',
      'breakdown',
      'value'
    ]) }} as surrogate_key,
    *
  from
    {{ source('tap_yfinance_dev', 'major_holders') }}
),

cte as (
  select
    surrogate_key,
    timestamp_extracted,
    ticker,
    breakdown,
    value,
    row_number() over(partition by surrogate_key order by _sdc_batched_at desc) as rn
  from
    cte_surrogate
)

select
  surrogate_key,
  timestamp_extracted,
  ticker,
  breakdown,
  value
from
  cte
where
  rn = 1
  {% if is_incremental() %}
    and date(timestamp_extracted) >= (select max(timestamp_extracted) - interval '3 days' from {{ this }})
  {% endif %}