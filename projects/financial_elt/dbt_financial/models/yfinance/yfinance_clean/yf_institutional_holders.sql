{{ config(schema='yfinance_clean', materialized='incremental', unique_key='surrogate_key') }}

with cte_surrogate as (
  select
    {{ dbt_utils.generate_surrogate_key([
      'date_reported',
      'ticker',
      'holder',
      'pct_out',
      'shares',
      'value'
    ]) }} as surrogate_key,
    *
  from
    {{ source('tap_yfinance_dev', 'institutional_holders') }}
),

cte as (
  select
    *,
    row_number() over(partition by surrogate_key order by _sdc_batched_at desc) as rn
  from
    cte_surrogate
)

select
  surrogate_key,
  date_reported,
  ticker,
  holder,
  pct_out,
  shares,
  value
from
  cte
where
  rn = 1
  {% if is_incremental() %}
    and date(date_reported) >= (select max(date(date_reported)) - interval '3 days' from {{ this }})
  {% endif %}