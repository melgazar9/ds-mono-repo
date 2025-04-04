{{ config(schema='yfinance_clean', materialized='incremental', unique_key='surrogate_key') }}

with cte_surrogate as (
  select
    {{ dbt_utils.generate_surrogate_key([
      'ticker',
      'grade_date',
      'firm',
      'to_grade',
      'from_grade',
      'action'
    ]) }} as surrogate_key,
    *
  from
    {{ source('tap_yfinance_dev', 'upgrades_downgrades') }}
),

cte as (
  select
    *,
    row_number() over(partition by grade_date, ticker order by _sdc_batched_at desc) as rn
  from
    cte_surrogate
)

select
  ticker,
  grade_date,
  firm,
  to_grade,
  from_grade,
  action,
  _sdc_batched_at
from
  cte
where
  rn = 1
  {% if is_incremental() %}
    and timestamp >= (select max(timestamp) - interval '3 days' from {{ this }})
  {% endif %}

order by 1