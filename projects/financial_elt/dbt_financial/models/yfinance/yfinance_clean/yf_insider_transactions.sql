{{ config(schema='yfinance_clean', materialized='incremental', unique_key='surrogate_key') }}

with cte_surrogate as (
  select
    {{ dbt_utils.generate_surrogate_key([
      'ticker',
      'start_date',
      'shares',
      'url',
      'text',
      'insider',
      'position',
      'transaction',
      'ownership',
      'value'
    ]) }} as surrogate_key,
    *
  from
    {{ source('tap_yfinance_dev', 'insider_transactions') }}
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
  ticker,
  start_date,
  shares,
  url,
  text,
  insider,
  position,
  transaction,
  ownership,
  value
from
  cte
where
  rn = 1
  {% if is_incremental() %}
    and date(start_date) >= (select max(date(start_date)) - interval '3 days' from {{ this }})
  {% endif %}