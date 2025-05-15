{{ config(schema='yf_clean', materialized='incremental', unique_key='surrogate_key') }}

with cte_surrogate as (
  select
    {{ dbt_utils.generate_surrogate_key([
      'ticker',
      'period',
      'strong_buy',
      'buy',
      'hold',
      'sell',
      'strong_sell'
    ]) }} as surrogate_key,
    *
  from
    {{ source('tap_yfinance_dev', 'recommendations') }}
),

cte as (
  select
    *,
    row_number() over(partition by surrogate_key order by timestamp_extracted desc) as rn
  from
    cte_surrogate
)

select
  surrogate_key,
  ticker,
  period,
  strong_buy,
  buy,
  hold,
  sell,
  strong_sell,
  timestamp_extracted
from
  cte
where
  rn = 1
  {% if is_incremental() %}
    and date(timestamp_extracted) >= (select max(date(timestamp_extracted)) - interval '3 days' from {{ this }})
  {% endif %}