{{ config(schema='yfinance_clean', materialized='incremental', unique_key='surrogate_key') }}

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
  ticker,
  timestamp_extracted
  period,
  strong_buy,
  buy,
  hold,
  sell,
  strong_sell
from
  cte
where
  rn = 1
  {% if is_incremental() %}
    and timestamp_extracted >= (select max(timestamp_extracted) - 3 from {{ this }})
  {% endif %}