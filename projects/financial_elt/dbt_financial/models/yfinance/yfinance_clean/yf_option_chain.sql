{{ config(schema='yfinance_clean', materialized='incremental', unique_key='surrogate_key') }}

with cte_surrogate as (
  select
    {{ dbt_utils.generate_surrogate_key([
      'last_trade_date',
      'last_trade_date_tz_aware',
      'timezone',
      'timestamp_extracted',
      'ticker',
      'contract_symbol',
      'strike',
      'last_price',
      'bid',
      'ask',
      'change',
      'percent_change',
      'volume',
      'open_interest',
      'implied_volatility',
      'in_the_money',
      'contract_size',
      'currency',
      'metadata'
    ]) }} as surrogate_key,
    *
  from
    {{ source('tap_yfinance_dev', 'option_chain') }}
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
  last_trade_date,
  last_trade_date_tz_aware,
  timezone,
  timestamp_extracted,
  ticker,
  contract_symbol,
  strike,
  last_price,
  bid,
  ask,
  change,
  percent_change,
  volume,
  open_interest,
  implied_volatility,
  in_the_money,
  contract_size,
  currency,
  metadata
from
  cte
where
  rn = 1
  {% if is_incremental() %}
    and date(timestamp_extracted) >= (select max(timestamp_extracted) - 3 from {{ this }})
  {% endif %}