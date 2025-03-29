{{ config(schema='yfinance_clean', materialized='incremental', unique_key='surrogate_key') }}

with cte_surrogate as (
  select
    {{ dbt_utils.generate_surrogate_key([
      'ticker',
      'url',
      'shares_owned_indirectly',
      'shares_owned_directly',
      'position_indirect_date',
      'position_direct_date',
      'position',
      'name',
      'most_recent_transaction',
      'latest_transaction_date'
    ]) }} as surrogate_key,
    *
  from
    {{ source('tap_yfinance_dev', 'insider_roster_holders') }}
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
  url,
  shares_owned_indirectly,
  shares_owned_directly,
  position_indirect_date,
  position_direct_date,
  position,
  name,
  most_recent_transaction,
  latest_transaction_date
from
  cte
where
  rn = 1
  {% if is_incremental() %}
    and date(latest_transaction_date) >= (select max(date(latest_transaction_date)) - interval '3 days' from {{ this }})
  {% endif %}