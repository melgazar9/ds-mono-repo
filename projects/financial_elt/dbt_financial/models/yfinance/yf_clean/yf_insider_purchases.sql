{{ config(schema='yf_clean', materialized='table', unique_key=['ticker', 'date_extracted']) }}

with cte as (
  select distinct
    *,
    date(timestamp_extracted) as date_extracted,
    row_number() over(partition by date(timestamp_extracted), ticker order by _sdc_batched_at desc) as rn
  from
    {{ source('tap_yfinance_dev', 'insider_purchases') }}
)

select
  timestamp_extracted,
  date_extracted,
  ticker,
  insider_purchases_last_6m,
  shares,
  trans
from
  cte
where
  rn = 1