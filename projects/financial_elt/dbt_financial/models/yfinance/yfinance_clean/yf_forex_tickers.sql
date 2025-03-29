{{ config(schema='yfinance_clean', materialized='table') }}

with cte as (
  select
    ticker,
    name,
    bloomberg_ticker,
    last_price,
    price,
    change,
    pct_change,
    row_number() over (partition by ticker order by _sdc_batched_at desc) as rn
  from
    {{ source('tap_yfinance_dev', 'forex_tickers') }}
)

select
  ticker,
  name,
  bloomberg_ticker,
  last_price,
  price,
  change,
  pct_change
from
  cte
where
  rn = 1