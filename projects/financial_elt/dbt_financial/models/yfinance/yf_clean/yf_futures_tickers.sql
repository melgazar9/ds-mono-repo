{{ config(schema='yf_clean', materialized='table') }}

with cte as (
  select
    ticker,
    name,
    last_price,
    price,
    market_time,
    change,
    pct_change,
    volume,
    open_interest,
    row_number() over (partition by ticker order by _sdc_batched_at desc) as rn
  from
    {{ source('tap_yfinance_dev', 'futures_tickers') }}
)

select
  ticker,
  name,
  last_price,
  price,
  market_time,
  change,
  pct_change,
  volume,
  open_interest
from
  cte
where
  rn = 1