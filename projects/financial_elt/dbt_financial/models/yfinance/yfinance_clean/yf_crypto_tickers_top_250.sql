{{ config(schema='yfinance_clean', materialized='table') }}

with cte as (
  select
    ticker,
    name,
    price,
    change,
    pct_change,
    market_cap,
    volume,
    volume_in_currency_24hr,
    total_volume_all_currencies_24h,
    circulating_supply,
    change_pct_52wk,
    open_interest,
    row_number() over (partition by ticker, name order by _sdc_batched_at desc) as rn
  from
    {{ source('tap_yfinance_dev', 'crypto_tickers_top_250') }}
)

select
  ticker,
  name,
  price,
  change,
  pct_change,
  market_cap,
  volume,
  volume_in_currency_24hr,
  total_volume_all_currencies_24h,
  circulating_supply,
  change_pct_52wk,
  open_interest
from
  cte
where
  rn = 1
order by 1