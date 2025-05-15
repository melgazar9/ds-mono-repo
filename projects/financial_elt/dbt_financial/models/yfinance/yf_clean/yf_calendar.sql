{{ config(schema='yf_clean', materialized='table', unique_key=['ticker', 'dividend_date', 'ex_dividend_date', 'earnings_date']) }}

with cte as (
  select distinct
    *,
    row_number() over(partition by dividend_date, ex_dividend_date, earnings_date, ticker order by _sdc_batched_at desc) as rn
  from
    {{ source('tap_yfinance_dev', 'calendar') }}
)

select
  dividend_date,
  ex_dividend_date,
  earnings_date,
  ticker,
  earnings_high,
  earnings_low,
  earnings_average,
  revenue_high,
  revenue_low,
  revenue_average
from
  cte
where
  rn = 1
order by dividend_date, ex_dividend_date, earnings_date, ticker