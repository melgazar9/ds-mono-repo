{{ config(schema='yfinance', materialized='incremental', unique_key=['timestamp', 'ticker']) }}

with cte as (
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
    revenue_average,
    row_number() over (partition by ticker, dividend_date, ex_dividend_date, earnings_date order by _sdc_batched_at desc) as rn
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
  {% if is_incremental() %}
    and timestamp >= (select max(timestamp) from {{ this }})
  {% endif %}

order by 1

