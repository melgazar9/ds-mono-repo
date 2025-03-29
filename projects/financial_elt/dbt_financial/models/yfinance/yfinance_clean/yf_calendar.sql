{{ config(schema='yfinance_clean', materialized='table') }}

select distinct
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
  {{ source('tap_yfinance_dev', 'calendar') }}