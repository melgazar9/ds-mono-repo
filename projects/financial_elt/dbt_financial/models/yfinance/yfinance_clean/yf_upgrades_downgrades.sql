{{ config(schema='yfinance_clean', materialized='table') }}

select distinct
  ticker,
  grade_date,
  firm,
  to_grade,
  from_grade,
  action
from
  {{ source('tap_yfinance_dev', 'upgrades_downgrades') }}