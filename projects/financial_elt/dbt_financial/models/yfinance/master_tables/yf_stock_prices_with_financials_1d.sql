{{ config(schema='yfinance_master_tables', materialized='incremental', unique_key=['day', 'ticker']) }}

with cte as (
  select
    spi.day,
    spi.timestamp,
    spi.timestamp_tz_aware,
    spi.timezone,
    spi.ticker,
    spi.open,
    spi.high,
    spi.low,
    spi.close,
    spi.volume,
    spi.dividends,
    spi.stock_splits,
    a.dividends as actions_dividends,
    a.stock_splits as actions_stock_splits
  from
    {{ ref('yf_stock_prices_interpolated_1d') }} spi
  left join {{ source('tap_yfinance_dev', 'actions') }} a on spi.ticker = a.ticker and date(spi.day) = date(a.timestamp)
)

select * from cte