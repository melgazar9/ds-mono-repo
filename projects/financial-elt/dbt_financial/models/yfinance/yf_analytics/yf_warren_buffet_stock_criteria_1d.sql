{{ config(schema='yf_core', materialized='table', unique_key=['day', 'ticker']) }}

with cte as (
  select
    date(spi.timestamp) as day,
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

    lag(spi.close, 30) over(partition by spi.ticker order by spi.timestamp) as close_lag_30d,
    f.net_income as annual_net_income,
    bs.total_debt as annual_total_debt,
    bs.stockholders_equity as annual_stockholders_equity,
    f.ebit,
    f.interest_expense,
    f.total_revenue as annual_revenue,
    ismt.operating_income as annual_operating_income,
    cf.capital_expenditure as annual_capital_expenditure,
    cf.free_cash_flow as annual_free_cash_flow,
--    bs.total_debt / bs.stockholders_equity as annual_debt_to_equity,
    (f.net_income / nullif(bs.stockholders_equity, 0)) >= 0.15 as annual_roe_ge_15
--    f.ebit / f.interest_expense as interest_coverage,
--    cf.free_cash_flow / f.total_revenue as fcf_percentage
--    i.market_cap as market_cap,
--    fcf.free_cash_flow / i.market_cap as fcf_yield,
--    i.trailing_pe,
--    i.market_cap / fcf.free_cash_flow as price_free_cashflow,
--    i.trailing_peg_ratio,
--    i.price_to_book

  from
    {{ ref('yf_stock_prices_1d') }} spi
  left join {{ ref('yf_financials') }} f on date(spi.timestamp) = date(f.date) and spi.ticker = f.ticker
  left join {{ ref('yf_balance_sheet') }} bs on date(spi.timestamp) = date(bs.date) and spi.ticker = bs.ticker
  left join {{ ref('yf_cash_flow') }} cf on date(spi.timestamp) = date(cf.date) and spi.ticker = cf.ticker
  left join {{ ref('yf_income_stmt') }} ismt on date(spi.timestamp) = date(ismt.date) and spi.ticker = ismt.ticker
  -- join yf_info when data becomes available
)

select
  cte.*
from
  cte
