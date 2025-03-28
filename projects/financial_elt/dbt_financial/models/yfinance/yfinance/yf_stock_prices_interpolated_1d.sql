{{ config(schema='yfinance', materialized='incremental', unique_key=['day', 'ticker']) }}

-- interpolated stock prices from timestamp, ticker. It forward-fills prices from missing timestamps.

with recursive day_intervals as (

  select
    min(timestamp) as interval_start,
    current_date at time zone 'UTC' as interval_end
  from
    {{ ref('yf_stock_prices_1d') }}

  union all

  select
    interval_start + interval '1 day',
    interval_end
  from
    day_intervals
  where
    interval_start < interval_end
),

cte_generated_days as (
  select interval_start as day from day_intervals
),

cte_ticker_intervals as (
  select distinct
    cgm.day,
    t.ticker
  from
    cte_generated_days cgm
  cross join
    (select distinct ticker from {{ ref('yf_stock_prices_1d') }}) t
),

cte_stock_tickers as (
  select
    ti.day,
    fp.timestamp,
    fp.timestamp_tz_aware,
    fp.timezone,
    ti.ticker,
    case when fp.open = 0 then null else fp.open end as open,
    case when fp.high = 0 then null else fp.high end as high,
    case when fp.low = 0 then null else fp.low end as low,
    case when fp.close = 0 then null else fp.close end as close,
    fp.volume,
    fp.dividends,
    fp.stock_splits

  from
    cte_ticker_intervals ti

  left join
    {{ ref('yf_stock_prices_1d') }} fp
  on
    date_trunc('day', ti.day) = date_trunc('day', fp.timestamp)
    and ti.ticker = fp.ticker
),

cte_ranked_tickers as (
  select
    day,
    timestamp,
    timestamp_tz_aware,
    timezone,
    ticker,
    open,
    high,
    low,
    close,
    volume,
    dividends,
    stock_splits,
    row_number() over (partition by day, ticker order by timestamp desc) as rn
  from
    cte_stock_tickers
)

select
  ct.day,
  ct.timestamp,
  ct.timestamp_tz_aware,
  ct.timezone,
  ct.ticker,
  coalesce(ct.open, lag(ct.open) over (partition by ct.ticker order by ct.day)) as open,
  coalesce(ct.high, lag(ct.high) over (partition by ct.ticker order by ct.day)) as high,
  coalesce(ct.low, lag(ct.low) over (partition by ct.ticker order by ct.day)) as low,
  coalesce(ct.close, lag(ct.close) over (partition by ct.ticker order by ct.day)) as close,
  coalesce(ct.volume, 0) as volume,
  dividends,
  stock_splits
from
  cte_ranked_tickers ct
where
  ct.rn = 1

  {% if is_incremental() %}
    and ct.day >= (select max(date(day)) - interval '5 day' from {{ this }})
  {% endif %}
order by 1