{{ config(schema='yfinance_clean', materialized='incremental', unique_key=['hour', 'ticker']) }}

-- interpolated stock prices from timestamp, ticker. It forward-fills prices from missing timestamps.

with recursive one_hour_intervals as (

  select
    min(timestamp) as interval_start,
    current_date at time zone 'UTC' as interval_end
  from
    {{ ref('yf_stock_prices_1h') }}

  union all

  select
    interval_start + interval '60 minute',
    interval_end
  from
    one_hour_intervals
  where
    interval_start < interval_end
),

cte_generated_one_hours as (
  select interval_start as one_hour from one_hour_intervals
),

cte_ticker_intervals as (
  select distinct
    cgm.one_hour,
    t.ticker
  from
    cte_generated_one_hours cgm
  cross join
    (select distinct ticker from {{ ref('yf_stock_prices_1h') }}) t
),

cte_stock_tickers as (
  select
    ti.one_hour,
    p.timestamp,
    p.timestamp_tz_aware,
    p.timezone,
    ti.ticker,
    case when p.open = 0 then null else p.open end as open,
    case when p.high = 0 then null else p.high end as high,
    case when p.low = 0 then null else p.low end as low,
    case when p.close = 0 then null else p.close end as close,
    p.volume,
    dividends,
    stock_splits

  from
    cte_ticker_intervals ti

  left join
    {{ ref('yf_stock_prices_1h') }} p
  on
    date_bin('60 min', ti.one_hour, 'epoch') = date_bin('60 min', p.timestamp, 'epoch')
    and ti.ticker = p.ticker
),

cte_ranked_tickers as (
  select
    one_hour,
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
    row_number() over (partition by one_hour, ticker order by timestamp desc) as rn
  from
    cte_stock_tickers
)

select
  ct.one_hour,
  ct.timestamp,
  ct.timestamp_tz_aware,
  ct.timezone,
  ct.ticker,
  coalesce(ct.open, lag(ct.open) over (partition by ct.ticker order by ct.one_hour)) as open,
  coalesce(ct.high, lag(ct.high) over (partition by ct.ticker order by ct.one_hour)) as high,
  coalesce(ct.low, lag(ct.low) over (partition by ct.ticker order by ct.one_hour)) as low,
  coalesce(ct.close, lag(ct.close) over (partition by ct.ticker order by ct.one_hour)) as close,
  coalesce(ct.volume, 0) as volume,
  dividends,
  stock_splits
from
  cte_ranked_tickers ct
where
  ct.rn = 1

  {% if is_incremental() %}
    and ct.one_hour >= (select max(date(one_hour)) - interval '5 day' from {{ this }})
  {% endif %}
order by 1