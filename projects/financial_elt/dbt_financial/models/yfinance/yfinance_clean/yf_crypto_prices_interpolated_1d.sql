{{ config(schema='yfinance', materialized='incremental', unique_key=['day', 'ticker']) }}

-- interpolated crypto prices from timestamp, ticker. It forward-fills prices from missing timestamps.

with recursive daily_intervals as (

  select
    min(timestamp) as interval_start,
    current_date at time zone 'UTC' as interval_end
  from
    {{ ref('yf_crypto_prices_1d') }}

  union all

  select
    interval_start + interval '1 day',
    interval_end
  from
    daily_intervals
  where
    interval_start < interval_end
),

cte_generated_days as (
  select interval_start as day from daily_intervals
),

cte_ticker_intervals as (
  select distinct
    cgm.day,
    t.ticker
  from
    cte_generated_days cgm
  cross join
    (select distinct ticker from {{ ref('yf_crypto_prices_1d') }}) t
),

cte_crypto_tickers as (
  select
    ti.day,
    p.timestamp,
    p.timestamp_tz_aware,
    p.timezone,
    ti.ticker,
    case when p.open = 0 then null else p.open end as open,
    case when p.high = 0 then null else p.high end as high,
    case when p.low = 0 then null else p.low end as low,
    case when p.close = 0 then null else p.close end as close,
    p.volume

  from
    cte_ticker_intervals ti

  left join
    {{ ref('yf_crypto_prices_1d') }} p
  on
    date_trunc('day', ti.day) = date_trunc('day', p.timestamp)
    and ti.ticker = p.ticker
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
    row_number() over (partition by day, ticker order by timestamp desc) as rn
  from
    cte_crypto_tickers
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
  coalesce(ct.volume, 0) as volume
from
  cte_ranked_tickers ct
where
  ct.rn = 1

  {% if is_incremental() %}
    and ct.day >= (select max(date(day)) - interval '5 day' from {{ this }})
  {% endif %}
order by 1