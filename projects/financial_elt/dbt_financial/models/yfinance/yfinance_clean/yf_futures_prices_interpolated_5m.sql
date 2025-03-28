{{ config(schema='yfinance_clean', materialized='incremental', unique_key=['minute', 'ticker']) }}

-- interpolated futures prices from timestamp, ticker. It forward-fills prices from missing timestamps.

with recursive five_minute_intervals as (

  select
    min(timestamp) as interval_start,
    current_date at time zone 'UTC' as interval_end
  from
    {{ ref('yf_futures_prices_5m') }}

  union all

  select
    interval_start + interval '5 minute',
    interval_end
  from
    five_minute_intervals
  where
    interval_start < interval_end
),

cte_generated_five_minutes as (
  select interval_start as five_minute from five_minute_intervals
),

cte_ticker_intervals as (
  select distinct
    cgm.five_minute,
    t.ticker
  from
    cte_generated_five_minutes cgm
  cross join
    (select distinct ticker from {{ ref('yf_futures_prices_5m') }}) t
),

cte_futures_tickers as (
  select
    ti.five_minute,
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
    {{ ref('yf_futures_prices_5m') }} p
  on
    date_bin('5 min', ti.five_minute, 'epoch') = date_bin('5 min', p.timestamp, 'epoch')
    and ti.ticker = p.ticker
),

cte_ranked_tickers as (
  select
    five_minute,
    timestamp,
    timestamp_tz_aware,
    timezone,
    ticker,
    open,
    high,
    low,
    close,
    volume,
    row_number() over (partition by five_minute, ticker order by timestamp desc) as rn
  from
    cte_futures_tickers
)

select
  ct.five_minute,
  ct.timestamp,
  ct.timestamp_tz_aware,
  ct.timezone,
  ct.ticker,
  coalesce(ct.open, lag(ct.open) over (partition by ct.ticker order by ct.five_minute)) as open,
  coalesce(ct.high, lag(ct.high) over (partition by ct.ticker order by ct.five_minute)) as high,
  coalesce(ct.low, lag(ct.low) over (partition by ct.ticker order by ct.five_minute)) as low,
  coalesce(ct.close, lag(ct.close) over (partition by ct.ticker order by ct.five_minute)) as close,
  coalesce(ct.volume, 0) as volume
from
  cte_ranked_tickers ct
where
  ct.rn = 1

  {% if is_incremental() %}
    and ct.five_minute >= (select max(date(five_minute)) - interval '5 day' from {{ this }})
  {% endif %}
order by 1