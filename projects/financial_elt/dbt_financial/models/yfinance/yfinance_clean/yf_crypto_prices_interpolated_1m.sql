{{ config(schema='yfinance_clean', materialized='table', unique_key=['minute', 'ticker']) }}

-- interpolated crypto prices from timestamp, ticker. It forward-fills prices from missing timestamps.

with recursive minute_intervals as (

  select
    min(timestamp) as interval_start,
    current_date at time zone 'UTC' as interval_end
  from
    {{ ref('yf_crypto_prices_1m') }}

  union all

  select
    interval_start + interval '1 minute',
    interval_end
  from
    minute_intervals
  where
    interval_start < interval_end
),

cte_generated_minutes as (
  select interval_start as minute from minute_intervals
),

cte_ticker_intervals as (
  select distinct
    cgm.minute,
    t.ticker
  from
    cte_generated_minutes cgm
  cross join
    (select distinct ticker from {{ ref('yf_crypto_prices_1m') }}) t
),

cte_crypto_tickers as (
  select
    ti.minute,
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
    {{ ref('yf_crypto_prices_1m') }} p
  on
    date_trunc('minute', ti.minute) = date_trunc('minute', p.timestamp)
    and ti.ticker = p.ticker
),

cte_ranked_tickers as (
  select
    minute,
    timestamp,
    timestamp_tz_aware,
    timezone,
    ticker,
    open,
    high,
    low,
    close,
    volume,
    row_number() over (partition by minute, ticker order by timestamp desc) as rn
  from
    cte_crypto_tickers
)

select
  ct.minute,
  ct.timestamp,
  ct.timestamp_tz_aware,
  ct.timezone,
  ct.ticker,
  coalesce(ct.open, lag(ct.open) over (partition by ct.ticker order by ct.minute)) as open,
  coalesce(ct.high, lag(ct.high) over (partition by ct.ticker order by ct.minute)) as high,
  coalesce(ct.low, lag(ct.low) over (partition by ct.ticker order by ct.minute)) as low,
  coalesce(ct.close, lag(ct.close) over (partition by ct.ticker order by ct.minute)) as close,
  coalesce(ct.volume, 0) as volume
from
  cte_ranked_tickers ct
where
  ct.rn = 1

  {% if is_incremental() %}
    and date(ct.minute) >= (select max(date(minute)) - interval '3 day' from {{ this }})
  {% endif %}
order by 1