{{ config(schema='yfinance_clean', materialized='incremental', unique_key=['minute', 'ticker']) }}

-- interpolated forex prices from timestamp, ticker. It forward-fills prices from missing timestamps.

with recursive hourly_intervals as (

  select
    min(timestamp) as interval_start,
    current_date at time zone 'UTC' as interval_end
  from
    {{ ref('yf_forex_prices_1h') }}

  union all

  select
    interval_start + interval '60 minute',
    interval_end
  from
    hourly_intervals
  where
    interval_start < interval_end
),

cte_generated_hours as (
  select interval_start as hour from hourly_intervals
),

cte_ticker_intervals as (
  select distinct
    cgm.hour,
    t.ticker
  from
    cte_generated_hours cgm
  cross join
    (select distinct ticker from {{ ref('yf_forex_prices_1h') }}) t
),

cte_forex_tickers as (
  select
    ti.hour,
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
    {{ ref('yf_forex_prices_1h') }} p
  on
    date_bin('60 min', ti.hour, 'epoch') = date_bin('60 min', p.timestamp, 'epoch')
    and ti.ticker = p.ticker
),

cte_ranked_tickers as (
  select
    hour,
    timestamp,
    timestamp_tz_aware,
    timezone,
    ticker,
    open,
    high,
    low,
    close,
    volume,
    row_number() over (partition by hour, ticker order by timestamp desc) as rn
  from
    cte_forex_tickers
)

select
  ct.hour,
  ct.timestamp,
  ct.timestamp_tz_aware,
  ct.timezone,
  ct.ticker,
  coalesce(ct.open, lag(ct.open) over (partition by ct.ticker order by ct.hour)) as open,
  coalesce(ct.high, lag(ct.high) over (partition by ct.ticker order by ct.hour)) as high,
  coalesce(ct.low, lag(ct.low) over (partition by ct.ticker order by ct.hour)) as low,
  coalesce(ct.close, lag(ct.close) over (partition by ct.ticker order by ct.hour)) as close,
  coalesce(ct.volume, 0) as volume
from
  cte_ranked_tickers ct
where
  ct.rn = 1

  {% if is_incremental() %}
    and ct.hour >= (select max(date(hour)) - interval '3 day' from {{ this }})
  {% endif %}
order by 1