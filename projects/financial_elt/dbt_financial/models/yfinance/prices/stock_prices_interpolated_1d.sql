{{ config(materialized='incremental', schema='yfinance_prices', unique_key=['minute', 'ticker']) }}

with recursive minute_intervals as (
  select
    min(timestamp) as interval_start,
    max(timestamp) as interval_end
  from
    {{ ref('stock_prices_deduped_1d') }}

  union all

  select
    date_add(minute, 1, interval_start),
    interval_end
  from
    minute_intervals
  where
    interval_start < interval_end
),

cte_generated_minutes as (
  select interval_start as minute from minute_intervals
),

-- every minute has every available ticker attached to it so we can interpolate (forward fill) the gaps
cte_ticker_intervals as (
  select distinct
    cgm.minute,
    t.ticker
  from
    cte_generated_minutes cgm
  cross join
    (select distinct ticker from {{ ref('stock_prices_deduped_1d') }}) t

),

cte_stock_tickers as (
  select
    ti.minute as minute,
    sp.timestamp,
    sp.timestamp_tz_aware,
    sp.timezone,
    ti.ticker,
    nullifzero(sp.open) as open,
    nullifzero(sp.high) as high,
    nullifzero(sp.low) as low,
    nullifzero(sp.close) as close,
    sp.volume,
    sp.dividends,
    sp.stock_splits,
    sp.batch_timestamp

  from
    cte_ticker_intervals ti

  left join
    {{ ref('stock_prices_deduped_1d') }} sp
  on
    ti.minute = date_trunc('minute', sp.timestamp)
    and ti.ticker = sp.ticker
  )

select
  st.minute,
  st.timestamp,
  st.timestamp_tz_aware,
  st.timezone,
  st.ticker,
  coalesce(st.open, lag(st.open ignore nulls) over(partition by st.ticker order by st.minute)) as open,
  coalesce(st.high, lag(st.high ignore nulls) over(partition by st.ticker order by st.minute)) as high,
  coalesce(st.low, lag(st.low ignore nulls) over(partition by st.ticker order by st.minute)) as low,
  coalesce(st.close, lag(st.close ignore nulls) over(partition by st.ticker order by st.minute)) as close,
  coalesce(st.volume, 0) as volume,
  coalesce(st.dividends, 0) as dividends,
  coalesce(st.stock_splits, 0) as stock_splits,
  coalesce(st.batch_timestamp, lag(st.batch_timestamp ignore nulls) over(partition by st.ticker order by st.minute)) as batch_timestamp

from
  cte_stock_tickers st

{% if is_incremental() %}
  where date(st.minute) >= (select max(date(minute) - 5) from {{ this }})
{% endif %}

order by 1