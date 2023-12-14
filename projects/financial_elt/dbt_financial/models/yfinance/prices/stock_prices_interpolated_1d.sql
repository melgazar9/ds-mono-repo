{{ config(materialized='incremental', schema='yfinance_prices', unique_key=['day', 'ticker']) }}

with day_intervals as (
  select
    min(date(timestamp)) as min_date,
    max(date(timestamp)) as max_date
  from
    {{ ref('stock_prices_deduped_1d') }}
),

cte_generated_days as (
  select
    date
  from
    unnest(generate_date_array((select min_date from day_intervals), (select max_date from day_intervals))) as date
),

-- every day has every available ticker attached to it so we can interpolate (forward fill) the gaps
cte_ticker_intervals as (
  select distinct
    cgm.date,
    t.ticker
  from
    cte_generated_days cgm
  cross join
    (select distinct ticker from {{ ref('stock_prices_deduped_1d') }}) t
),

cte_stock_tickers as (
  select
    ti.date,
    sp.timestamp,
    sp.timestamp_tz_aware,
    sp.timezone,
    ti.ticker,
    nullif(sp.open, 0) as open,
    nullif(sp.high, 0) as high,
    nullif(sp.low, 0) as low,
    nullif(sp.close, 0) as close,
    sp.volume
  from
    cte_ticker_intervals ti
  left join
    {{ ref('stock_prices_deduped_1d') }} sp
  on
    ti.date = date(timestamp_trunc(sp.timestamp, day)) and ti.ticker = sp.ticker
)

select
  ct.date,
  ct.timestamp,
  ct.timestamp_tz_aware,
  ct.timezone,
  ct.ticker,
  coalesce(ct.open, lag(ct.open) over(partition by ct.ticker order by ct.date)) as open,
  coalesce(ct.high, lag(ct.high) over(partition by ct.ticker order by ct.date)) as high,
  coalesce(ct.low, lag(ct.low) over(partition by ct.ticker order by ct.date)) as low,
  coalesce(ct.close, lag(ct.close) over(partition by ct.ticker order by ct.date)) as close,
  coalesce(ct.volume, 0) as volume

from
  cte_stock_tickers ct

{% if is_incremental() %}
  where ct.date >= (select max(date(date)) - 5 from {{ this }})
{% endif %}

order by 1