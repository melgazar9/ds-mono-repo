{{ config(
    materialized='table',
    schema='polygon_analytics',
    indexes=[
        {'columns': ['old_ticker'], 'unique': false},
        {'columns': ['ticker'], 'unique': false},
        {'columns': ['composite_figi'], 'unique': false}
    ]
) }}

with cte_ticker_changes as (
    select
        composite_figi,
        cik,
        ticker,
        name,
        date,
        type,
        lag(ticker) over (partition by composite_figi order by date) as old_ticker,
        lag(name) over (partition by composite_figi order by date) as old_name
    from
        {{ source('tap_polygon_production', 'ticker_events') }}
    where
        type = 'ticker_change'
),

cte_latest_ticker_change as (
    select distinct on (composite_figi)
        composite_figi,
        cik,
        ticker as current_ticker,
        old_ticker,
        name,
        old_name,
        date,
        type
    from
        cte_ticker_changes
    where
        old_ticker is not null
    order by composite_figi, date desc
)

select
    t.composite_figi,
    t.cik,
    t.ticker,
    te.old_ticker,
    t.name,
    te.date as event_date,
    t.active,
    t.currency_symbol,
    t.currency_name,
    t.base_currency_symbol,
    t.delisted_utc,
    t.last_updated_utc,
    t.locale,
    t.market,
    t.primary_exchange,
    t.share_class_figi,
    te.type as event_type,
    t.type as ticker_type,
    t.source_feed
from
    {{ source('tap_polygon_production', 'tickers') }} t
left join
    cte_latest_ticker_change te
on
    t.composite_figi = te.composite_figi
    and t.ticker = te.current_ticker
order by t.ticker