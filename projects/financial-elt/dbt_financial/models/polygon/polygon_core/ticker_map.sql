{{ config(
    materialized='table',
    schema='polygon_core',
    indexes=[
        {'columns': ['old_ticker'], 'unique': false},
        {'columns': ['ticker'], 'unique': false},
        {'columns': ['composite_figi'], 'unique': false}
    ]
) }}

with cte_current_tickers as (
    select distinct on (coalesce(composite_figi, cik))
        composite_figi,
        cik,
        ticker,
        name,
        active,
        currency_symbol,
        currency_name,
        base_currency_symbol,
        delisted_utc,
        last_updated_utc,
        locale,
        market,
        primary_exchange,
        share_class_figi,
        type,
        source_feed
    from
        {{ source('tap_polygon_production', 'tickers') }}
    where
        composite_figi is not null or cik is not null
)

select
    t.composite_figi,
    t.cik,
    te.ticker as old_ticker,
    t.ticker,
    te.name as old_name,
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
    cte_current_tickers t
left join
    {{ source('tap_polygon_production', 'ticker_events') }} te
on
    (t.composite_figi = te.composite_figi or (t.composite_figi is null and t.cik = te.cik))
    and te.type = 'ticker_change'
    and te.ticker != t.ticker
order by t.ticker, te.date