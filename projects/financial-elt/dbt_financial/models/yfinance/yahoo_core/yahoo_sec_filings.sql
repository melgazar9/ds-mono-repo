{{ config(materialized='table', schema='yahoo_core') }}

with cte_union as (
    select
        ticker,
        date,
        epoch_date,
        type,
        title,
        edgar_url,
        exhibits::text[] as exhibits,
        max_age,
        _sdc_batched_at
    from
        {{ source('tap_yfinance_production', 'sec_filings') }}

    union

    select
        ticker,
        date,
        extract(epoch from epoch_date)::integer as epoch_date,
        type,
        title,
        edgar_url,
        exhibits::text[] as exhibits,
        max_age,
        _sdc_batched_at
    from
        {{ source('tap_yahooquery_production', 'sec_filings') }}
)

select distinct
    ticker,
    date,
    epoch_date,
    type,
    title,
    edgar_url,
    exhibits,
    max_age,
    max(_sdc_batched_at) as _sdc_batched_at
from
    cte_union
group by 1, 2, 3, 4, 5, 6, 7, 8