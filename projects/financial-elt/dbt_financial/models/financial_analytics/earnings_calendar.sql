{{ config(materialized='table', schema='financial_analytics') }}

/*
    Unified earnings calendar combining FMP and yfinance data sources.

    Key features:
    - FMP provides comprehensive historical earnings dates (no timing)
    - yfinance provides earnings dates with pre/post-market timing information
    - Deduplicates by preferring yfinance (has timing) over FMP when both exist
    - release_timing: 'pre-market' (hour < 9), 'post-market' (hour >= 9), or NULL if no timing

    Usage for backtesting:
    - If release_timing = 'pre-market': filter OUT the earnings date itself
    - If release_timing = 'post-market': filter OUT the following trading day
    - If release_timing IS NULL: filter OUT BOTH dates (conservative approach)
*/

with fmp_earnings as (
    select
        symbol as ticker,
        date as earnings_date,
        eps_actual,
        eps_estimated as eps_estimate,
        revenue_actual,
        revenue_estimated as revenue_estimate,
        last_updated,
        'FMP' as source,
        null::text as release_timing,
        null::timestamp as earnings_timestamp,
        1 as priority  -- Lower priority than yfinance
    from {{ source('tap_fmp_production', 'earnings_calendar') }}
    where date is not null
),

yfinance_earnings as (
    select
        ticker,
        timestamp::date as earnings_date,
        reported_eps as eps_actual,
        eps_estimate,
        null::numeric as revenue_actual,
        null::numeric as revenue_estimate,
        null::date as last_updated,
        'yfinance' as source,
        -- Classify pre-market vs post-market based on hour
        case
            when extract(hour from timestamp) < 9 then 'pre-market'
            when extract(hour from timestamp) >= 9 then 'post-market'
            else null
        end as release_timing,
        timestamp as earnings_timestamp,
        2 as priority  -- Higher priority (has timing info)
    from {{ source('tap_yfinance_production', 'earnings_dates') }}
    where timestamp is not null
),

combined_earnings as (
    select * from fmp_earnings
    union all
    select * from yfinance_earnings
),

-- Deduplicate: prefer yfinance (has timing) over FMP when both exist for same ticker/date
deduped as (
    select *
    from (
        select *,
               row_number() over (
                   partition by ticker, earnings_date
                   order by priority desc, last_updated desc
               ) as rn
        from combined_earnings
    ) ranked
    where rn = 1
),

-- Add filter dates based on release timing
final as (
    select
        ticker,
        earnings_date,
        eps_actual,
        eps_estimate,
        case
            when eps_actual is not null and eps_estimate is not null
            then eps_actual - eps_estimate
        end as eps_difference,
        case
            when eps_actual is not null and eps_estimate is not null and eps_estimate != 0
            then ((eps_actual - eps_estimate) / abs(eps_estimate)) * 100
        end as surprise_percent,
        revenue_actual,
        revenue_estimate,
        source,
        release_timing,
        earnings_timestamp,
        last_updated,
        -- Calculate which date(s) to filter for backtesting
        case
            when release_timing = 'pre-market' then earnings_date
            when release_timing = 'post-market' then earnings_date + interval '1 day'
            else earnings_date  -- If no timing, filter this date (will also filter next in backtest logic)
        end::date as filter_date_primary,
        case
            when release_timing is null then earnings_date + interval '1 day'  -- Conservative: filter both dates
            else null
        end::date as filter_date_secondary,
        case
            when eps_actual is not null and eps_estimate is not null then 'complete'
            when eps_actual is not null then 'actual_only'
            when eps_estimate is not null then 'estimate_only'
            else 'incomplete'
        end as data_quality
    from deduped
)

select
    ticker,
    earnings_date,
    eps_actual,
    eps_estimate,
    eps_difference,
    surprise_percent,
    revenue_actual,
    revenue_estimate,
    source,
    release_timing,
    earnings_timestamp,
    filter_date_primary,
    filter_date_secondary,
    data_quality,
    last_updated
from final
where ticker is not null
order by ticker, earnings_date desc
