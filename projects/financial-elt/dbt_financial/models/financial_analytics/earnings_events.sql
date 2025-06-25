{{ config(materialized='table', schema='financial_analytics') }}

with earnings_base as (
    -- earnings dates
    select 
        ticker,
        timestamp::date as date,
        'earnings_date' as type,
        reported_eps as eps_actual,
        eps_estimate,
        pct_surprise as surprise_percent,
        null as revenue,
        null as period,
        timestamp as extracted_at
    from
        {{ source('tap_yfinance_production', 'earnings_dates') }}
    
    union all
    
    -- earnings history 
    select 
        ticker,
        quarter::date as date,
        'history' as type,
        eps_actual,
        eps_estimate,
        surprise_percent,
        null as revenue,
        quarter as period,
        timestamp_extracted as extracted_at
    from
        {{ source('tap_yfinance_production', 'earnings_history') }}
    
    union all
    
    -- earnings stream
    select 
        ticker,
        date::date,
        type,
        actual as eps_actual,
        estimate as eps_estimate,
        null as surprise_percent,
        revenue,
        null as period,
        current_timestamp as extracted_at
    from
        source('tap_yahooquery_production', 'earnings') }}
    where
        type in ('eps', 'quarterly', 'yearly')
    
    union all
    
    -- earnings estimates
    select 
        ticker,
        null as date,
        'estimate' as type,
        null as eps_actual,
        avg as eps_estimate,
        null as surprise_percent,
        null as revenue,
        period,
        timestamp_extracted as extracted_at
    from
        {{ source('tap_yfinance_production', 'earnings_estimate') }}
),

-- dedupe by keeping most recent record per ticker/date/type
deduped as (
    select *
    from (
        select *,
               row_number() over (
                   partition by ticker, coalesce(date, '1900-01-01'), type 
                   order by extracted_at desc
               ) as rn
        from earnings_base
    ) ranked
    where rn = 1
)

select 
    ticker,
    date,
    type,
    period,
    eps_actual,
    eps_estimate,
    case 
        when eps_actual is not null and eps_estimate is not null 
        then eps_actual - eps_estimate 
    end as eps_difference,
    surprise_percent,
    revenue,
    extracted_at,
    case 
        when eps_actual is not null and eps_estimate is not null then 'complete'
        when eps_actual is not null then 'actual_only'  
        when eps_estimate is not null then 'estimate_only'
        else 'incomplete'
    end as data_quality
from deduped
where ticker is not null
order by ticker, date desc nulls last, type