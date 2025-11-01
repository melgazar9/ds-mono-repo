{{ config(
    materialized='table',
    schema='financial_analytics',
    indexes=[
        {'columns': ['ticker', 'event_date'], 'unique': false},
        {'columns': ['composite_figi', 'event_date'], 'unique': false},
        {'columns': ['event_type'], 'unique': false},
        {'columns': ['requires_price_adjustment'], 'unique': false},
        {'columns': ['potential_delisting'], 'unique': false}
    ]
) }}

/*
  M&A Events:

  - DEFM14A/C - Definitive merger proxy
  - DEFA14A/C - Additional merger materials
  - SC TO-T/A, SC TO-I/A, SC TO-C - All tender offer variants
  - 425 - Merger communications
  - SC 13D (both formats) - Activist 5%+ stake disclosures
  - S-4/F-4 - Merger registration statements

  Delisting Events:

  - SC 13E3 - Going private transactions
  - 15-* - Delisting notices
  - 25-* - Delisting notifications

  Kept Sources:

  - All dividends (no filtering - you adjust the gap % calculation)
  - Stock splits/reverse splits
  - Symbol changes
  - Critical M&A/delisting SEC filings (simplified)
  - FMP as primary, Polygon/Yahoo as fallback (via deduplication)
*/

with cte_fmp_dividends as (
    select
        symbol as ticker,
        date::date as event_date,
        'USD' as currency,
        dividend as cash_amount,
        'dividend' as event_type,
        concat('Dividend $', round(dividend, 4), ' (USD)') as description,
        null::numeric as split_from,
        null::numeric as split_to,
        null::text as filing_type,
        true as requires_price_adjustment,
        false as potential_delisting,
        10 as sort_priority,
        'fmp' as data_source,
        98 as data_quality_score
    from
        {{ source('tap_fmp_production', 'dividends_company') }}
    where
        date is not null
        and dividend is not null
        and dividend > 0
        and symbol is not null
),

cte_fmp_splits as (
    select
        symbol as ticker,
        date::date as event_date,
        null::text as currency,
        null::numeric as cash_amount,
        case
            when numerator < denominator then 'reverse_split'
            else 'split'
        end as event_type,
        concat('Stock split ', numerator, ':', denominator, ' (', round(numerator::numeric / nullif(denominator, 0), 5), ':1)') as description,
        numerator as split_from,
        denominator as split_to,
        null::text as filing_type,
        true as requires_price_adjustment,
        false as potential_delisting,
        20 as sort_priority,
        'fmp' as data_source,
        98 as data_quality_score
    from
        {{ source('tap_fmp_production', 'stock_split_details') }}
    where
        date is not null
        and symbol is not null
        and numerator is not null
        and denominator is not null
        and denominator > 0
),

cte_fmp_symbol_changes as (
    select
        old_symbol as ticker,
        date::date as event_date,
        null::text as currency,
        null::numeric as cash_amount,
        'ticker_change' as event_type,
        concat('Ticker Change: ', old_symbol, ' → ', new_symbol) as description,
        null::numeric as split_from,
        null::numeric as split_to,
        null::text as filing_type,
        false as requires_price_adjustment,
        false as potential_delisting,
        90 as sort_priority,
        'fmp' as data_source,
        90 as data_quality_score
    from
        {{ source('tap_fmp_production', 'symbol_changes') }}
    where
        date is not null
        and old_symbol is not null
        and new_symbol is not null
),

cte_fmp_sec_filings as (
    select
        symbol as ticker,
        filing_date::date as event_date,
        null::text as currency,
        null::numeric as cash_amount,
        case
            -- Critical M&A Events (Gap Strategy Exclusions)
            when form_type in ('DEFM14A', 'DEFM14C') then 'definitive_merger'
            when form_type in ('DEFA14A', 'DEFA14C') then 'merger_proxy'
            when form_type in ('SC TO-T', 'SC TO-T/A', 'SC TO-I', 'SC TO-I/A', 'SC TO-C') then 'tender_offer'
            when form_type = '425' then 'merger_communication'

            -- Activist/Large Acquisitions (5%+ stakes)
            when form_type in ('SC 13D', 'SC 13D/A', 'SCHEDULE 13D', 'SCHEDULE 13D/A') then 'activist_disclosure'

            -- Going Private / Delisting
            when form_type in ('SC 13E3', 'SC 13E3/A') then 'going_private'
            when form_type like '15-%' then 'delisting_notice'
            when form_type like '25%' then 'delisting_notification'

            -- Merger Registration
            when form_type like 'S-4%' or form_type like 'F-4%' then 'merger_registration'
        end as event_type,

        concat('SEC Filing: ', form_type) as description,
        null::numeric as split_from,
        null::numeric as split_to,
        form_type as filing_type,
        true as requires_price_adjustment,  -- All these forms indicate fundamental gap catalysts
        case
            -- Delisting Risk Events
            when form_type in ('DEFM14A', 'DEFM14C', 'DEFA14A') then true
            when form_type in ('SC TO-T', 'SC TO-T/A', 'SC TO-I', 'SC TO-I/A') then true
            when form_type in ('SC 13E3', 'SC 13E3/A') then true
            when form_type like '15-%' or form_type like '25%' then true
            else false
        end as potential_delisting,
        case
            -- Priority Scoring (lower = higher priority)
            when form_type in ('DEFM14A', 'DEFM14C') then 1
            when form_type in ('SC TO-T', 'SC TO-T/A') then 2
            when form_type in ('DEFA14A', 'DEFA14C') then 3
            when form_type = '425' then 4
            when form_type in ('SC 13E3', 'SC 13E3/A') then 5
            when form_type in ('SC TO-I', 'SC TO-I/A') then 6
            when form_type in ('SC 13D', 'SC 13D/A', 'SCHEDULE 13D', 'SCHEDULE 13D/A') then 7
            when form_type like '15-%' or form_type like '25%' then 10
            when form_type like 'S-4%' or form_type like 'F-4%' then 12
            else 80
        end as sort_priority,
        'fmp' as data_source,
        case
            -- Quality Scoring for FMP (All critical forms get high scores)
            when form_type in ('DEFM14A', 'DEFM14C', 'SC TO-T', 'SC TO-T/A', '425') then 99
            when form_type in ('DEFA14A', 'DEFA14C') then 98
            when form_type in ('SC 13D', 'SC 13D/A', 'SCHEDULE 13D', 'SCHEDULE 13D/A') then 95
            when form_type in ('SC 13E3', 'SC 13E3/A') then 94
            when form_type like '15-%' or form_type like '25%' then 88
            when form_type like 'S-4%' or form_type like 'F-4%' then 85
            else 60
        end as data_quality_score
    from {{ source('tap_fmp_production', 'sec_filings_by_symbol') }}
    where
        filing_date is not null
        and symbol is not null
        and trim(symbol) != ''
        -- Only include critical M&A and delisting forms
        and (
            form_type in ('DEFM14A', 'DEFM14C', 'DEFA14A', 'DEFA14C', '425',
                         'SC TO-T', 'SC TO-T/A', 'SC TO-I', 'SC TO-I/A', 'SC TO-C',
                         'SC 13D', 'SC 13D/A', 'SCHEDULE 13D', 'SCHEDULE 13D/A',
                         'SC 13E3', 'SC 13E3/A')
            or form_type like 'S-4%'
            or form_type like 'F-4%'
            or form_type like '15-%'
            or form_type like '25%'
        )
),

-- Polygon Data Sources (Fallback)
cte_dividends as (
    select
        ticker,
        ex_dividend_date as event_date,
        currency,
        sum(cash_amount) as cash_amount,
        'dividend' as event_type,
        concat('Dividend $', round(sum(cash_amount), 4), ' (', currency, ')') as description,
        null::numeric as split_from,
        null::numeric as split_to,
        null::text as filing_type,
        true as requires_price_adjustment,
        false as potential_delisting,
        10 as sort_priority,
        'polygon' as data_source,
        95 as data_quality_score
    from
        {{ source('tap_polygon_production', 'dividends') }}
    where
        ex_dividend_date is not null
        and cash_amount is not null
        and cash_amount > 0
    group by 1, 2, 3
),

cte_splits as (
    select
        ticker,
        execution_date as event_date,
        null::text as currency,
        null::numeric as cash_amount,
        case
            when split_from > split_to then 'reverse_split'
            else 'split'
        end as event_type,
        concat('Stock split ', split_from, ':', split_to, ' (', round(split_from::numeric / nullif(split_to, 0), 5), ':1)') as description,
        split_from,
        split_to,
        null::text as filing_type,
        true as requires_price_adjustment,
        false as potential_delisting,
        20 as sort_priority,
        'polygon' as data_source,
        95 as data_quality_score
    from
        {{ source('tap_polygon_production', 'splits') }}
),

cte_sec_filings as (
    select
        ticker,
        date as event_date,
        null::text as currency,
        null::numeric as cash_amount,
        case
            -- Definitive M&A Events (Highest Priority)
            when type = 'DEFM14A' then 'definitive_merger'
            when type = 'DEFA14A' then 'merger_proxy'
            when type in ('SC TO-T', 'SC TO-T/A') then 'tender_offer'
            when type = '425' then 'merger_communication'
            when type in ('SC 13D', 'SC 13D/A') then 'acquisition_disclosure'
            when type = 'SC 13G/A' then 'ownership_disclosure'
            when type = 'SC 13G' then 'ownership_disclosure'

            -- 8-K Classification
            when type = '8-K' and lower(coalesce(title, '')) ~ '.*(merger|acquisition|tender|buyout|takeover).*' then 'merger_8k'
            when type = '8-K' and lower(coalesce(title, '')) ~ '.*(spin.?off|divestiture|sale.*assets).*' then 'spinoff_8k'
            when type = '8-K' and lower(coalesce(title, '')) ~ '.*(bankruptcy|chapter.*11|chapter.*7|insolvency).*' then 'bankruptcy_8k'
            when type = '8-K' and lower(coalesce(title, '')) ~ '.*(delist|nasdaq.*removal|nyse.*removal).*' then 'delisting_8k'
            when type = '8-K' and lower(coalesce(title, '')) ~ '.*(restructur|reorganiz|recapitaliz).*' then 'restructuring_8k'
            when type = '8-K' and lower(coalesce(title, '')) ~ '.*(ceo|cfo|president).*resign.*' then 'executive_departure_8k'
            when type = '8-K' and lower(coalesce(title, '')) ~ '.*(dividend|distribution).*' then 'dividend_8k'
            when type = '8-K' and lower(coalesce(title, '')) ~ '.*(split|stock.*split).*' then 'split_8k'
            when type = '8-K' and lower(coalesce(title, '')) ~ '.*(earnings|results|financial).*' then 'earnings_8k'
            when type = '8-K' and lower(coalesce(title, '')) ~ '.*(agreement|contract|deal).*' then 'material_agreement_8k'
            when type = '8-K' and lower(coalesce(title, '')) ~ '.*(lawsuit|litigation|settlement).*' then 'litigation_8k'
            when type = '8-K' and lower(coalesce(title, '')) ~ '.*(guidance|outlook|forecast).*' then 'guidance_8k'
            when type = '8-K' and lower(coalesce(title, '')) ~ '.*(fda|approval|drug).*' then 'regulatory_8k'
            when type = '8-K' then 'material_event_8k'  -- Catch-all for other 8-K filings

            -- 8-K Amendments
            when type = '8-K/A' and lower(coalesce(title, '')) ~ '.*(merger|acquisition).*' then 'merger_8k_amendment'
            when type = '8-K/A' then 'material_event_8k_amendment'

            -- Other Important Filings
            when type = 'DEF 14A' and lower(coalesce(title, '')) ~ '.*(merger|acquisition|sale).*' then 'merger_proxy_def14a'
            when type = 'PREM14A' and lower(coalesce(title, '')) ~ '.*(merger|acquisition).*' then 'preliminary_merger_proxy'
            when type = 'PRE 14A' and lower(coalesce(title, '')) ~ '.*(merger|acquisition).*' then 'preliminary_proxy'
            when type = 'DFAN14A' then 'definitive_additional_proxy'
            when type = '10-K' and lower(coalesce(title, '')) ~ '.*(going.*concern|substantial.*doubt).*' then 'going_concern'
            when type = '10-K/A' and lower(coalesce(title, '')) ~ '.*(going.*concern|substantial.*doubt).*' then 'going_concern_amendment'
            when type = 'PX14A6G' then 'proxy_solicitation'
            when type = 'CORRESP' then 'sec_correspondence'
            when type = 'NT 10-K' then 'late_10k'
            when type = 'NT 10-Q' then 'late_10q'

            -- Routine filings (lower priority)
            when type = '10-Q' then 'quarterly_report'
            when type = '6-K' then 'foreign_report'
            when type = '20-F' then 'annual_foreign_report'
            when type = 'S-8' then 'stock_plan_registration'
            when type = 'S-3ASR' then 'shelf_registration'
            when type = '11-K' then 'employee_plan_annual'
            when type = 'ARS' then 'annual_report_summary'
            when type = 'SD' then 'specialized_disclosure'

            else 'other_sec_filing'
        end as event_type,
        
        coalesce(title, concat('SEC Filing: ', type)) as description,
        null::numeric as split_from,
        null::numeric as split_to,
        type as filing_type,
        case
            -- High Price Impact Events
            when type in ('DEFM14A', 'DEFA14A', 'SC TO-T', 'SC TO-T/A', '425') then true
            when type in ('SC 13D', 'SC 13D/A', 'SC 13G', 'SC 13G/A') then true
            when type = '8-K' and lower(coalesce(title, '')) ~ '.*(merger|acquisition|tender|spinoff|bankruptcy|split|dividend.*special).*' then true
            when type = 'DFAN14A' then true
            else false
        end as requires_price_adjustment,
        case
            -- Delisting Risk Events
            when type in ('DEFM14A', 'DEFA14A', 'SC TO-T', 'SC TO-T/A') then true
            when type = '8-K' and lower(coalesce(title, '')) ~ '.*(merger|acquisition|delisting|bankruptcy|going.*private).*' then true
            when type = '10-K' and lower(coalesce(title, '')) ~ '.*(going.*concern|substantial.*doubt).*' then true
            else false
        end as potential_delisting,
        case
            -- Priority Scoring (lower = higher priority)
            when type = 'DEFM14A' then 1
            when type = 'SC TO-T' then 2
            when type = 'DEFA14A' then 3
            when type = '425' then 4
            when type = '8-K' and lower(coalesce(title, '')) ~ '.*(merger|acquisition).*' then 5
            when type = '8-K' and lower(coalesce(title, '')) ~ '.*(tender|buyout).*' then 6
            when type = 'SC 13D' then 7
            when type = '8-K' and lower(coalesce(title, '')) ~ '.*(spinoff|split).*' then 8
            when type = '8-K' and lower(coalesce(title, '')) ~ '.*(bankruptcy|delisting).*' then 9
            when type = 'DFAN14A' then 10
            when type = '8-K' and lower(coalesce(title, '')) ~ '.*(dividend.*special).*' then 11
            when type = '8-K' and lower(coalesce(title, '')) ~ '.*(ceo|cfo).*resign.*' then 12
            when type = '8-K' and lower(coalesce(title, '')) ~ '.*(fda|approval).*' then 13
            when type = '8-K' and lower(coalesce(title, '')) ~ '.*(earnings|guidance).*' then 14
            when type = '8-K' then 15  -- Other material 8-K events
            when type = 'SC 13G' then 20
            when type = 'PREM14A' then 25
            when type = 'PRE 14A' then 30
            when type = '8-K/A' then 35
            when type = 'DEF 14A' then 40
            when type in ('NT 10-K', 'NT 10-Q') then 50
            when type = 'PX14A6G' then 60
            when type = 'CORRESP' then 70
            else 80
        end as sort_priority,
        'yahoo' as data_source,
        case
            -- Quality Scoring
            when type in ('DEFM14A', 'SC TO-T', '425') then 99
            when type = 'DEFA14A' then 98
            when type = '8-K' and lower(coalesce(title, '')) ~ '.*(merger|acquisition|tender).*' then 95
            when type in ('SC 13D', 'SC 13D/A') then 90
            when type = '8-K' and lower(coalesce(title, '')) ~ '.*(spinoff|split|bankruptcy).*' then 88
            when type = 'DFAN14A' then 85
            when type = '8-K' and lower(coalesce(title, '')) ~ '.*(dividend.*special|ceo.*resign|fda).*' then 80
            when type = '8-K' and lower(coalesce(title, '')) ~ '.*(earnings|guidance).*' then 75
            when type = '8-K' then 70  -- Other material 8-K events
            when type in ('PREM14A', 'SC 13G') then 75
            when type in ('PRE 14A', 'DEF 14A') then 70
            when type in ('8-K/A', 'NT 10-K', 'NT 10-Q') then 65
            else 50
        end as data_quality_score
    from {{ ref('yahoo_sec_filings') }}
    where
        date is not null
        and ticker is not null
        and trim(ticker) != ''
),

cte_ticker_changes as (
    select
        ticker,
        date as event_date,
        null::text as currency,
        null::numeric as cash_amount,
        'ticker_change' as event_type,
        concat('Ticker Change: ', ticker, ' → ', name) as description,
        null::numeric as split_from,
        null::numeric as split_to,
        type as filing_type,
        false as requires_price_adjustment,
        false as potential_delisting,
        90 as sort_priority,
        'polygon' as data_source,
        75 as data_quality_score
    from
        {{ source('tap_polygon_production', 'ticker_events') }}
    where
        date is not null
        and ticker is not null
        and trim(ticker) != ''
        and type = 'ticker_change'
),

cte_all_corporate_actions as (
    -- FMP Data (Primary - Higher Quality)
    select * from cte_fmp_dividends
    union all
    select * from cte_fmp_splits
    union all
    select * from cte_fmp_symbol_changes
    union all
    select * from cte_fmp_sec_filings
    union all
    -- Polygon/Yahoo Data (Fallback)
    select * from cte_dividends
    union all
    select * from cte_splits
    union all
    select * from cte_sec_filings
    union all
    select * from cte_ticker_changes
),

-- Deduplication: Prioritize FMP over Polygon/Yahoo for same ticker+date+event_type
cte_deduplicated as (
    select
        *,
        row_number() over (
            partition by ticker, event_date, event_type
            order by data_quality_score desc, data_source
        ) as rn
    from cte_all_corporate_actions
),

cte_lag as (
    select
        ticker,
        event_date,
        currency,
        cash_amount,
        event_type,
        description,
        split_from,
        split_to,
        filing_type,
        requires_price_adjustment,
        potential_delisting,
        sort_priority,
        data_source,
        data_quality_score,
        lag(event_type) over (partition by ticker order by event_date, sort_priority) as prev_event_type,
        lag(event_date) over (partition by ticker order by event_date, sort_priority) as prev_event_date,
        lead(event_type) over (partition by ticker order by event_date, sort_priority) as next_event_type,
        lead(event_date) over (partition by ticker order by event_date, sort_priority) as next_event_date,
        event_date - lag(event_date) over (partition by ticker order by event_date, sort_priority) as days_since_last_event,
        count(*) over (partition by ticker, event_date) as events_same_date
    from cte_deduplicated
    where rn = 1
)

select
    coalesce(tm.composite_figi, 'UNMAPPED') as composite_figi,
    coalesce(tm.cik, 'UNMAPPED') as cik,
    ea.ticker,
    coalesce(tm.old_ticker, ea.ticker) as old_ticker,
    ea.event_date,
    ea.event_type,
    ea.description,
    ea.cash_amount,
    ea.currency,
    ea.split_from,
    ea.split_to,
    case
        when ea.split_to > 0 then round(ea.split_from::numeric / ea.split_to, 6)
        else null
    end as split_ratio,
    ea.filing_type,
    ea.requires_price_adjustment,
    ea.potential_delisting,
    ea.data_quality_score,

    case
        when ea.event_type in ('definitive_merger', 'tender_offer', 'merger_8k', 'acquisition_8k') then 'high_impact_ma'
        when ea.event_type in ('acquisition_disclosure', 'merger_proxy', 'merger_communication') then 'medium_impact_ma'
        when ea.event_type in ('spinoff_8k', 'bankruptcy_8k', 'delisting_8k') then 'high_impact_corporate'
        when ea.event_type in ('split', 'reverse_split') then 'price_adjustment_required'
        when ea.event_type = 'dividend' and ea.cash_amount >= 1.0 then 'high_dividend'
        when ea.event_type = 'dividend' then 'regular_dividend'
        when ea.event_type in ('going_concern', 'late_10k', 'late_10q') then 'distress_signal'
        else 'monitor'
    end as trading_signal_flag,

    case
        when ea.potential_delisting = true then 'extreme'
        when ea.event_type in ('definitive_merger', 'tender_offer') then 'high'
        when ea.requires_price_adjustment = true then 'medium'
        when ea.event_type in ('going_concern', 'bankruptcy_8k') then 'high'
        else 'low'
    end as trading_risk_level,

    ea.sort_priority,
    ea.data_source,
    ea.prev_event_type,
    ea.prev_event_date,
    ea.next_event_type,
    ea.next_event_date,
    ea.days_since_last_event,
    ea.events_same_date,
    case when tm.ticker is not null then true else false end as has_ticker_mapping,
    current_timestamp as created_at,
    '{{ run_started_at }}' as data_version

from
    cte_lag ea
left join {{ ref('polygon_ticker_map') }} tm on ea.ticker = tm.ticker

order by ea.ticker, ea.event_date, ea.sort_priority