{{ config(
    materialized='table',
    schema='polygon_analytics',
    indexes=[
        {'columns': ['filing_id'], 'unique': true},
        {'columns': ['ticker', 'filing_date'], 'unique': false}
    ]
) }}

WITH base AS (
    SELECT
        MD5(cik || '|' || filing_date::text || '|' || fiscal_period || '|' || fiscal_year::text) as filing_id,
        CASE
            WHEN tickers IS NOT NULL AND jsonb_typeof(tickers) = 'array' AND jsonb_array_length(tickers) > 0
            THEN TRIM('"' FROM (tickers->0)::text)
            WHEN tickers IS NOT NULL AND jsonb_typeof(tickers) = 'string'
            THEN TRIM('"' FROM tickers::text)
        END as ticker,
        filing_date,
        financials->'balance_sheet' as balance_sheet
    FROM {{ source('tap_polygon_production', 'financials') }}
    WHERE financials->'balance_sheet' IS NOT NULL
),

balance_sheet_items AS (
    SELECT
        filing_id,
        ticker,
        filing_date,

        -- Assets
        (balance_sheet->'assets'->>'value')::numeric as total_assets,
        (balance_sheet->'assets'->>'unit') as total_assets_unit,
        (balance_sheet->'current_assets'->>'value')::numeric as current_assets,
        (balance_sheet->'current_assets'->>'unit') as current_assets_unit,
        (balance_sheet->'noncurrent_assets'->>'value')::numeric as noncurrent_assets,
        (balance_sheet->'fixed_assets'->>'value')::numeric as fixed_assets,
        (balance_sheet->'intangible_assets'->>'value')::numeric as intangible_assets,

        -- Specific Asset Categories
        (balance_sheet->'cash_and_cash_equivalents_at_carrying_value'->>'value')::numeric as cash_and_equivalents,
        (balance_sheet->'inventory'->>'value')::numeric as inventory,
        (balance_sheet->'accounts_receivable_net'->>'value')::numeric as accounts_receivable,
        (balance_sheet->'goodwill'->>'value')::numeric as goodwill,

        -- Liabilities
        (balance_sheet->'liabilities'->>'value')::numeric as total_liabilities,
        (balance_sheet->'liabilities'->>'unit') as total_liabilities_unit,
        (balance_sheet->'current_liabilities'->>'value')::numeric as current_liabilities,
        (balance_sheet->'noncurrent_liabilities'->>'value')::numeric as noncurrent_liabilities,
        (balance_sheet->'accounts_payable'->>'value')::numeric as accounts_payable,
        (balance_sheet->'long_term_debt_noncurrent'->>'value')::numeric as long_term_debt,
        (balance_sheet->'short_term_borrowings'->>'value')::numeric as short_term_debt,

        -- Equity
        (balance_sheet->'equity'->>'value')::numeric as total_equity,
        (balance_sheet->'equity'->>'unit') as total_equity_unit,
        (balance_sheet->'equity_attributable_to_parent'->>'value')::numeric as equity_attributable_to_parent,
        (balance_sheet->'equity_attributable_to_noncontrolling_interest'->>'value')::numeric as noncontrolling_interest,
        (balance_sheet->'retained_earnings_accumulated_deficit'->>'value')::numeric as retained_earnings,
        (balance_sheet->'common_stocks_including_additional_paid_in_capital'->>'value')::numeric as common_stock_and_apic,

        CURRENT_TIMESTAMP as created_at

    FROM base
)

SELECT * FROM balance_sheet_items
WHERE filing_id IS NOT NULL
  AND ticker IS NOT NULL
ORDER BY ticker, filing_date DESC