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
        financials->'income_statement' as income_stmt
    FROM {{ source('tap_polygon_production', 'financials') }}
    WHERE financials->'income_statement' IS NOT NULL
),

-- Extract all income statement line items dynamically
income_items AS (
    SELECT
        filing_id,
        ticker,
        filing_date,

        -- Core Revenue Items
        (income_stmt->'revenues'->>'value')::numeric as revenues,
        (income_stmt->'revenues'->>'unit') as revenues_unit,
        (income_stmt->'revenues'->>'source') as revenues_source,

        -- Cost and Expenses
        (income_stmt->'cost_of_revenue'->>'value')::numeric as cost_of_revenue,
        (income_stmt->'cost_of_revenue'->>'unit') as cost_of_revenue_unit,
        (income_stmt->'operating_expenses'->>'value')::numeric as operating_expenses,
        (income_stmt->'operating_expenses'->>'unit') as operating_expenses_unit,

        -- Profit Metrics
        (income_stmt->'gross_profit'->>'value')::numeric as gross_profit,
        (income_stmt->'gross_profit'->>'unit') as gross_profit_unit,
        (income_stmt->'operating_income_loss'->>'value')::numeric as operating_income_loss,
        (income_stmt->'operating_income_loss'->>'unit') as operating_income_loss_unit,

        -- Net Income
        (income_stmt->'net_income_loss'->>'value')::numeric as net_income_loss,
        (income_stmt->'net_income_loss'->>'unit') as net_income_loss_unit,
        (income_stmt->'net_income_loss'->>'source') as net_income_loss_source,

        -- Other Income/Expenses
        (income_stmt->'nonoperating_income_loss'->>'value')::numeric as nonoperating_income_loss,
        (income_stmt->'interest_expense_operating'->>'value')::numeric as interest_expense_operating,
        (income_stmt->'income_tax_expense_benefit'->>'value')::numeric as income_tax_expense_benefit,

        -- Per Share Data
        (income_stmt->'basic_earnings_per_share'->>'value')::numeric as basic_earnings_per_share,
        (income_stmt->'diluted_earnings_per_share'->>'value')::numeric as diluted_earnings_per_share,
        (income_stmt->'weighted_average_shares_outstanding_basic'->>'value')::numeric as weighted_average_shares_basic,
        (income_stmt->'weighted_average_shares_outstanding_diluted'->>'value')::numeric as weighted_average_shares_diluted,

        -- Specialized Items
        (income_stmt->'research_and_development'->>'value')::numeric as research_and_development,
        (income_stmt->'selling_general_and_administrative_expenses'->>'value')::numeric as selling_general_administrative,

        CURRENT_TIMESTAMP as created_at

    FROM base
)

SELECT * FROM income_items
WHERE filing_id IS NOT NULL
  AND ticker IS NOT NULL
ORDER BY ticker, filing_date DESC