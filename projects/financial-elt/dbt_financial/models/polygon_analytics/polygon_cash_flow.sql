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
        financials->'cash_flow_statement' as cash_flow
    FROM {{ source('tap_polygon_production', 'financials') }}
    WHERE financials->'cash_flow_statement' IS NOT NULL
),

cash_flow_items AS (
    SELECT
        filing_id,
        ticker,
        filing_date,

        -- Operating Activities
        (cash_flow->'net_cash_flow_from_operating_activities'->>'value')::numeric as operating_cash_flow,
        (cash_flow->'net_cash_flow_from_operating_activities'->>'unit') as operating_cash_flow_unit,
        (cash_flow->'net_cash_flow_from_operating_activities_continuing'->>'value')::numeric as operating_cash_flow_continuing,

        -- Investing Activities
        (cash_flow->'net_cash_flow_from_investing_activities'->>'value')::numeric as investing_cash_flow,
        (cash_flow->'net_cash_flow_from_investing_activities'->>'unit') as investing_cash_flow_unit,
        (cash_flow->'net_cash_flow_from_investing_activities_continuing'->>'value')::numeric as investing_cash_flow_continuing,

        -- Financing Activities
        (cash_flow->'net_cash_flow_from_financing_activities'->>'value')::numeric as financing_cash_flow,
        (cash_flow->'net_cash_flow_from_financing_activities'->>'unit') as financing_cash_flow_unit,
        (cash_flow->'net_cash_flow_from_financing_activities_continuing'->>'value')::numeric as financing_cash_flow_continuing,

        -- Net Cash Flow
        (cash_flow->'net_cash_flow'->>'value')::numeric as net_cash_flow,
        (cash_flow->'net_cash_flow'->>'unit') as net_cash_flow_unit,
        (cash_flow->'net_cash_flow_continuing'->>'value')::numeric as net_cash_flow_continuing,

        -- Specific Cash Flow Items
        (cash_flow->'depreciation_depletion_and_amortization'->>'value')::numeric as depreciation_amortization,
        (cash_flow->'stock_based_compensation'->>'value')::numeric as stock_based_compensation,
        (cash_flow->'payments_to_acquire_property_plant_and_equipment'->>'value')::numeric as capex_payments,
        (cash_flow->'proceeds_from_issuance_of_common_stock'->>'value')::numeric as common_stock_issuance,
        (cash_flow->'payments_of_dividends'->>'value')::numeric as dividend_payments,
        (cash_flow->'repurchase_of_common_stock'->>'value')::numeric as share_repurchases,

        -- Exchange Rate Effects
        (cash_flow->'exchange_gains_losses'->>'value')::numeric as exchange_rate_effects,

        CURRENT_TIMESTAMP as created_at

    FROM base
)

SELECT * FROM cash_flow_items
WHERE filing_id IS NOT NULL
  AND ticker IS NOT NULL
ORDER BY ticker, filing_date DESC