{{ config(
    materialized='view',
    schema='polygon_analytics'
) }}

SELECT
    f.filing_id,
    f.ticker,
    f.company_name,
    f.filing_date,
    f.fiscal_year,
    f.fiscal_period,
    f.timeframe,
    f.accession_number,

    -- Income Statement (in millions)
    ROUND(COALESCE(i.revenues, 0) / 1000000, 2) as revenue_millions,
    ROUND(COALESCE(i.net_income_loss, 0) / 1000000, 2) as net_income_millions,
    ROUND(COALESCE(i.operating_income_loss, 0) / 1000000, 2) as operating_income_millions,
    ROUND(COALESCE(i.gross_profit, 0) / 1000000, 2) as gross_profit_millions,

    -- Balance Sheet (in millions)
    ROUND(COALESCE(b.total_assets, 0) / 1000000, 2) as total_assets_millions,
    ROUND(COALESCE(b.total_liabilities, 0) / 1000000, 2) as total_liabilities_millions,
    ROUND(COALESCE(b.total_equity, 0) / 1000000, 2) as total_equity_millions,
    ROUND(COALESCE(b.current_assets, 0) / 1000000, 2) as current_assets_millions,
    ROUND(COALESCE(b.current_liabilities, 0) / 1000000, 2) as current_liabilities_millions,

    -- Cash Flow (in millions)
    ROUND(COALESCE(c.operating_cash_flow, 0) / 1000000, 2) as operating_cash_flow_millions,
    ROUND(COALESCE(c.investing_cash_flow, 0) / 1000000, 2) as investing_cash_flow_millions,
    ROUND(COALESCE(c.financing_cash_flow, 0) / 1000000, 2) as financing_cash_flow_millions,

    -- Key Ratios
    CASE
        WHEN b.current_liabilities > 0 THEN ROUND(b.current_assets / b.current_liabilities, 2)
        ELSE NULL
    END as current_ratio,

    CASE
        WHEN i.revenues > 0 THEN ROUND(i.net_income_loss / i.revenues, 4)
        ELSE NULL
    END as net_margin,

    -- URLs for source verification
    f.source_filing_url,
    f.source_filing_file_url

FROM {{ ref('polygon_financials_filing_metadata') }} f
LEFT JOIN {{ ref('polygon_income_statement') }} i ON f.filing_id = i.filing_id
LEFT JOIN {{ ref('polygon_balance_sheet') }} b ON f.filing_id = b.filing_id
LEFT JOIN {{ ref('polygon_cash_flow') }} c ON f.filing_id = c.filing_id

ORDER BY f.ticker, f.filing_date DESC