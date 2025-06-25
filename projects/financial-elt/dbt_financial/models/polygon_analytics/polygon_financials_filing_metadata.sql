{{ config(
    materialized='table',
    schema='polygon_analytics',
    indexes=[
        {'columns': ['ticker', 'filing_date'], 'unique': false},
        {'columns': ['filing_id'], 'unique': true},
        {'columns': ['cik'], 'unique': false}
    ]
) }}

SELECT
    -- Create unique filing ID
    MD5(cik || '|' || filing_date::text || '|' || fiscal_period || '|' || fiscal_year::text) as filing_id,

    -- Company identifiers
    cik,
    company_name,
    CASE
        WHEN tickers IS NOT NULL AND jsonb_typeof(tickers) = 'array' AND jsonb_array_length(tickers) > 0
        THEN TRIM('"' FROM (tickers->0)::text)
        WHEN tickers IS NOT NULL AND jsonb_typeof(tickers) = 'string'
        THEN TRIM('"' FROM tickers::text)
        ELSE NULL
    END as ticker,

    -- Filing metadata
    filing_date,
    fiscal_year::int as fiscal_year,
    fiscal_period,
    timeframe,
    sic,
    start_date,
    end_date,
    acceptance_datetime,

    -- URLs and references
    source_filing_url,
    source_filing_file_url,
    SUBSTRING(source_filing_url FROM '([0-9]{10}-[0-9]{2}-[0-9]{6})$') as accession_number,

    -- System fields
    _sdc_extracted_at as extracted_at,
    CURRENT_TIMESTAMP as created_at

FROM {{ source('tap_polygon_production', 'financials') }}
WHERE filing_date >= '2010-01-01'
  AND company_name IS NOT NULL
  AND CASE
      WHEN tickers IS NOT NULL AND jsonb_typeof(tickers) = 'array' AND jsonb_array_length(tickers) > 0
      THEN TRIM('"' FROM (tickers->0)::text)
      WHEN tickers IS NOT NULL AND jsonb_typeof(tickers) = 'string'
      THEN TRIM('"' FROM tickers::text)
      ELSE NULL
  END IS NOT NULL