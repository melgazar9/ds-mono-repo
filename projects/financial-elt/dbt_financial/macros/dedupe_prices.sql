{% macro dedupe_prices() %}
    {% set price_tables = [
        'crypto_prices_1m',
        'crypto_prices_2m',
        'crypto_prices_5m',
        'crypto_prices_1h',
        'crypto_prices_1d',
        'futures_prices_1m',
        'futures_prices_2m',
        'futures_prices_5m',
        'futures_prices_1h',
        'futures_prices_1d',
        'forex_prices_1m',
        'forex_prices_2m',
        'forex_prices_5m',
        'forex_prices_1h',
        'forex_prices_1d',
        'stock_prices_1m',
        'stock_prices_2m',
        'stock_prices_5m',
        'stock_prices_1h',
        'stock_prices_1d',
    ] %}

    {% for price_table in price_tables %}
        {% set delete_sql %}
            DELETE FROM {{ source('tap_yfinance_dev', price_table) }} T
            WHERE ctid IN (
                SELECT ctid FROM (
                    SELECT
                        ctid,
                        ROW_NUMBER() OVER (
                            PARTITION BY ticker, "timestamp"
                            ORDER BY _sdc_received_at DESC, volume DESC, ctid
                        ) AS rn
                    FROM {{ source('tap_yfinance_dev', price_table) }}
                ) ranked_rows
                WHERE rn > 1
            );
        {% endset %}

        {{ log("Running dedupe on " ~ price_table, info=True) }}
        {% do run_query(delete_sql) %}
    {% endfor %}
{% endmacro %}
