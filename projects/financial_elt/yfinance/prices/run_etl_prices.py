from etl_price_utils import *

###### this is the main driver file that acts as an interface for running the ETL process ######

ENVIRONMENT = os.getenv('ENVIRONMENT').lower()
assert ENVIRONMENT in ('dev', 'production')

if ENVIRONMENT == 'dev':
    SCHEMA = 'yfinance_el_dev'
elif ENVIRONMENT == 'production':
    SCHEMA = 'yfinance_el_production'

print(f'\n*** Running Environment {ENVIRONMENT} || Populating schema {SCHEMA} ***\n')

try:
    start = time.time()
    intervals_to_download = ('1m', '2m', '5m', '1h', '1d')
    yf_params = {'repair': True, 'auto_adjust': True, 'back_adjust': False, 'timeout': 300, 'raise_errors': False}

    ###### run the pipelines ######

    pipeline = YFPriceETL(schema=SCHEMA, populate_mysql=True, populate_snowflake=True, populate_bigquery=False)
    pipeline.connect_to_dwhs()

    gc.collect()

    ### stocks ###

    pipeline.etl_stock_tickers()
    pipeline.etl_prices(asset_class='stocks', debug_tickers=['AAPL', 'TSLA', 'MSFT'],
                        intervals_to_download=intervals_to_download,
                        write_to_db_after_interval_completes=False,
                        write_to_db_after_n_tickers=100,
                        yf_params=yf_params)

    gc.collect()

    ### forex ###

    pipeline.etl_forex_pairs()
    pipeline.etl_prices(asset_class='forex',debug_tickers=['EURUSD=X', 'RUB=X'],
                        intervals_to_download=intervals_to_download,
                        write_to_db_after_interval_completes=False,
                        write_to_db_after_n_tickers=100,
                        yf_params=yf_params)

    gc.collect()

    ### crypto ###

    pipeline.etl_top_250_crypto_tickers()
    pipeline.etl_prices(asset_class='crypto',debug_tickers=['BTC-USD', 'ETH-USD'],
                        intervals_to_download=intervals_to_download,
                        write_to_db_after_interval_completes=False,
                        write_to_db_after_n_tickers=100,
                        yf_params=yf_params)

    pipeline.close_dwh_connections()

    gc.collect()

    # remove old log files
    if 'logs' in os.listdir():
        remove_old_contents('logs')

    print(f'\nTotal time took {round((time.time() - start) / 60, 3)} minutes.\n')

except Exception as e:
    import traceback

    print(traceback.format_exc())
    print(f'\n{e}\n')

    email_credentials = json_string_to_dict(os.getenv('EMAIL_CREDENTIALS'))
    subject = f"Financial ELT Failed in {ENVIRONMENT} environment {datetime.today().strftime('%Y-%m-%d %H:%S:%S')}."
    body = "The script encountered an error:\n\n{}\n\n{}".format(str(traceback.format_exc()), str(e))

    send_email(to_addrs=email_credentials['username'],
               from_addr=email_credentials['username'],
               subject=subject,
               body=body,
               password=email_credentials['password'])
