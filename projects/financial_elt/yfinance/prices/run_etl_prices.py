from etl_price_utils import *

###### this main driver file acts as interface for running ETL process ######

ENVIRONMENT = os.getenv('ENVIRONMENT').lower()
assert ENVIRONMENT in ('dev', 'production')

if ENVIRONMENT == 'dev':
    SCHEMA = 'yfinance_dev'
elif ENVIRONMENT == 'production':
    SCHEMA = 'yfinance'

start = time.time()

intervals_to_download = ('1m', '2m', '5m', '1h', '1d')
yf_params = {'repair': True, 'auto_adjust': True, 'back_adjust': False, 'timeout': 300, 'raise_errors': False}


###### run the pipelines ######

pipeline = YFPriceETL(schema=SCHEMA, populate_mysql=True, populate_snowflake=False, populate_bigquery=False)

pipeline.connect_to_dwhs()

### stocks ###

pipeline.etl_stock_tickers()
pipeline.etl_stock_prices(batch_download=False,
                          intervals_to_download=intervals_to_download,
                          write_to_db_after_interval_completes=True,
                          yf_params=yf_params)

### crypto ###

pipeline.etl_top_250_crypto_tickers()
# pipeline.etl_crypto_prices(batch_download=False,
#                            intervals_to_download=intervals_to_download,
#                            write_to_db_after_interval_completes=True,
#                            yf_params=yf_params)

### forex ###

pipeline.etl_forex_pairs()
# pipeline.etl_forex_prices(intervals_to_download=intervals_to_download,
#                           write_to_db_after_interval_completes=True,
#                           yf_params=yf_params)


pipeline.close_dwh_connections()

### TODO: integrate fix missing ticker timestamps ###

# start_fix = time.time()
# pipeline.fix_missing_ticker_intervals(intervals_to_fix=intervals_to_download, yf_params=yf_params)

# print(f'\nYahoo finance ETL process took {round((start_fix - start) / 60, 3)} minutes.\n')
# print(f'\nTicker fixes took {round((time.time() - start_fix) / 60, 3)} minutes.\n')


# remove old log files
if 'logs' in os.listdir():
    remove_old_contents('logs')

print(f'\nTotal time took {round((time.time() - start) / 60, 3)} minutes.\n')
