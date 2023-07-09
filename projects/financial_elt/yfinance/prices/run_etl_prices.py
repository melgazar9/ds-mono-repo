from etl_price_utils import *

###### this is the main driver file that acts as an interface for running the ETL process ######

ENVIRONMENT = os.getenv('ENVIRONMENT').lower()
assert ENVIRONMENT in ('dev', 'production')

if ENVIRONMENT == 'dev':
    SCHEMA = 'yfinance_dev'
elif ENVIRONMENT == 'production':
    SCHEMA = 'yfinance'

print(f'\n*** Running Environment {ENVIRONMENT} || Populating schema {SCHEMA} ***\n')

start = time.time()

intervals_to_download = ('1m', '2m', '5m', '1h', '1d')
yf_params = {'repair': True, 'auto_adjust': True, 'back_adjust': False, 'timeout': 300, 'raise_errors': False}


###### run the pipelines ######

pipeline = YFPriceETL(schema=SCHEMA, populate_mysql=True, populate_snowflake=False, populate_bigquery=False)

pipeline.connect_to_dwhs()

### stocks ###

pipeline.etl_stock_tickers()
pipeline.etl_prices(asset_class='stocks',
                    intervals_to_download=intervals_to_download,
                    write_to_db_after_interval_completes=True,
                    yf_params=yf_params)

# ### forex ###

pipeline.etl_forex_pairs()
pipeline.etl_prices(asset_class='forex',
                    intervals_to_download=intervals_to_download,
                    write_to_db_after_interval_completes=True,
                    yf_params=yf_params)


### crypto ###

pipeline.etl_top_250_crypto_tickers()
pipeline.etl_prices(asset_class='crypto',
                    intervals_to_download=intervals_to_download,
                    write_to_db_after_interval_completes=True,
                    yf_params=yf_params)


pipeline.close_dwh_connections()

# remove old log files
if 'logs' in os.listdir():
    remove_old_contents('logs')

print(f'\nTotal time took {round((time.time() - start) / 60, 3)} minutes.\n')