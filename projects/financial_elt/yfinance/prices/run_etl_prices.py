from etl_price_utils import *

###### this main driver file acts as interface for running ETL process ######

start = time.time()

intervals_to_download = ('1m', '2m', '5m', '1h', '1d')
yf_params = {'threads': False}

### run the pipeline ###

pipeline = YFPriceETL(populate_mysql=False,
                      populate_snowflake=False,
                      populate_bigquery=True,
                      num_workers=1)

pipeline.connect_to_dwhs()

### stocks ###

pipeline.etl_stock_tickers()
pipeline.etl_stock_prices(batch_download=False,
                          intervals_to_download=intervals_to_download,
                          write_to_db_after_interval_complete=True,
                          yf_params=yf_params)

### crypto ###

pipeline.etl_top_250_crypto_tickers()
# pipeline.etl_crypto_prices(batch_download=False,
#                            intervals_to_download=intervals_to_download,
#                            write_to_db_after_interval_complete=True,
#                            yf_params=yf_params)

### forex ###

# pipeline.etl_forex_tickers()
# pipeline.etl_forex_prices(batch_download=False,
#                           intervals_to_download=intervals_to_download,
#                           write_to_db_after_interval_complete=True,
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
