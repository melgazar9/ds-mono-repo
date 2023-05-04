from etl_stock_price_utils import *

start = time.time()

intervals_to_download = ('1m', '2m', '5m', '1h', '1d')
yf_params = {'threads': False}


### run the pipeline ###

pipeline = YFPriceETL(populate_snowflake=True, populate_mysql=True, populate_bigquery=True, num_workers=1, debug_tickers=['AAPL', 'MSFT', 'SQ'])

pipeline.connect_to_dwhs()

pipeline.etl_stock_tickers()
pipeline.etl_stock_prices(batch_download=False,
                          intervals_to_download=intervals_to_download,
                          write_to_db_after_interval_complete=False,
                          yf_params=yf_params)

pipeline.close_dwh_connections()

### TODO: integrate fix missing ticker timestamps ###

# start_fix = time.time()

# pipeline.fix_missing_ticker_intervals(intervals_to_fix=intervals_to_download, yf_params=yf_params)

# print(f'\nYahoo finance ETL process took {round((start_fix - start) / 60, 3)} minutes.\n')
# print(f'\nTicker fixes took {round((time.time() - start_fix) / 60, 3)} minutes.\n')

print(f'\nTotal time took {round((time.time() - start) / 60, 3)} minutes.\n')
