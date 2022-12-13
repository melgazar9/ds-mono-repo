from yf_elt_utils import *

start = time.time()

intervals_to_download = ('1m', '1d')

### run the pipeline ###

pipeline = YFinanceELT(dwh='bigquery', num_workers=3)

pipeline.connect_to_db()
pipeline.elt_stock_tickers()
pipeline.elt_stock_prices(batch_download=True, intervals_to_download=intervals_to_download)

start_fix = time.time()

pipeline.fix_missing_ticker_intervals(intervals_to_fix=intervals_to_download)

print(f'\nYahoo finance ELT process took {round((start_fix - start) / 60, 3)} minutes.\n')
print(f'\nTicker fixes took {round((time.time() - start_fix) / 60, 3)} minutes.\n')
print(f'\nTotal time took {round((time.time() - start) / 60, 3)} minutes.\n')