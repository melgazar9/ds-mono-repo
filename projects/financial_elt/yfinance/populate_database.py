from yf_elt_utils import *

start = time.time()

# populate a mysql database frequently (after each ticker per interval is pulled), then populate snowflake and/or
# bigquery after the interval iteration is complete... this only amounts to ~10 API calls to bigquery / snowflake per
# run, as opposed to over 50k API calls when setting dwh='snowflake' or dwh='bigquery'

pipeline = YFinanceELT(dwh='mysql', populate_snowflake=True, populate_bigquery=True)

pipeline.connect_to_db()
pipeline.elt_stock_tickers()
pipeline.elt_stock_prices(batch_download=True)

start_fix = time.time()

pipeline.fix_missing_ticker_intervals()

print(f'\nYahoo finance ELT process took {round((start_fix - start) / 60, 3)} minutes.\n')
print(f'\nTicker fixes took {round((time.time() - start_fix) / 60, 3)} minutes.\n')
print(f'\nTotal time took {round((time.time() - start) / 60, 3)} minutes.\n')