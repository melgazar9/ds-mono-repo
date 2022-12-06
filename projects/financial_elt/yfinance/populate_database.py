from yf_elt_utils import *

start = time.time()

# populate a mysql database frequently (after each ticker per interval is pulled), then populate snowflake and/or
# bigquery after the interval iteration is complete... this only amounts to ~10 API calls to bigquery / snowflake per
# run, as opposed to over 50k API calls when setting dwh='snowflake' or dwh='bigquery'

yf_elt = YFinanceELT(dwh='mysql', populate_snowflake=False, populate_bigquery=True)

yf_elt.connect_to_db()
yf_elt.elt_stock_tickers()
yf_elt.elt_stock_prices(batch_download=False)

start_fix = time.time()

yf_elt.fix_missing_ticker_intervals()

print(f'\nYahoo finance ELT process took {round((start_fix - start) / 60, 3)} minutes.\n')
print(f'\nTicker fixes took {round((time.time() - start_fix) / 60, 3)} minutes.\n')
print(f'\nTotal time took {round((time.time() - start) / 60, 3)} minutes.\n')