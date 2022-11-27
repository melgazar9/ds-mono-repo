from yf_elt_utils import *

start = time.time()

# populate a mysql database frequently (after each ticker per interval is pulled), then populate snowflake and/or
# bigquery after the interval iteration is complete... this only amounts to ~10 API calls to bigquery / snowflake per
# run, as opposed to over 50k API calls when setting dwh='snowflake' or dwh='bigquery'

yf_el = YFinanceEL(dwh='mysql', populate_snowflake=True, populate_bigquery=True)

yf_el.connect_to_db()
yf_el.el_stock_tickers()
yf_el.el_stock_prices(batch_download=True)

print(f'\nYahoo finance ELT process took {round((time.time() - start) / 60, 3)} minutes.\n')
