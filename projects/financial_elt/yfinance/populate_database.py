from yfinance_elt_utils import *

start = time.time()

yf_el = YFinanceEL(dwh='bigquery')

yf_el.connect_to_db()
yf_el.el_stock_tickers()
yf_el.el_stock_prices()

print(f'\nYahoo finance ELT process took {round((time.time() - start) / 60, 3)} minutes.\n')