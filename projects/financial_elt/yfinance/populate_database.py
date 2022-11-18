from yfinance_elt_utils import *

yf_el = YFinanceEL()

yf_el.connect_to_db()
yf_el.el_stock_tickers()
yf_el.el_stock_prices()
