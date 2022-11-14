from numerai_elt_utils import *

yf_etl = YFinanceETL()

yf_etl.connect_to_db()
yf_etl.etl_stock_prices()