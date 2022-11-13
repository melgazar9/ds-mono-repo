from numerai_elt_utils import *
from ds_core.db_connectors import MySQLConnect
import numerapi

### Connectors ###

db = MySQLConnect(database='numerai_signals')
napi = numerapi.SignalsAPI(os.environ['NUMERAI_PUBLIC_KEY'], os.environ['NUMERAI_PRIVATE_KEY'])

### Filters ###

if len(db.run_sql("select * from information_schema.tables where table_schema = 'numerai_signals' and table_name = 'yahoo_daily_stocks'")):
    start_date = db.run_sql("SELECT MAX(date) FROM yahoo_daily_stocks;")
else:
    start_date = '2005-01-01'

all_yahoo_tickers = download_ticker_map(napi).rename(columns={'yahoo': 'yahoo_ticker', 'ticker': 'numerai_ticker'})
all_yahoo_tickers = all_yahoo_tickers.head()

### Execution ###

dfs = download_yfinance_data(all_yahoo_tickers['yahoo_ticker'])

for key in dfs.keys():
    df = pd.merge(dfs[key], all_yahoo_tickers, on='yahoo_ticker')

    column_order = ['date', 'numerai_ticker', 'bloomberg_ticker', 'yahoo_ticker',
                    'open', 'high', 'low', 'close', 'adj_close', 'volume']

    assert len(np.intersect1d(df.columns, column_order)) == df.shape[1],\
        'Column mismatch! Review download_yfinance_data function!'

    df = df[column_order]

    # TODO: fix the insert - need to run db.commit()
    db.run_sql(f"""
      CREATE TABLE IF NOT EXISTS
        yahoo_daily_stocks
      (
       date TIMESTAMP,
       numerai_ticker VARCHAR(16),
       bloomberg_ticker VARCHAR(16),
       yahoo_ticker VARCHAR(16),
       open DECIMAL(20, 13),
       high DECIMAL(20, 13),
       low DECIMAL(20, 13),
       close DECIMAL(20, 13),
       adj_close DECIMAL(20, 13),
       volume DECIMAL(20, 13)
      );
      
      INSERT INTO TABLE yahoo_daily_stocks
      (date, numerai_ticker, bloomberg_ticker, yahoo_ticker, open, high, low, close, adj_close, volume)
      VALUES (
        {df['date']},
        {df['numerai_ticker']},
        {df['bloomberg_ticker']},
        {df['yahoo_ticker']},
        {df['open']},
        {df['high']},
        {df['low']},
        {df['close']},
        {df['adj_close']},
        {df['volume']}
      );""")