from numerai_elt_utils import *
from ds_core.db_connectors import MySQLConnect
import numerapi

### Connectors ###

db = MySQLConnect(database='numerai_signals',
                  user=os.environ.get('MYSQL_USER'),
                  password=os.environ.get('MYSQL_PASSWORD'))

con = db.connect()

napi = numerapi.SignalsAPI(os.environ.get('NUMERAI_PUBLIC_KEY'), os.environ.get('NUMERAI_PRIVATE_KEY'))

### Filters ###

all_yahoo_tickers = download_ticker_map(napi).rename(columns={'yahoo': 'yahoo_ticker', 'ticker': 'numerai_ticker'}).head()

if len(db.run_sql("select * from information_schema.tables where table_schema = 'numerai_signals' and table_name = 'yahoo_daily_stocks'")):
    start_timestamp = db.run_sql("SELECT MAX(timestamp) FROM yahoo_stocks_1h;").iloc[0].iloc[0].strftime('%Y-%m-%d %H:%M:%S')
else:
    start_timestamp = '2005-01-01 00:00:00'


### Execution ###

dfs = download_yf_data(all_yahoo_tickers['yahoo_ticker'],
                       yf_params=dict(start=start_timestamp, prepost=True))

column_order = ['timestamp', 'numerai_ticker', 'bloomberg_ticker', 'yahoo_ticker', 'open', 'high', 'low', 'close', 'adj_close', 'volume']

for key in dfs.keys():
    df = pd.merge(dfs[key], all_yahoo_tickers, on='yahoo_ticker')

    assert len(np.intersect1d(df.columns, column_order)) == df.shape[1],\
        'Column mismatch! Review download_yf_data function!'

    df = df[column_order]
    df.to_sql(name=f'yahoo_stocks_{key}', con=con, index=False, if_exists='append')
