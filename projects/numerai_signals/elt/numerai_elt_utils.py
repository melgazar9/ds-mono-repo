from ds_core.ds_utils import *
from ds_core.db_connectors import MySQLConnect
import numerapi
import yfinance as yf
import simplejson

class YFinanceEL:

    """
    Description:
    ------------
    Responsible for yfinance extracting and loading yfinance data (The E and L in ELT).
    Extracts data from instances of yfinance object and loads data into a MySQL database.

    """

    def __init__(self):
        pass

    def connect_to_db(self,
                      database='yfinance',
                      user=os.environ.get('MYSQL_USER'),
                      password=os.environ.get('MYSQL_PASSWORD'),
                      create_schema_if_not_exists=True):

        if create_schema_if_not_exists:
            self.db = MySQLConnect(database='', user=user, password=password)
            self.db.run_sql(f'CREATE DATABASE IF NOT EXISTS {database};')

        self.db = MySQLConnect(database=database, user=user, password=password)
        self.con = self.db.connect()
        return

    def el_stock_prices(self):
        napi = numerapi.SignalsAPI(os.environ.get('NUMERAI_PUBLIC_KEY'), os.environ.get('NUMERAI_PRIVATE_KEY'))

        all_yahoo_tickers = download_ticker_map(napi).rename(columns={'yahoo': 'yahoo_ticker', 'ticker': 'numerai_ticker'})
        

        if len(self.db.run_sql("select 1 from information_schema.tables where table_schema='yfinance' and table_name='stock_prices_1m';")):
            start_timestamp = \
                self.db.run_sql("SELECT MAX(timestamp) - interval 1 day FROM stock_prices_1d;")\
                .iloc[0].iloc[0].strftime('%Y-%m-%d %H:%M:%S')
        else:
            start_timestamp = '1970-01-01 00:00:00'

        dfs = download_yf_prices(all_yahoo_tickers['yahoo_ticker'].tolist(),
                                 yf_params=dict(start=start_timestamp, prepost=True))

        column_order = ['id', 'timestamp', 'numerai_ticker', 'bloomberg_ticker', 'yahoo_ticker', 'open', 'high', 'low', 'close', 'volume', 'dividends', 'stock_splits']

        for key in dfs.keys():
            self.db.run_sql(f"""
                CREATE TABLE IF NOT EXISTS stock_prices_{key} (
                  id INT NOT NULL PRIMARY KEY,
                  timestamp DATETIME NOT NULL,
                  numerai_ticker VARCHAR(32),
                  bloomberg_ticker VARCHAR(32),
                  yahoo_ticker VARCHAR(32),
                  open DECIMAL(38, 12),
                  high DECIMAL(38, 12),
                  low DECIMAL(38, 12),
                  close DECIMAL(38, 12),
                  volume DECIMAL(38, 12),
                  dividends DECIMAL(38, 12),
                  stock_splits DECIMAL(38, 12)
                  )
                  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
                """)

            df = pd.merge(dfs[key], all_yahoo_tickers, on='yahoo_ticker')

            assert len(np.intersect1d(df.columns, column_order)) == df.shape[1], \
                'Column mismatch! Review download_yf_data function!'

            idx_start = self.db.run_sql(f"SELECT MAX(id) FROM stock_prices_{key}")
            idx_start = 0 if pd.isnull(idx_start.values) else idx_start.iloc[0].iloc[0]

            df.sort_values(by='timestamp', inplace=True)
            df.loc[:, 'id'] = [i for i in range(idx_start + 1, idx_start + 1 + df.shape[0])]
            df = df[column_order]
            df.to_sql(name=f'stock_prices_{key}', con=self.con, index=False, if_exists='append')

            self.db.run_sql(f"""
              CREATE TABLE stock_prices_{key}_bk LIKE stock_prices_{key}; 
              
              INSERT INTO stock_prices_{key}_bk
              
              WITH cte as (
                SELECT
                  *,
                  ROW_NUMBER() OVER(PARTITION BY timestamp, numerai_ticker, yahoo_ticker, bloomberg_ticker ORDER BY id desc, timestamp DESC) AS rn
                FROM
                  stock_prices_{key}
                )
              
                SELECT
                  id,
                  timestamp,
                  numerai_ticker,
                  bloomberg_ticker,
                  yahoo_ticker,
                  open,
                  high,
                  low,
                  close,
                  volume,
                  dividends,
                  stock_splits
                FROM
                  cte
                WHERE
                  rn = 1
                ORDER BY
                  id, timestamp, numerai_ticker, yahoo_ticker, bloomberg_ticker;
            """)

            self.db.run_sql(f"""
              ALTER TABLE stock_prices_{key}_bk DROP id;
              ALTER TABLE stock_prices_{key}_bk AUTO_INCREMENT = 1;
              ALTER TABLE stock_prices_{key}_bk ADD id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;
              
              DROP TABLE stock_prices_{key};
              CREATE TABLE stock_prices_{key} LIKE stock_prices_{key}_bk;
              INSERT INTO stock_prices_{key} SELECT * FROM stock_prices_{key}_bk;
              
            """)

            idx_cols = self.db.run_sql(f"""
                SELECT
                  *
                FROM
                  information_schema.statistics 
                WHERE
                  table_schema = '{self.db.database}'
                  AND table_name = 'stock_prices_{key}'
            """)

            if 'timestamp' not in idx_cols['COLUMN_NAME'].tolist():
                self.db.run_sql(f"CREATE INDEX ts ON stock_prices_{key} (timestamp);")

            self.db.run_sql(f"DROP TABLE stock_prices_{key}_bk;")

        return

class YFinanceTransform:

    """
    Description:
    ------------
    Responsible for yfinance data transformations after extracting and loading data using method calls in YFinanceEL.
    """

    def __init__(self):
        pass

    def connect_to_db(self,
                      database='yfinance',
                      user=os.environ.get('MYSQL_USER'),
                      password=os.environ.get('MYSQL_PASSWORD')):

        self.db = MySQLConnect(database=database, user=user, password=password)
        self.con = self.db.connect()
        return

    def transform_stock_prices(self):
        self.db.run_sql()




def download_yf_prices(tickers,
                       intervals_to_download=('1d', '1h', '1m', '2m', '5m'),
                       num_workers=1,
                       n_chunks=1,
                       yf_params=None,
                       yahoo_ticker_colname='yahoo_ticker',
                       verbose=True):
    """
    Parameters
    __________
    See yf.Ticker(<ticker>).history() docs for a detailed description of yf parameters
    tickers: list of tickers to pass to yf.Ticker(<ticker>).history() - it will be parsed to be in the format "AAPL MSFT FB"
    intervals_to_download : list of intervals to download OHLCV data for each stock (e.g. ['1w', '1d', '1h'])
    num_workers: number of threads used to download the data
        so far only 1 thread is implemented
    n_chunks: int number of chunks to pass to yf.Ticker(<ticker>).history()
        1 is the slowest but most reliable because if two are passed and one fails, then both tickers are not returned
    tz_localize_location: timezone location to set the datetime
    **yf_params: dict - passed to yf.Ticker(<ticker>).history(**yf_params)
        set threads = True for faster performance, but tickers will fail, scipt may hang
        set threads = False for slower performance, but more tickers will succeed
    NOTE: passing some intervals return unreliable stock data (e.g. '3mo' returns many NAs when they should not be)

    Restrictions
    ------------
    Yahoo finance API currently has a 2000 requests per hour or 48000 requests per day limit at the time of writing.
    If the limit hits 1900 requests from the first API call, it will sleep for the remaining time (new hour).
    Similarly, if there are 47000 requests from the first API call it will sleep for the remaining time (new day)
    """

    failed_ticker_downloads = []
    yf_params = {} if yf_params is None else yf_params

    if 'start' not in yf_params.keys():
        if verbose:
            print('*** yf params start set to 2005-01-01! ***')
        yf_params['start'] = '2005-01-01'
    if 'threads' not in yf_params.keys() or not yf_params['threads']:
        if verbose:
            print('*** yf params threads set to False! ***')
        yf_params['threads'] = False
    if not verbose:
        yf_params['progress'] = False

    start_date = yf_params['start']
    assert pd.Timestamp(start_date) <= datetime.today(), 'Start date cannot be after the current date!'

    if num_workers == 1:

        n_requests = 0
        request_start_timestamp = datetime.now()

        dict_of_dfs = {}
        for i in intervals_to_download:
            current_runtime_seconds = (datetime.now() - request_start_timestamp).seconds
            if n_requests > 1900 and current_runtime_seconds > 3500:
                if verbose:
                    print(f'\nToo many requests in one hour. Pausing requests for {current_runtime_seconds} seconds.\n')
                time.sleep(np.abs(3600 - current_runtime_seconds))
            if n_requests > 45000 and current_runtime_seconds > 85000:
                print(f'\nToo many requests in one day. Pausing requests for {current_runtime_seconds} seconds.\n')
                time.sleep(np.abs(86400 - current_runtime_seconds))

            print(f'\n*** Running interval {i} ***\n')

            yf_params['interval'] = i

            # Maximum lookback period for intervals i
            # 1m: 7 days
            # 2m: 60 days
            # 5m: 60 days
            # 15m: 60 days
            # 30m: 60 days
            # 60m: 730 days
            # 90m: 60 days
            # 1h: 730 days
            # 1d: 50+ years
            # 5d: 50+ years
            # 1wk: 50+ years
            # 1mo: 50+ years --- Buggy!
            # 3mo: 50+ years --- Buggy!

            if i == '1m':
                yf_params['start'] = datetime.today() - timedelta(days=6)
            elif i in ['2m', '5m', '15m', '30m', '90m']:
                yf_params['start'] = datetime.today() - timedelta(days=58)
            elif i in ['60m', '1h']:
                yf_params['start'] = datetime.today() - timedelta(days=728)
            else:
                yf_params['start'] = pd.to_datetime(yf_params['start']).strftime('%Y-%m-%d')

            if yf_params['threads'] == True:
                t = yf.Tickers(tickers)
                df_i = t.history(tickers, **yf_params)\
                        .stack()\
                        .rename_axis(index=['timestamp', yahoo_ticker_colname])\
                        .reset_index()

                n_requests += 1

                if isinstance(df_i.columns, pd.MultiIndex):
                    df_i.columns = flatten_multindex_columns(df_i)
                df_i = clean_columns(df_i)
                dict_of_dfs[i] = df_i
            else:
                ticker_chunks = [tickers[i:i+n_chunks] for i in range(0, len(tickers), n_chunks)]
                chunk_dfs_lst = []
                column_order = clean_columns(
                    pd.DataFrame(
                        columns=['timestamp', yahoo_ticker_colname, 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits']
                    )
                ).columns.tolist()

                for chunk in ticker_chunks:
                    if verbose:
                        print(f"Running chunk {chunk}")
                    try:
                        if n_chunks == 1 or len(chunk) == 1:
                            try:
                                t = yf.Ticker(chunk[0])
                                df_tmp = \
                                    t.history(chunk, **yf_params)\
                                     .rename_axis(index='timestamp')\
                                     .pipe(lambda x: clean_columns(x))

                                n_requests += 1

                                df_tmp.loc[:, yahoo_ticker_colname] = chunk[0]
                                df_tmp.reset_index(inplace=True)
                                df_tmp = df_tmp[column_order]

                                if df_tmp.shape[0] == 0:
                                    continue
                            except:
                                if verbose:
                                    print(f"failed download for tickers: {chunk}")
                                failed_ticker_downloads.append(chunk)
                                continue

                        else:
                            # should be the order of column_order
                            t = yf.Tickers(chunk)
                            df_tmp = \
                                t.history(chunk, **yf_params)\
                                 .stack()\
                                 .rename_axis(index=['timestamp', yahoo_ticker_colname])\
                                 .reset_index()\
                                 .pipe(lambda x: clean_columns(x))

                            n_requests += 1

                            if df_tmp.shape[0] == 0:
                                continue

                            df_tmp = df_tmp[column_order]

                        chunk_dfs_lst.append(df_tmp)

                    except simplejson.errors.JSONDecodeError:
                        if verbose:
                            print(f"JSONDecodeError for tickers: {chunk}")
                        failed_ticker_downloads.append(chunk)
                        pass

                df_i = pd.concat(chunk_dfs_lst)
                dict_of_dfs[i] = df_i
                del chunk_dfs_lst, df_i
                yf_params['start'] = start_date

            ### print errors ###

            if verbose:
                if len(failed_ticker_downloads) > 0:
                    if n_chunks > 1:
                        failed_ticker_downloads = list(itertools.chain(*failed_ticker_downloads))

                print(f"\nTotal failed ticker downloads:\n{failed_ticker_downloads}")

    else:
        raise ValueError("Multi-threading not supported yet.")

    return dict_of_dfs


def download_ticker_map(napi,
                        numerai_ticker_link='https://numerai-signals-public-data.s3-us-west-2.amazonaws.com/signals_ticker_map_w_bbg.csv',
                        main_ticker_col='bloomberg_ticker',
                        yahoo_ticker_colname='yahoo',
                        verbose=True):

    eligible_tickers = pd.Series(napi.ticker_universe(), name=yahoo_ticker_colname)
    ticker_map = pd.read_csv(numerai_ticker_link)
    ticker_map = ticker_map[ticker_map[main_ticker_col].isin(eligible_tickers)]

    if verbose:
        print(f"Number of eligible tickers: {len(eligible_tickers)}")
        print(f"Number of eligible tickers in map: {len(ticker_map)}")

    # Remove null / empty tickers from the yahoo tickers
    valid_tickers = [i for i in ticker_map[yahoo_ticker_colname]
                     if not pd.isnull(i)
                     and not str(i).lower() == 'nan'\
                     and not str(i).lower() == 'null'\
                     and i is not None\
                     and not str(i).lower() == ''\
                     and len(i) > 0]

    if verbose:
        print('tickers before cleaning:', ticker_map.shape)  # before removing bad tickers

    ticker_map = ticker_map[ticker_map[yahoo_ticker_colname].isin(valid_tickers)]

    if verbose:
        print('tickers after cleaning:', ticker_map.shape)

    return ticker_map
