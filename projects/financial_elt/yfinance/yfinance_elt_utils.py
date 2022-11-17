from ds_core.ds_utils import *
from ds_core.db_connectors import MySQLConnect
from numerapi import SignalsAPI
import yfinance as yf
import simplejson
from pytickersymbols import PyTickerSymbols

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
        # napi = numerapi.SignalsAPI(os.environ.get('NUMERAI_PUBLIC_KEY'), os.environ.get('NUMERAI_PRIVATE_KEY'))

        numerai_yahoo_tickers = \
            download_numerai_signals_ticker_map()\
                .rename(columns={'yahoo': 'yahoo_ticker', 'ticker': 'numerai_ticker'})

        pts_updated_valid_yahoo_tickers = \
            pd.DataFrame(download_pts_tickers())[['yahoo_tickers']]\
              .rename(columns={'yahoo_tickers': 'yahoo_ticker'})

        if len(self.db.run_sql("select 1 from information_schema.tables where table_schema='yfinance' and table_name='stock_prices_1m';")):
            start_timestamp = \
                self.db.run_sql("SELECT MAX(timestamp) - interval 1 day FROM stock_prices_1m;")\
                .iloc[0].iloc[0].strftime('%Y-%m-%d')
        else:
            start_timestamp = '1970-01-01'

        yahoo_tickers = \
            pd.merge(numerai_yahoo_tickers,
                     pts_updated_valid_yahoo_tickers,
                     on='yahoo_ticker',
                     how='outer')\
            .drop_duplicates()\
            .reset_index(drop=True)

        dfs = download_yf_prices(yahoo_tickers['yahoo_ticker'].tolist(),
                                 yf_params=dict(start=start_timestamp),
                                 mysql_con=self.db)

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

            df = pd.merge(dfs[key], yahoo_tickers, on='yahoo_ticker')
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
                  ROW_NUMBER() OVER(
                    PARTITION BY timestamp, numerai_ticker, yahoo_ticker, bloomberg_ticker
                    ORDER BY id DESC, timestamp DESC
                    ) AS rn
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


def get_valid_yfinance_start_timestamp(interval, start='1970-01-01 00:00:00'):

    """
    Description
    -----------
    Get a valid yfinance date to lookback

    Valid intervals with maximum lookback period
    1m: 7 days
    2m: 60 days
    5m: 60 days
    15m: 60 days
    30m: 60 days
    60m: 730 days
    90m: 60 days
    1h: 730 days
    1d: 50+ years
    5d: 50+ years
    1wk: 50+ years
    1mo: 50+ years --- Buggy!
    3mo: 50+ years --- Buggy!

    Note: Often times yfinance returns an error even when looking back maximum number of days - 1,
        by default, return a date 2 days closer to the current date than the maximum specified in the yfinance docs
    """

    valid_intervals = ['1m', '2m', '5m', '15m', '30m', '60m', '1h', '90m', '1d', '5d', '1wk', '1mo', '3mo']
    assert interval in valid_intervals, f'must pass a valid interval {valid_intervals}'

    if interval == '1m':
        start = max((datetime.today() - timedelta(days=5)), pd.to_datetime(start))
    elif interval in ['2m', '5m', '15m', '30m', '90m']:
        start = (max((datetime.today() - timedelta(days=58)), pd.to_datetime(start)))
    elif interval in ['60m', '1h']:
        start = max((datetime.today() - timedelta(days=728)), pd.to_datetime(start))
    else:
        start = pd.to_datetime(start)

    start = start.strftime('%Y-%m-%d') # yfinance doesn't like strftime with hours minutes or seconds
    return start

def download_yf_prices(tickers,
                       intervals_to_download=('1d', '1h', '1m', '2m', '5m'),
                       num_workers=1,
                       n_chunks=1,
                       yahoo_ticker_colname='yahoo_ticker',
                       mysql_con=None,
                       yf_params=None,
                       verbose=True):
    """
    Parameters
    __________
    See yf.Ticker(<ticker>).history() docs for a detailed description of yf parameters
    tickers: list of tickers to pass to yf.Ticker(<ticker>).history() - it will be parsed to be in the format "AAPL MSFT FB"

    intervals_to_download: list of intervals to download OHLCV data for each stock (e.g. ['1w', '1d', '1h'])

    num_workers: number of threads used to download the data
        so far only 1 thread is implemented

    n_chunks: int number of chunks to pass to yf.Ticker(<ticker>).history()
        1 is the slowest but most reliable because if two are passed and one fails, then both tickers are not returned

    yahoo_ticker_colname: str of column name to set of output yahoo ticker columns

    mysql_con: database connection object to mysql using MySQLConnect class
        disabled if None - it won't search a MySQL DB for missing tickers.
        when db is successfully connected:
            if ticker isn't found in MySQL db table, then set the start date to '1970-01-01' for that specific ticker
            else use default params
        Note: To use this parameter, a MySQL database needs to be set up, which stores the output tables into a schema
            called 'yfinance' when calling YFinanceEL().el_stock_prices()

    **yf_params: dict - passed to yf.Ticker(<ticker>).history(**yf_params)
        set threads = True for faster performance, but tickers will fail, scipt may hang
        set threads = False for slower performance, but more tickers will succeed

    Note: passing some intervals return unreliable stock data (e.g. '3mo' returns many NAs when they should not be)

    Restrictions
    ------------
    Yahoo finance API currently has a 2000 requests per hour or 48000 requests per day limit at the time of writing.
    If the limit hits 1900 requests from the first API call, it will sleep for the remaining time (new hour).
    Similarly, if there are 47000 requests from the first API call it will sleep for the remaining time (new day)
    """

    start = time.time()

    failed_ticker_downloads = {}
    for i in intervals_to_download:
        failed_ticker_downloads[i] = []
    yf_params = {} if yf_params is None else yf_params
    if 'prepost' not in yf_params.keys():
        yf_params['prepost'] = True

    if 'start' not in yf_params.keys():
        if verbose:
            print('*** yf params start set to 1970-01-01! ***')
        yf_params['start'] = '1970-01-01'
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

        column_order = clean_columns(
            pd.DataFrame(
                columns=['timestamp', yahoo_ticker_colname, 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends',
                         'Stock Splits']
            )
        ).columns.tolist()

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

            if mysql_con is not None:
                existing_tables = \
                    mysql_con.run_sql(
                        "SELECT DISTINCT(table_name) FROM information_schema.tables WHERE table_schema = 'yfinance'"
                    )

                if f'stock_prices_{i}' in existing_tables['TABLE_NAME'].tolist():
                    stored_tickers = \
                        mysql_con.run_sql(f'SELECT DISTINCT(yahoo_ticker) FROM stock_prices_{i}')['yahoo_ticker'].tolist()
                else:
                    stored_tickers = []

            if yf_params['threads'] == True:
                if mysql_con is not None and any(x for x in tickers if x not in stored_tickers):
                    yf_params['start'] = get_valid_yfinance_start_timestamp(i)
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

                for chunk in ticker_chunks:
                    if verbose:
                        print(f"Running chunk {chunk}")
                    try:
                        if n_chunks == 1 or len(chunk) == 1:
                            try:
                                if mysql_con is not None and any(x for x in chunk if x not in stored_tickers):
                                    yf_params['start'] = get_valid_yfinance_start_timestamp(i)
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
                                failed_ticker_downloads[i].append(chunk)
                                continue
                        else:
                            if mysql_con is not None and any(x for x in chunk if x not in stored_tickers):
                                yf_params['start'] = get_valid_yfinance_start_timestamp(i)
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
                        failed_ticker_downloads[i].append(chunk)

                    yf_params['start'] = start_date

                try:
                    df_i = pd.concat(chunk_dfs_lst)
                    dict_of_dfs[i] = df_i
                    del chunk_dfs_lst, df_i
                except ValueError:
                    print('\n*** ValueError occurred when trying to concatenate df_i = pd.concat(chunk_dfs_lst) ***\n')

            if verbose:
                for ftd in failed_ticker_downloads:
                    failed_ticker_downloads[ftd] = flatten_list(failed_ticker_downloads[ftd])
                print(f"\nFailed ticker downloads:\n{failed_ticker_downloads}")

    else:
        raise ValueError("Multi-threading not supported yet.")

    if verbose:
        print("\nDownloading yfinance data took %s minutes\n" % round((time.time() - start) / 60, 3))

    return dict_of_dfs


def download_pts_tickers():
    pts = PyTickerSymbols()
    all_getters = list(filter(
        lambda x: (
            x.endswith('_yahoo_tickers') or x.endswith('_google_tickers')
        ),
        dir(pts),
    ))

    all_tickers = {'yahoo_tickers': [], 'google_tickers': []}
    for t in all_getters:
        if t.endswith('google_tickers'):
            all_tickers['google_tickers'].append((getattr(pts, t)()))
        elif t.endswith('yahoo_tickers'):
            all_tickers['yahoo_tickers'].append((getattr(pts, t)()))
    all_tickers['google_tickers'] = flatten_list(all_tickers['google_tickers'])
    all_tickers['yahoo_tickers'] = flatten_list(all_tickers['yahoo_tickers'])
    return all_tickers

def download_numerai_signals_ticker_map(
    napi=SignalsAPI(),
    numerai_ticker_link='https://numerai-signals-public-data.s3-us-west-2.amazonaws.com/signals_ticker_map_w_bbg.csv',
    yahoo_ticker_colname='yahoo',
    verbose=True
    ):

    ticker_map = pd.read_csv(numerai_ticker_link)
    eligible_tickers = pd.Series(napi.ticker_universe(), name='bloomberg_ticker')
    ticker_map = pd.merge(ticker_map, eligible_tickers, on='bloomberg_ticker', how='right')

    if verbose:
        # print(f"Number of eligible tickers: {len(eligible_tickers)}")
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
