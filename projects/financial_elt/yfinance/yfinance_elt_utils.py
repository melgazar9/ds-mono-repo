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
    If writing a custom DB connector class the method names must contain "connect" and "run_sql"
        connect must connect to the DB and run_sql must run sql queries with read and write privileges
    """

    def __init__(self):
        pass

    def connect_to_db(self, create_schema_if_not_exists=True, **mysql_connect_params):
        if create_schema_if_not_exists:
            if 'database' not in mysql_connect_params:
                mysql_connect_params['database'] = 'yfinance'
            self.db = MySQLConnect(**mysql_connect_params)
            self.db.database = '' # 'mysql'
            self.db.run_sql(f"CREATE DATABASE IF NOT EXISTS {mysql_connect_params['database']};")

        self.db = MySQLConnect(**mysql_connect_params)
        self.db.connect()
        return self

    def el_stock_tickers(self):
        # napi = numerapi.SignalsAPI(os.environ.get('NUMERAI_PUBLIC_KEY'), os.environ.get('NUMERAI_PRIVATE_KEY'))

        df_pts_tickers = download_pts_tickers()

        numerai_yahoo_tickers = \
            download_numerai_signals_ticker_map().rename(columns={'yahoo': 'yahoo_ticker', 'ticker': 'numerai_ticker'})

        df1 = pd.merge(df_pts_tickers, numerai_yahoo_tickers, on='yahoo_ticker', how='left').set_index('yahoo_ticker')
        df2 = pd.merge(numerai_yahoo_tickers, df_pts_tickers, on='yahoo_ticker', how='left').set_index('yahoo_ticker')
        df3 = pd.merge(df_pts_tickers, numerai_yahoo_tickers, left_on='yahoo_ticker', right_on='numerai_ticker', how='left')\
                 .rename(columns={'yahoo_ticker_x': 'yahoo_ticker', 'yahoo_ticker_y': 'yahoo_ticker_old'})\
                 .set_index('yahoo_ticker')
        df4 = pd.merge(df_pts_tickers, numerai_yahoo_tickers, left_on='yahoo_ticker', right_on='bloomberg_ticker', how='left') \
                .rename(columns={'yahoo_ticker_x': 'yahoo_ticker', 'yahoo_ticker_y': 'yahoo_ticker_old'}) \
                .set_index('yahoo_ticker')

        df_tickers_wide = clean_columns(pd.concat([df1, df2, df3, df4], axis=1))

        for col in df_tickers_wide.columns:
            suffix = col[-1]
            if suffix.isdigit():
                root_col = col.strip('_' + suffix)
                df_tickers_wide[root_col] = df_tickers_wide[root_col].fillna(df_tickers_wide[col])

        df_tickers = \
            df_tickers_wide.reset_index()\
            [['yahoo_ticker', 'google_ticker', 'bloomberg_ticker', 'numerai_ticker', 'yahoo_ticker_old']]\
            .sort_values(by=['yahoo_ticker', 'google_ticker', 'bloomberg_ticker', 'numerai_ticker', 'yahoo_ticker_old'])\
            .drop_duplicates()

        df_tickers.loc[:, 'yahoo_valid_pts'] = False
        df_tickers.loc[:, 'yahoo_valid_numerai'] = False

        df_tickers.loc[
            df_tickers['yahoo_ticker'].isin(df_pts_tickers['yahoo_ticker'].tolist()), 'yahoo_valid_pts'] = True

        df_tickers.loc[
            df_tickers['yahoo_ticker'].isin(numerai_yahoo_tickers['numerai_ticker'].tolist()), 'yahoo_valid_numerai'
        ] = True

        df_tickers.to_sql('tickers', con=self.db.con, if_exists='replace', index=False)

        return


    def el_stock_prices(self,
                        intervals_to_download=('1m', '2m', '5m', '1h', '1d'),
                        n_chunks=1,
                        batch_download=False,
                        convert_tz_aware_to_string=False):

        """
        Description
        -----------
        Download yahoo finance stock prices via yfinance python library and populate a database with the data

        Parameters
        ----------
        batch_download: bool whether to download all tickers in batch and then populate the database
            default is False because batch downloading many tickers and populating a db has many downsides:
                - if working on the cloud, it's significantly more expensive because a bigger instance is needded
                - unknown errors populating the db don't appear until all tickers have been downloaded (could take days)

            if False then the steps are as follows:
                1. download a single tickers price history (start date = max start date from the previous run)
                    if there was no previous run, default to 1950-01-
                2. if dealing with data more granular than one day, check to see which timestamps are already in the db
                    if there exists timestamps in the db then delete them from the pandas df
                3. append the db with that single tickers price data from the pandas df
                4. alter db table: sort the db by timestamp and ticker

        convert_tz_aware_to_string: bool whether to change the data type of timestamp_tz_aware to a string
            likely need to set to True when using a MySQL database. Postgres and cloud based platforms like BigQuery
            and Snowflake should be robust to tz-aware timestamps.
        """

        df_tickers = self.db.run_sql("SELECT yahoo_ticker, bloomberg_ticker, numerai_ticker FROM tickers;")

        column_order = ['timestamp', 'timestamp_tz_aware', 'yahoo_ticker', 'bloomberg_ticker', 'numerai_ticker',
                        'open', 'high', 'low', 'close', 'volume', 'dividends', 'stock_splits']

        stock_price_getter = YFStockPriceGetter(db_con=self.db, convert_tz_aware_to_string=convert_tz_aware_to_string)

        if not batch_download:
            print('\n*** Running sequential download ***\n')
            self.db.connect()
            con = self.db.con
            yf_params = {}
            if n_chunks == 1:
                for i in intervals_to_download:
                    yf_params['interval'] = i
                    stock_price_getter._get_max_stored_ticker_timestamps(table_name=f'stock_prices_{i}')
                    stored_tickers = stock_price_getter.stored_tickers.copy()
                    stock_price_getter._create_stock_prices_table_if_not_exists(table_name=f'stock_prices_{i}')

                    for ticker in df_tickers['yahoo_ticker'].tolist()[0:5]:
                        if ticker in stored_tickers['yahoo_ticker'].tolist():
                            start_date = \
                                stored_tickers[
                                    stored_tickers['yahoo_ticker'] == ticker\
                                    ]['max_timestamp']\
                                    .min()\
                                    .strftime('%Y-%m-%d')
                        else:
                            start_date = '1950-01-01'

                        yf_params['start'] = get_valid_yfinance_start_timestamp(interval=i, start=start_date)

                        df = stock_price_getter.download_single_stock_price_history(
                            ticker,
                            yf_history_params=yf_params
                            )
                        df = pd.merge(df, df_tickers, on='yahoo_ticker', how='left')
                        df = df[column_order]
                        df.to_sql(f'stock_prices_{i}', con=con, index=False, if_exists='append')
                        self._dedupe_yf_stock_price_interval(interval=i)
        else:
            print('\n*** Running batch download ***\n')
            dfs = stock_price_getter.batch_download_stock_price_history(
                df_tickers['yahoo_ticker'].unique().tolist()[0:3],
                intervals_to_download=intervals_to_download
                )

            for key in dfs.keys():
                stock_price_getter._create_stock_prices_table_if_not_exists(table_name=f'stock_prices_{key}')
                df = pd.merge(dfs[key], df_tickers, on='yahoo_ticker', how='left')
                assert len(np.intersect1d(df.columns, column_order)) == df.shape[1], \
                    'Column mismatch! Review download_yf_data function!'
                df.sort_values(by='timestamp', inplace=True)
                df = df[column_order]
                self.db.connect()
                df.to_sql(name=f'stock_prices_{key}', con=self.db.con, index=False, if_exists='append')

                self._dedupe_yf_stock_price_interval(interval=key)
        return

    def _dedupe_yf_stock_price_interval(self, interval, create_timestamp_index=True):

        ### need to perform table deduping because yfinance timestamp restrictions don't allow minutes as input ###

        self.db.run_sql(f"""

              DROP TABLE IF EXISTS stock_prices_{interval}_bk;
              
              CREATE TABLE stock_prices_{interval}_bk LIKE stock_prices_{interval}; 

              INSERT INTO stock_prices_{interval}_bk

              WITH cte as (
                SELECT
                  *,
                  ROW_NUMBER() OVER(
                    PARTITION BY timestamp, timestamp_tz_aware, numerai_ticker, yahoo_ticker, bloomberg_ticker
                    ORDER BY timestamp DESC, timestamp_tz_aware DESC
                    ) AS rn
                FROM
                  stock_prices_{interval}
                )

                SELECT
                  timestamp,
                  timestamp_tz_aware,
                  yahoo_ticker,
                  bloomberg_ticker,
                  numerai_ticker,
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
                  timestamp, yahoo_ticker, bloomberg_ticker, numerai_ticker;
            """)

        self.db.run_sql(f"""
              DROP TABLE stock_prices_{interval};
              CREATE TABLE stock_prices_{interval} LIKE stock_prices_{interval}_bk;
              INSERT INTO stock_prices_{interval} SELECT * FROM stock_prices_{interval}_bk;
            """)

        if create_timestamp_index:
            idx_cols = self.db.run_sql(f"""
                    SELECT
                      *
                    FROM
                      information_schema.statistics 
                    WHERE
                      table_schema = '{self.db.database}'
                      AND table_name = 'stock_prices_{interval}'
                """)

            if 'timestamp' not in idx_cols['COLUMN_NAME'].tolist():
                self.db.run_sql(f"CREATE INDEX ts ON stock_prices_{interval} (timestamp);")

        self.db.run_sql(f"DROP TABLE stock_prices_{interval}_bk;")

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
        con = self.db.connect()
        return con

    def transform_stock_prices(self):
        pass


def get_valid_yfinance_start_timestamp(interval, start='1950-01-01 00:00:00'):

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





class YFStockPriceGetter:

    """

    Parameters
    ----------
    yf_params: dict - passed to yf.Ticker(<ticker>).history(**yf_params)
            set threads = True for faster performance, but tickers will fail, scipt may hang
            set threads = False for slower performance, but more tickers will succeed

    db_con: database connection object to mysql using MySQLConnect class
        disabled if None - it won't search a MySQL DB for missing tickers.
        when db is successfully connected:
            if ticker isn't found in MySQL db table, then set the start date to '1950-01-01' for that specific ticker
            else use default params
        Note: To use this parameter, a MySQL database needs to be set up, which stores the output tables into a schema
            called 'yfinance' when calling YFinanceEL().el_stock_prices()

    database: str of the database to connect to --- ignored if db_con is None

    num_workers: int of number of workers to use on machine

    n_chunks: int of number of stock prices to process at once
        IMPORTANT: setting n_chunks > 1 may have some loss of data integrity (yfinance issue)

    yahoo_ticker_colname: str of column name to set of output yahoo ticker columns
    """

    def __init__(self,
                 yf_params=None,
                 db_con=None,
                 database='yfinance',
                 num_workers=1,
                 n_chunks=1,
                 yahoo_ticker_colname='yahoo_ticker',
                 convert_tz_aware_to_string=True,
                 verbose=False):

        self.yf_params = {} if yf_params is None else yf_params
        self.db_con = db_con
        self.database = database
        self.num_workers = num_workers
        self.n_chunks = n_chunks
        self.yahoo_ticker_colname = yahoo_ticker_colname
        self.convert_tz_aware_to_string = convert_tz_aware_to_string
        self.verbose = verbose

        ### update yf_params ###

        if 'prepost' not in self.yf_params.keys():
            self.yf_params['prepost'] = True
        if 'start' not in self.yf_params.keys():
            if self.verbose:
                print('*** yf params start set to 1950-01-01! ***')
            self.yf_params['start'] = '1950-01-01'
        if 'threads' not in self.yf_params.keys() or not self.yf_params['threads']:
            if self.verbose:
                print('*** yf params threads set to False! ***')
            self.yf_params['threads'] = False

        if not self.verbose:
            self.yf_params['progress'] = False

        self.start_date = self.yf_params['start']
        assert pd.Timestamp(self.start_date) <= datetime.today(), 'Start date cannot be after the current date!'

        self.column_order = clean_columns(
            pd.DataFrame(
                columns=['timestamp', 'timestamp_tz_aware', self.yahoo_ticker_colname, 'Open', 'High', 'Low',
                         'Close', 'Volume', 'Dividends', 'Stock Splits']
            )
        ).columns.tolist()

        self.n_requests = 0
        self.failed_ticker_downloads = {}
        return

    def _request_limit_check(self):
        self.request_start_timestamp = datetime.now()
        self.current_runtime_seconds = (datetime.now() - self.request_start_timestamp).seconds

        if self.n_requests > 1900 and self.current_runtime_seconds > 3500:
            if self.verbose:
                print(f'\nToo many requests in one hour. Pausing requests for {self.current_runtime_seconds} seconds.\n')
            time.sleep(np.abs(3600 - self.current_runtime_seconds))
        if self.n_requests > 45000 and self.current_runtime_seconds > 85000:
            print(f'\nToo many requests in one day. Pausing requests for {self.current_runtime_seconds} seconds.\n')
            time.sleep(np.abs(86400 - self.current_runtime_seconds))
        return

    def _create_stock_prices_table_if_not_exists(self, table_name):
        self.db_con.run_sql(f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                          timestamp DATETIME NOT NULL,
                          timestamp_tz_aware VARCHAR(32) NOT NULL,
                          yahoo_ticker VARCHAR(32),
                          bloomberg_ticker VARCHAR(32),
                          numerai_ticker VARCHAR(32),
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
        return

    def download_single_stock_price_history(self, ticker, yf_history_params=None):
        yf_history_params = self.yf_params.copy() if yf_history_params is None else yf_history_params
        t = yf.Ticker(ticker)
        df = \
            t.history(**yf_history_params) \
             .rename_axis(index='timestamp') \
             .pipe(lambda x: clean_columns(x))

        self.n_requests += 1

        df.loc[:, self.yahoo_ticker_colname] = ticker
        df.reset_index(inplace=True)
        df['timestamp_tz_aware'] = df['timestamp'].copy()
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        if self.convert_tz_aware_to_string:
            df['timestamp_tz_aware'] = df['timestamp_tz_aware'].astype(str)
        df = df[self.column_order]
        return df

    def _get_max_stored_ticker_timestamps(self, table_name):
        self.stored_tickers = pd.DataFrame(columns=['yahoo_ticker', 'max_timestamp'])

        if self.db_con is not None:
            existing_tables = \
                self.db_con.run_sql(
                    f"SELECT DISTINCT(table_name) FROM information_schema.tables WHERE table_schema = '{self.db_con.database}'"
                )

            if f'{table_name}' in existing_tables['TABLE_NAME'].tolist():
                self.stored_tickers = \
                    self.db_con.run_sql(f"""
                        SELECT
                            yahoo_ticker,
                            MAX(timestamp) AS max_timestamp
                        FROM
                            {table_name}
                        GROUP BY 1
                        """)
        return self


    def batch_download_stock_price_history(self,
                                           tickers,
                                           intervals_to_download=('1d', '1h', '1m', '2m', '5m'),
                                           yf_history_params=None):
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

        Note: passing some intervals return unreliable stock data (e.g. '3mo' returns many NAs when they should not be)

        Restrictions
        ------------
        Yahoo finance API currently has a 2000 requests per hour or 48000 requests per day limit at the time of writing.
        If the limit hits 1900 requests from the first API call, it will sleep for the remaining time (new hour).
        Similarly, if there are 47000 requests from the first API call it will sleep for the remaining time (new day)
        """

        ### initial setup ###

        start = time.time()
        yf_history_params = self.yf_params.copy()

        ### download ticker history ###

        if self.num_workers == 1:
            dict_of_dfs = {}
            for i in intervals_to_download:

                self._request_limit_check()

                print(f'\n*** Running interval {i} ***\n')

                self._get_max_stored_ticker_timestamps(table_name=f'stock_prices_{i}')

                yf_history_params['interval'] = i

                if yf_history_params['threads']:
                    self._request_limit_check()
                    if self.db_con is not None \
                            and any(x for x in tickers if x not in self.stored_tickers['yahoo_ticker'].tolist()):
                        yf_history_params['start'] = get_valid_yfinance_start_timestamp(i)

                    t = yf.Tickers(tickers)
                    df_i = t.history(**yf_history_params)\
                            .stack()\
                            .rename_axis(index=['timestamp', self.yahoo_ticker_colname])\
                            .reset_index()

                    self.n_requests += 1

                    if isinstance(df_i.columns, pd.MultiIndex):
                        df_i.columns = flatten_multindex_columns(df_i)
                    df_i = clean_columns(df_i)
                    dict_of_dfs[i] = df_i
                else:
                    ticker_chunks = [tickers[i:i+self.n_chunks] for i in range(0, len(tickers), self.n_chunks)]
                    chunk_dfs_lst = []

                    for chunk in ticker_chunks:
                        if self.verbose:
                            print(f"Running chunk {chunk}")
                        try:
                            if self.n_chunks == 1 or len(chunk) == 1:
                                try:
                                    if self.db_con is not None:
                                        if chunk[0] not in self.stored_tickers['yahoo_ticker'].tolist():
                                            yf_history_params['start'] = get_valid_yfinance_start_timestamp(i)
                                        else:
                                            yf_history_params['start'] = \
                                                self.stored_tickers[self.stored_tickers['yahoo_ticker'] == chunk[0]][
                                                    'max_timestamp'].min().strftime('%Y-%m-%d')

                                    self._request_limit_check()

                                    t = yf.Ticker(chunk[0])
                                    df_tmp = \
                                        t.history(**yf_history_params)\
                                         .rename_axis(index='timestamp')\
                                         .pipe(lambda x: clean_columns(x))

                                    self.n_requests += 1

                                    df_tmp.loc[:, self.yahoo_ticker_colname] = chunk[0]
                                    df_tmp.reset_index(inplace=True)
                                    df_tmp['timestamp_tz_aware'] = df_tmp['timestamp'].copy()
                                    df_tmp['timestamp'] = pd.to_datetime(df_tmp['timestamp'], utc=True)
                                    if self.convert_tz_aware_to_string:
                                        df_tmp['timestamp_tz_aware'] = df_tmp['timestamp_tz_aware'].astype(str)
                                    df_tmp = df_tmp[self.column_order]

                                    if df_tmp.shape[0] == 0:
                                        continue
                                except:
                                    if self.verbose:
                                        print(f"failed download for tickers: {chunk}")
                                    self.failed_ticker_downloads[i].append(chunk)
                                    continue
                            else:
                                if self.db_con is not None:
                                    if any(x for x in chunk if x not in self.stored_tickers['yahoo_ticker'].tolist()):
                                        yf_history_params['start'] = get_valid_yfinance_start_timestamp(i)
                                    else:
                                        yf_history_params['start'] = \
                                            self.stored_tickers[self.stored_tickers['yahoo_ticker'].isin(chunk)][
                                                'max_timestamp'].min().strftime('%Y-%m-%d')

                                self._request_limit_check()

                                t = yf.Tickers(chunk)
                                df_tmp = \
                                    t.history(**yf_history_params)\
                                     .stack()\
                                     .rename_axis(index=['timestamp', self.yahoo_ticker_colname])\
                                     .reset_index()\
                                     .pipe(lambda x: clean_columns(x))

                                self.n_requests += 1

                                if df_tmp.shape[0] == 0:
                                    continue

                                df_tmp = df_tmp[self.column_order]

                            chunk_dfs_lst.append(df_tmp)

                        except simplejson.errors.JSONDecodeError:
                            if self.verbose:
                                print(f"JSONDecodeError for tickers: {chunk}")
                            self.failed_ticker_downloads[i].append(chunk)

                        yf_history_params['start'] = self.start_date

                    try:
                        df_i = pd.concat(chunk_dfs_lst)
                        dict_of_dfs[i] = df_i
                        del chunk_dfs_lst, df_i
                    except ValueError:
                        print('\n*** ValueError occurred when trying to concatenate df_i = pd.concat(chunk_dfs_lst) ***\n')

                if self.verbose:
                    for ftd in self.failed_ticker_downloads:
                        self.failed_ticker_downloads[ftd] = flatten_list(self.failed_ticker_downloads[ftd])
                    print(f"\nFailed ticker downloads:\n{self.failed_ticker_downloads}")

        else:
            raise ValueError("Multi-threading not supported yet.")

        if self.verbose:
            print("\nDownloading yfinance data took %s minutes\n" % round((time.time() - start) / 60, 3))

        return dict_of_dfs

















# def batch_download_yf_prices(tickers,
#                              intervals_to_download=('1d', '1h', '1m', '2m', '5m'),
#                              num_workers=1,
#                              n_chunks=1,
#                              yahoo_ticker_colname='yahoo_ticker',
#                              db_con=None,
#                              database='yfinance',
#                              yf_params=None,
#                              verbose=True):
#     """
#     Parameters
#     __________
#     See yf.Ticker(<ticker>).history() docs for a detailed description of yf parameters
#     tickers: list of tickers to pass to yf.Ticker(<ticker>).history() - it will be parsed to be in the format "AAPL MSFT FB"
#
#     intervals_to_download: list of intervals to download OHLCV data for each stock (e.g. ['1w', '1d', '1h'])
#
#     num_workers: number of threads used to download the data
#         so far only 1 thread is implemented
#
#     n_chunks: int number of chunks to pass to yf.Ticker(<ticker>).history()
#         1 is the slowest but most reliable because if two are passed and one fails, then both tickers are not returned
#
#     yahoo_ticker_colname: str of column name to set of output yahoo ticker columns
#
#     db_con: database connection object to mysql using MySQLConnect class
#         disabled if None - it won't search a MySQL DB for missing tickers.
#         when db is successfully connected:
#             if ticker isn't found in MySQL db table, then set the start date to '1950-01-01' for that specific ticker
#             else use default params
#         Note: To use this parameter, a MySQL database needs to be set up, which stores the output tables into a schema
#             called 'yfinance' when calling YFinanceEL().el_stock_prices()
#
#     database: str of the database to connect to --- ignored if db_con is None
#
#     **yf_params: dict - passed to yf.Ticker(<ticker>).history(**yf_params)
#         set threads = True for faster performance, but tickers will fail, scipt may hang
#         set threads = False for slower performance, but more tickers will succeed
#
#     Note: passing some intervals return unreliable stock data (e.g. '3mo' returns many NAs when they should not be)
#
#     Restrictions
#     ------------
#     Yahoo finance API currently has a 2000 requests per hour or 48000 requests per day limit at the time of writing.
#     If the limit hits 1900 requests from the first API call, it will sleep for the remaining time (new hour).
#     Similarly, if there are 47000 requests from the first API call it will sleep for the remaining time (new day)
#     """
#
#     ### initial setup ###
#
#     start = time.time()
#
#     failed_ticker_downloads = {}
#     for i in intervals_to_download:
#         failed_ticker_downloads[i] = []
#     yf_params = {} if yf_params is None else yf_params
#     if 'prepost' not in yf_params.keys():
#         yf_params['prepost'] = True
#
#     if 'start' not in yf_params.keys():
#         if verbose:
#             print('*** yf params start set to 1950-01-01! ***')
#         yf_params['start'] = '1950-01-01'
#     if 'threads' not in yf_params.keys() or not yf_params['threads']:
#         if verbose:
#             print('*** yf params threads set to False! ***')
#         yf_params['threads'] = False
#     if not verbose:
#         yf_params['progress'] = False
#
#     start_date = yf_params['start']
#
#     assert pd.Timestamp(start_date) <= datetime.today(), 'Start date cannot be after the current date!'
#
#     ### download ticker history ###
#
#     if num_workers == 1:
#         n_requests = 0
#         request_start_timestamp = datetime.now()
#
#         column_order = clean_columns(
#             pd.DataFrame(
#                 columns=['timestamp', yahoo_ticker_colname, 'Open', 'High', 'Low',
#                          'Close', 'Volume', 'Dividends', 'Stock Splits']
#             )
#         ).columns.tolist()
#
#         dict_of_dfs = {}
#         for i in intervals_to_download:
#             current_runtime_seconds = (datetime.now() - request_start_timestamp).seconds
#             if n_requests > 1900 and current_runtime_seconds > 3500:
#                 if verbose:
#                     print(f'\nToo many requests in one hour. Pausing requests for {current_runtime_seconds} seconds.\n')
#                 time.sleep(np.abs(3600 - current_runtime_seconds))
#             if n_requests > 45000 and current_runtime_seconds > 85000:
#                 print(f'\nToo many requests in one day. Pausing requests for {current_runtime_seconds} seconds.\n')
#                 time.sleep(np.abs(86400 - current_runtime_seconds))
#
#             print(f'\n*** Running interval {i} ***\n')
#
#             yf_params['interval'] = i
#
#             if db_con is not None:
#                 existing_tables = \
#                     db_con.run_sql(
#                         f"SELECT DISTINCT(table_name) FROM information_schema.tables WHERE table_schema = '{database}'"
#                     )
#
#                 if f'stock_prices_{i}' in existing_tables['TABLE_NAME'].tolist():
#
#                     stored_tickers = \
#                         db_con.run_sql(f"""
#                             SELECT
#                                 yahoo_ticker,
#                                 MAX(timestamp) AS max_timestamp
#                             FROM
#                                 stock_prices_{i}
#                             GROUP BY 1
#                             """)
#                 else:
#                     stored_tickers = pd.DataFrame(columns=['yahoo_ticker', 'max_timestamp'])
#
#             if yf_params['threads'] == True:
#                 if db_con is not None and any(x for x in tickers if x not in stored_tickers['yahoo_ticker'].tolist()):
#                     yf_params['start'] = get_valid_yfinance_start_timestamp(i)
#                 t = yf.Tickers(tickers)
#                 df_i = t.history(**yf_params)\
#                         .stack()\
#                         .rename_axis(index=['timestamp', yahoo_ticker_colname])\
#                         .reset_index()
#
#                 n_requests += 1
#
#                 if isinstance(df_i.columns, pd.MultiIndex):
#                     df_i.columns = flatten_multindex_columns(df_i)
#                 df_i = clean_columns(df_i)
#                 dict_of_dfs[i] = df_i
#             else:
#                 ticker_chunks = [tickers[i:i+n_chunks] for i in range(0, len(tickers), n_chunks)]
#                 chunk_dfs_lst = []
#
#                 for chunk in ticker_chunks:
#                     if verbose:
#                         print(f"Running chunk {chunk}")
#                     try:
#                         if n_chunks == 1 or len(chunk) == 1:
#                             try:
#                                 if db_con is not None:
#                                     if chunk[0] not in stored_tickers['yahoo_ticker'].tolist():
#                                         yf_params['start'] = get_valid_yfinance_start_timestamp(i)
#                                     else:
#                                         yf_params['start'] = \
#                                             stored_tickers[stored_tickers['yahoo_ticker'] == chunk[0]][
#                                                 'max_timestamp'].min().strftime('%Y-%m-%d')
#
#                                 t = yf.Ticker(chunk[0])
#                                 df_tmp = \
#                                     t.history(**yf_params)\
#                                      .rename_axis(index='timestamp')\
#                                      .pipe(lambda x: clean_columns(x))
#
#                                 n_requests += 1
#
#                                 df_tmp.loc[:, yahoo_ticker_colname] = chunk[0]
#                                 df_tmp.reset_index(inplace=True)
#                                 df_tmp = df_tmp[column_order]
#
#                                 if df_tmp.shape[0] == 0:
#                                     continue
#                             except:
#                                 if verbose:
#                                     print(f"failed download for tickers: {chunk}")
#                                 failed_ticker_downloads[i].append(chunk)
#                                 continue
#                         else:
#                             if db_con is not None:
#                                 if any(x for x in chunk if x not in stored_tickers['yahoo_ticker'].tolist()):
#                                     yf_params['start'] = get_valid_yfinance_start_timestamp(i)
#                                 else:
#                                     yf_params['start'] = \
#                                         stored_tickers[stored_tickers['yahoo_ticker'].isin(chunk)][
#                                             'max_timestamp'].min().strftime('%Y-%m-%d')
#
#                             t = yf.Tickers(chunk)
#                             df_tmp = \
#                                 t.history(**yf_params)\
#                                  .stack()\
#                                  .rename_axis(index=['timestamp', yahoo_ticker_colname])\
#                                  .reset_index()\
#                                  .pipe(lambda x: clean_columns(x))
#
#                             n_requests += 1
#
#                             if df_tmp.shape[0] == 0:
#                                 continue
#
#                             df_tmp = df_tmp[column_order]
#
#                         chunk_dfs_lst.append(df_tmp)
#
#                     except simplejson.errors.JSONDecodeError:
#                         if verbose:
#                             print(f"JSONDecodeError for tickers: {chunk}")
#                         failed_ticker_downloads[i].append(chunk)
#
#                     yf_params['start'] = start_date
#
#                 try:
#                     df_i = pd.concat(chunk_dfs_lst)
#                     dict_of_dfs[i] = df_i
#                     del chunk_dfs_lst, df_i
#                 except ValueError:
#                     print('\n*** ValueError occurred when trying to concatenate df_i = pd.concat(chunk_dfs_lst) ***\n')
#
#             if verbose:
#                 for ftd in failed_ticker_downloads:
#                     failed_ticker_downloads[ftd] = flatten_list(failed_ticker_downloads[ftd])
#                 print(f"\nFailed ticker downloads:\n{failed_ticker_downloads}")
#
#     else:
#         raise ValueError("Multi-threading not supported yet.")
#
#     if verbose:
#         print("\nDownloading yfinance data took %s minutes\n" % round((time.time() - start) / 60, 3))
#
#     return dict_of_dfs


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
    if len(all_tickers['yahoo_tickers']) == len(all_tickers['google_tickers']):
        all_tickers = pd.DataFrame(all_tickers)
    else:
        all_tickers = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in all_tickers.items()]))

    all_tickers = \
        all_tickers\
            .rename(columns={'yahoo_tickers': 'yahoo_ticker', 'google_tickers': 'google_ticker'})\
            .sort_values(by=['yahoo_ticker', 'google_ticker'])\
            .drop_duplicates()
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
