from ds_core.ds_utils import *
from ds_core.db_connectors import *
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

    Parameters
    ----------
    dwh: str of the data warehouse name to connect to when calling self.connect_to_db()
    schema: str of the schema name to create and dump data to within the data warehouse
    """

    def __init__(self, dwh='mysql', schema='yfinance', verbose=True):
        self.dwh = dwh
        self.schema = schema
        self.verbose = verbose

        self.df_dtype_mappings = {
            str: ['yahoo_ticker', 'numerai_ticker', 'bloomberg_ticker'],
            np.number: ['open', 'high', 'low', 'close', 'dividends', 'stock_splits'],
            np.integer: ['volume']
        }

        self.create_timestamp_index = True if self.dwh == 'mysql' else False

    def connect_to_db(self, create_schema_if_not_exists=True, **db_connect_params):
        """
        Description
        -----------
        Connect to database

        Parameters
        ----------
        create_schema_if_not_exists: bool to create the schema if it doesn't exist
        """

        if create_schema_if_not_exists:
            if self.dwh == 'mysql':
                if 'database' not in db_connect_params:
                    db_connect_params['database'] = 'yfinance'
                self.db = MySQLConnect(**db_connect_params)
                self.db.database = ''
                try:
                    self.db = MySQLConnect(**db_connect_params)
                except:
                    warnings.warn("""\n
                    Error connecting to empty schema string ''.
                    Connecting to schema 'mysql' instead.
                    To disable this set the database to some other value when instantiating the object.
                    \n""")
                    self.db.database = 'mysql'
                    self.db.run_sql(f"CREATE SCHEMA IF NOT EXISTS {db_connect_params['database']};")
                    self.db = MySQLConnect(**db_connect_params)

            elif self.dwh == 'bigquery':
                db_connect_params['schema'] = 'yfinance'
                self.db = BigQueryConnect(**db_connect_params)
                self.db.run_sql(f"CREATE SCHEMA IF NOT EXISTS {db_connect_params['schema']};")
        self.db.connect()
        return self

    def el_stock_tickers(self):
        ticker_downloader = TickerDownloader()
        df_tickers = ticker_downloader.download_valid_tickers()
        if self.dwh == 'mysql':
            df_tickers.to_sql('tickers', con=self.db.con, if_exists='replace', index=False)
        elif self.dwh == 'bigquery':
            self.db.run_sql(f"""
                CREATE TABLE IF NOT EXISTS {self.db.schema}.tickers
                (
                 yahoo_ticker STRING,
                 bloomberg_ticker STRING,
                 numerai_ticker STRING
                )
            """)
            job_config = bigquery.job.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
            self.db.client.load_table_from_dataframe(df_tickers, f'{self.db.schema}.tickers', job_config=job_config)
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

        df_tickers = \
            self.db.run_sql(f"SELECT yahoo_ticker, bloomberg_ticker, numerai_ticker FROM {self.schema}.tickers;")

        column_order = ['timestamp', 'timestamp_tz_aware', 'yahoo_ticker', 'bloomberg_ticker', 'numerai_ticker',
                        'open', 'high', 'low', 'close', 'volume', 'dividends', 'stock_splits']

        stock_price_getter = \
            YFStockPriceGetter(dwh=self.dwh,
                               db_con=self.db,
                               convert_tz_aware_to_string=convert_tz_aware_to_string,
                               verbose=self.verbose)

        if not batch_download:
            print('\n*** Running sequential download ***\n')
            self.db.connect()
            if self.dwh == 'mysql':
                con = self.db.con
            elif self.dwh == 'bigquery':
                con = self.db.client

            yf_params = stock_price_getter.yf_params.copy()

            if n_chunks == 1:
                for i in intervals_to_download:
                    yf_params['interval'] = i

                    stock_price_getter._get_max_stored_ticker_timestamps(table_name=f'stock_prices_{i}')
                    stored_tickers = stock_price_getter.stored_tickers.copy()
                    stock_price_getter._create_stock_prices_table_if_not_exists(table_name=f'stock_prices_{i}')

                    for ticker in df_tickers['yahoo_ticker'].tolist():
                        if ticker in stored_tickers['yahoo_ticker'].tolist():
                            start_date = \
                                stored_tickers[
                                    stored_tickers['yahoo_ticker'] == ticker\
                                    ]['max_timestamp']\
                                    .min()\
                                    .strftime('%Y-%m-%d')
                        else:
                            start_date = '1950-01-01'


                        # TODO: Validate yfinance API is not dropping rows.
                        #  For now I'm overriding start date to always be '1950-01-01'

                        start_date = '1950-01-01'

                        yf_params['start'] = get_valid_yfinance_start_timestamp(interval=i, start=start_date)

                        df = stock_price_getter.download_single_stock_price_history(
                            ticker,
                            yf_history_params=yf_params
                            )

                        if not df.shape[0]:
                            continue

                        df = pd.merge(df, df_tickers, on='yahoo_ticker', how='left')
                        df = df[column_order]
                        df = df.ffill().bfill()

                        if self.dwh == 'mysql':
                            df.to_sql(f'stock_prices_{i}', con=con, index=False, if_exists='append')
                        elif self.dwh == 'bigquery':
                            for k, v in self.df_dtype_mappings.items():
                                df[v] = df[v].astype(k)

                            # df.to_gbq is super slow, but since we're in append-only mode (BigQuery appends table only
                            # by default) it shouldn't be too bad, especially since we're already in a slow for loop
                            if self.verbose:
                                print('\nUploading to BigQuery...\n')
                            df.to_gbq(f'{self.db.schema}.stock_prices_{i}', if_exists='append')
                            time.sleep(2)  # rate limit is 5 GBQ table update operations per 10 seconds

                            # load_table_from_dataframe has pyarrow datatype issues
                            # self.db.client.load_table_from_dataframe(df,
                            #                                          f'{self.db.schema}.stock_prices_{i}',
                            #                                          job_config=stock_price_getter.job_config)
                            # calling job.result() is not needed to append table and adds additional processing time

                    self._dedupe_yf_stock_price_interval(interval=i, create_timestamp_index=self.create_timestamp_index)

                if self.verbose:
                    for ftd in stock_price_getter.failed_ticker_downloads:
                        stock_price_getter.failed_ticker_downloads[ftd] = \
                            flatten_list(stock_price_getter.failed_ticker_downloads[ftd])
                    print(f"\nFailed ticker downloads:\n{stock_price_getter.failed_ticker_downloads}")
        else:
            print('\n*** Running batch download ***\n')
            dfs = stock_price_getter.batch_download_stock_price_history(
                df_tickers['yahoo_ticker'].unique().tolist(),
                intervals_to_download=intervals_to_download
                )

            for key in dfs.keys():
                stock_price_getter._create_stock_prices_table_if_not_exists(table_name=f'stock_prices_{key}')
                df = pd.merge(dfs[key], df_tickers, on='yahoo_ticker', how='left')
                assert len(np.intersect1d(df.columns, column_order)) == df.shape[1], \
                    'Column mismatch! Review download_yf_data function!'
                df.sort_values(by='timestamp', inplace=True)
                df = df[column_order]
                df = df.ffill().bfill()
                for k, v in self.df_dtype_mappings.items():
                    df[v] = df[v].astype(k)
                self.db.connect()
                if self.dwh == 'mysql':
                    df.to_sql(name=f'stock_prices_{key}', con=self.db.con, index=False, if_exists='append')
                elif self.dwh == 'bigquery':
                    if self.verbose:
                        print('\nUploading to BigQuery...\n')
                    df.to_gbq(f'{self.db.schema}.stock_prices_{key}', if_exists='append')
                    time.sleep(2) # rate limit is 5 GBQ table update operations per 10 seconds
                self._dedupe_yf_stock_price_interval(interval=key, create_timestamp_index=self.create_timestamp_index)
        return

    def _dedupe_yf_stock_price_interval(self, interval, create_timestamp_index=True):

        ### need to perform table deduping because yfinance timestamp restrictions don't allow minutes as input ###

        self.db.run_sql(f"""

              CREATE SCHEMA IF NOT EXISTS {self.schema}_bk;
              
              DROP TABLE IF EXISTS {self.schema}_bk.stock_prices_{interval}_bk;
              
              CREATE TABLE {self.schema}_bk.stock_prices_{interval}_bk LIKE {self.schema}.stock_prices_{interval}; 

              INSERT INTO {self.schema}_bk.stock_prices_{interval}_bk

              WITH cte as (
                SELECT
                  *,
                  ROW_NUMBER() OVER(
                    PARTITION BY timestamp, yahoo_ticker
                    ORDER BY timestamp DESC, timestamp_tz_aware DESC
                    ) AS rn
                FROM
                  {self.schema}.stock_prices_{interval}
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
              DROP TABLE {self.schema}.stock_prices_{interval};
              CREATE TABLE {self.schema}.stock_prices_{interval} LIKE {self.schema}_bk.stock_prices_{interval}_bk;
              INSERT INTO {self.schema}.stock_prices_{interval} SELECT * FROM {self.schema}_bk.stock_prices_{interval}_bk;
            """)

        if create_timestamp_index:
            if self.dwh == 'mysql':
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
            elif self.dwh == 'bigquery':
                raise NotImplementedError('Creating timestamp indices is not currently supported on bigquery.')

        # self.db.run_sql(f"DROP TABLE {self.schema}_bk.stock_prices_{interval}_bk;")
        return


class YFinanceTransform:

    """
    Description:
    ------------
    Responsible for yfinance data transformations after extracting and loading data using method calls in YFinanceEL.
    """

    def __init__(self, dwh='mysql'):
        self.dwh = dwh

    def connect_to_db(self,
                      database='yfinance',
                      user=os.environ.get('MYSQL_USER'),
                      password=os.environ.get('MYSQL_PASSWORD')):

        if self.dwh == 'mysql':
            self.db = MySQLConnect(database=database, user=user, password=password)
            con = self.db.connect()
        elif self.dwh == 'bigquery':
            self.db = BigQueryConnect()
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
    dwh: str of db engine - currently only 'mysql' and 'bigquery' has been tested

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
                 dwh='mysql',
                 yf_params=None,
                 db_con=None,
                 database='yfinance',
                 num_workers=1,
                 n_chunks=1,
                 yahoo_ticker_colname='yahoo_ticker',
                 convert_tz_aware_to_string=True,
                 verbose=False):
        self.dwh = dwh
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

        """
        Description
        -----------
        create a stock_price_{interval} table within a specific schema (e.g. yfinance)

        Parameters
        ----------
        table_name: str name of the table to be created
        """

        if self.dwh == 'mysql':
            if self.convert_tz_aware_to_string:
                tz_aware_col = 'timestamp_tz_aware VARCHAR(32) NOT NULL'
            else:
                tz_aware_col = 'timestamp_tz_aware DATETIME NOT NULL'

            self.db_con.run_sql(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                  timestamp DATETIME NOT NULL,
                  {tz_aware_col},
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
        elif self.dwh == 'bigquery':
            tz_aware_dtype = "STRING" if self.convert_tz_aware_to_string else "TIMESTAMP"

            self.job_config = \
                bigquery.LoadJobConfig(
                    schema=[
                        bigquery.SchemaField(name="timestamp", field_type="TIMESTAMP"),
                        bigquery.SchemaField(name="timestamp_tz_aware", field_type=tz_aware_dtype),
                        bigquery.SchemaField(name="yahoo_ticker", field_type="STRING"),
                        bigquery.SchemaField(name="bloomberg_ticker", field_type="STRING"),
                        bigquery.SchemaField(name="numerai_ticker", field_type="STRING"),
                        bigquery.SchemaField(name="open", field_type="NUMERIC"),
                        bigquery.SchemaField(name="high", field_type="NUMERIC"),
                        bigquery.SchemaField(name="low", field_type="NUMERIC"),
                        bigquery.SchemaField(name="close", field_type="NUMERIC"),
                        bigquery.SchemaField(name="volume", field_type="INTEGER"),
                        bigquery.SchemaField(name="dividends", field_type="NUMERIC"),
                        bigquery.SchemaField(name="stock_splits", field_type="NUMERIC")
                    ],
                    autodetect=False
                )
        return

    def download_single_stock_price_history(self, ticker, yf_history_params=None):
        yf_history_params = self.yf_params.copy() if yf_history_params is None else yf_history_params
        if yf_history_params['interval'] not in self.failed_ticker_downloads.keys():
            self.failed_ticker_downloads[yf_history_params['interval']] = []

        # TODO: Validate yfinance API is not dropping rows.
        #  For now I'm overriding start date to always be '1950-01-01'
        yf_history_params['start'] = get_valid_yfinance_start_timestamp(yf_history_params['interval'])

        t = yf.Ticker(ticker)
        try:
            df = \
                t.history(**yf_history_params) \
                 .rename_axis(index='timestamp') \
                 .pipe(lambda x: clean_columns(x))

            self.n_requests += 1

            if not df.shape[0]:
                self.failed_ticker_downloads[yf_history_params['interval']].append(ticker)
                return pd.DataFrame(columns=self.column_order)
        except:
            self.failed_ticker_downloads[yf_history_params['interval']].append(ticker)
            return

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
            if self.dwh == 'mysql':
                existing_tables = \
                    self.db_con.run_sql(
                        f"SELECT DISTINCT(table_name) FROM information_schema.tables WHERE table_schema = '{self.db_con.database}'"
                    ).pipe(lambda x: clean_columns(x))
            elif self.dwh == 'bigquery':
                existing_tables = \
                    self.db_con.run_sql(
                        f"SELECT DISTINCT(table_name) FROM `{self.db_con.schema}.INFORMATION_SCHEMA.TABLES`;"
                    ).pipe(lambda x: clean_columns(x))

            if f'{table_name}' in existing_tables['table_name'].tolist():
                self.stored_tickers = \
                    self.db_con.run_sql(f"""
                        SELECT
                            yahoo_ticker,
                            MAX(timestamp) AS max_timestamp
                        FROM
                            {self.db_con.schema}.{table_name}
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
        for interval in intervals_to_download:
            self.failed_ticker_downloads[interval] = []

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

                    # TODO: Validate yfinance API is not dropping rows.
                    #  For now I'm overriding start date to always be '1950-01-01'
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

                                    # TODO: Validate yfinance API is not dropping rows.
                                    #  For now I'm overriding start date to always be '1950-01-01'
                                    yf_history_params['start'] = get_valid_yfinance_start_timestamp(i)

                                    self._request_limit_check()

                                    t = yf.Ticker(chunk[0])
                                    df_tmp = \
                                        t.history(**yf_history_params)\
                                         .rename_axis(index='timestamp')\
                                         .pipe(lambda x: clean_columns(x))

                                    self.n_requests += 1

                                    if not df_tmp.shape[0]:
                                        self.failed_ticker_downloads[i].append(chunk)
                                        continue

                                    df_tmp.loc[:, self.yahoo_ticker_colname] = chunk[0]
                                    df_tmp.reset_index(inplace=True)
                                    df_tmp['timestamp_tz_aware'] = df_tmp['timestamp'].copy()
                                    df_tmp['timestamp'] = pd.to_datetime(df_tmp['timestamp'], utc=True)
                                    if self.convert_tz_aware_to_string:
                                        df_tmp['timestamp_tz_aware'] = df_tmp['timestamp_tz_aware'].astype(str)
                                    df_tmp = df_tmp[self.column_order]

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

                                # TODO: Validate yfinance API is not dropping rows.
                                #  For now I'm overriding start date to always be '1950-01-01'
                                yf_history_params['start'] = get_valid_yfinance_start_timestamp(i)

                                self._request_limit_check()

                                t = yf.Tickers(chunk)
                                df_tmp = \
                                    t.history(**yf_history_params)\
                                     .stack()\
                                     .rename_axis(index=['timestamp', self.yahoo_ticker_colname])\
                                     .reset_index()\
                                     .pipe(lambda x: clean_columns(x))

                                self.n_requests += 1

                                if not df_tmp.shape[0]:
                                    self.failed_ticker_downloads[i].append(chunk)
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

class TickerDownloader:

    def __init__(self):
        pass

    @staticmethod
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

    @staticmethod
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

    @classmethod
    def download_valid_tickers(cls):
        # napi = numerapi.SignalsAPI(os.environ.get('NUMERAI_PUBLIC_KEY'), os.environ.get('NUMERAI_PRIVATE_KEY'))

        df_pts_tickers = cls.download_pts_tickers()

        numerai_yahoo_tickers = \
            cls.download_numerai_signals_ticker_map()\
               .rename(columns={'yahoo': 'yahoo_ticker', 'ticker': 'numerai_ticker'})

        df1 = pd.merge(df_pts_tickers, numerai_yahoo_tickers, on='yahoo_ticker', how='left').set_index('yahoo_ticker')
        df2 = pd.merge(numerai_yahoo_tickers, df_pts_tickers, on='yahoo_ticker', how='left').set_index('yahoo_ticker')
        df3 = pd.merge(df_pts_tickers, numerai_yahoo_tickers, left_on='yahoo_ticker', right_on='numerai_ticker', how='left') \
            .rename(columns={'yahoo_ticker_x': 'yahoo_ticker', 'yahoo_ticker_y': 'yahoo_ticker_old'}) \
            .set_index('yahoo_ticker')
        df4 = pd.merge(df_pts_tickers, numerai_yahoo_tickers, left_on='yahoo_ticker', right_on='bloomberg_ticker',
                       how='left') \
            .rename(columns={'yahoo_ticker_x': 'yahoo_ticker', 'yahoo_ticker_y': 'yahoo_ticker_old'}) \
            .set_index('yahoo_ticker')

        df_tickers_wide = clean_columns(pd.concat([df1, df2, df3, df4], axis=1))

        for col in df_tickers_wide.columns:
            suffix = col[-1]
            if suffix.isdigit():
                root_col = col.strip('_' + suffix)
                df_tickers_wide[root_col] = df_tickers_wide[root_col].fillna(df_tickers_wide[col])

        df_tickers = \
            df_tickers_wide.reset_index() \
                [['yahoo_ticker', 'google_ticker', 'bloomberg_ticker', 'numerai_ticker', 'yahoo_ticker_old']] \
                .sort_values(by=['yahoo_ticker', 'google_ticker', 'bloomberg_ticker', 'numerai_ticker', 'yahoo_ticker_old']) \
                .drop_duplicates()

        df_tickers.loc[:, 'yahoo_valid_pts'] = False
        df_tickers.loc[:, 'yahoo_valid_numerai'] = False

        df_tickers.loc[
            df_tickers['yahoo_ticker'].isin(df_pts_tickers['yahoo_ticker'].tolist()), 'yahoo_valid_pts'] = True

        df_tickers.loc[
            df_tickers['yahoo_ticker'].isin(numerai_yahoo_tickers['numerai_ticker'].tolist()), 'yahoo_valid_numerai'
        ] = True

        return df_tickers