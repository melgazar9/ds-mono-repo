from ds_core.ds_utils import *
from ds_core.db_connectors import *
from numerapi import SignalsAPI
import yfinance as yf
import simplejson
from pytickersymbols import PyTickerSymbols
import pandas_market_calendars as pmc
import dask
from dask import delayed


class YFPriceETL:
    """
    Description:
    ------------
    Responsible for yfinance ETL
    Extracts data from instances of yfinance object, applies minor transformations, and loads data into a db or dwh.
    If writing a custom DB connector class the method names must contain "connect" and "run_sql"
        connect must connect to the DB and run_sql must run sql queries with read and write privileges

    Parameters
    ----------
    schema: str of the schema name to create and dump data to within the data warehouse (e.g. 'yfinance')
    database: str of the database name - currently only used when dwh='snowflake'
    populate_mysql: bool to append MySQL with data dumped to db (dwh).
        MySQL tables will add index constraint & dedupe after each interval loop (e.g. after each '1d', '1m', etc...)
    populate_bigquery: bool to append BigQuery with data dumped to db (dwh).
        BigQuery tables will be deduped after each interval loop (e.g. after each '1d', '1m', etc...)
    populate_snowflake: bool to append Snowflake with data dumped to db (dwh).
        Snowflake tables will be deduped after each interval loop (e.g. after each '1d', '1m', etc...)
    convert_tz_aware_to_string: bool whether to change the data type of timestamp_tz_aware to a string
            likely need to set to True when using a MySQL database. Postgres and cloud based platforms like BigQuery
            and Snowflake should be robust to tz-aware timestamps.
    num_workers: int of number of threads to use when downloading yfinance stock price data
    debug_tickers: array or tuple of tickers to debug in debug mode
    write_method: str of the method to write the stock price df to the db / dwh
    to_sql_chunksize: int of chunksize to use when writing the df to the db / dwh
    write_pandas_threads: only used if write_method='write_pandas' - the number of threads to use called in write_pandas
    verbose: bool to print steps in stdout
    """

    def __init__(self,
                 schema='yfinance',
                 database='FINANCIAL_DB',
                 populate_mysql=False,
                 populate_bigquery=False,
                 populate_snowflake=False,
                 convert_tz_aware_to_string=True,
                 num_workers=1,
                 debug_tickers=None,
                 write_method='write_pandas',
                 to_sql_chunksize=16000,
                 write_pandas_threads=6,
                 verbose=True):
        self.schema = 'yfinance' if schema is None else schema
        self.database = database
        self.populate_mysql = populate_mysql
        self.populate_bigquery = populate_bigquery
        self.populate_snowflake = populate_snowflake
        self.convert_tz_aware_to_string = convert_tz_aware_to_string
        self.num_workers = num_workers
        self.debug_tickers = () if debug_tickers is None else debug_tickers
        self.write_method = write_method
        self.to_sql_chunksize = to_sql_chunksize
        self.write_pandas_threads = write_pandas_threads
        self.verbose = verbose

        # self.debug_tickers = ['AAPL', 'AMZN', 'GOOG', 'META', 'NFLX', 'SQ', 'BGA.AX', 'EUTLF', 'ZZZ.TO']

        assert isinstance(self.debug_tickers, (tuple, list))

        self.df_dtype_mappings = {
            str: ['yahoo_ticker', 'numerai_ticker', 'bloomberg_ticker'],
            np.number: ['open', 'high', 'low', 'close', 'dividends', 'stock_splits'],
            np.integer: ['volume']
        }

        self.dwh_connections = dict()
        self.mysql_client = None
        self.snowflake_client = None
        self.bigquery_client = None

        self.create_timestamp_index_dbs = ('mysql',)

    def _connect_to_snowflake(self, snowflake_connect_params=None):
        if self.populate_snowflake:
            if snowflake_connect_params is None:
                snowflake_connect_params = \
                    dict(
                        user=os.environ.get('SNOWFLAKE_USER'),
                        password=os.environ.get('SNOWFLAKE_PASSWORD'),
                        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'),
                        account=os.environ.get('SNOWFLAKE_ACCOUNT'),
                        database=os.environ.get('SNOWFLAKE_DATABASE'),
                        schema=os.environ.get('SNOWFLAKE_SCHEMA')
                    )

            if self.database is None:
                self.database = 'FINANCIAL_DB'

            if snowflake_connect_params['schema'] is None:
                snowflake_connect_params['schema'] = self.schema

            snowflake_connect_params.update(dict(database=self.database))
            self.snowflake_client = SnowflakeConnect(**snowflake_connect_params)
            self.snowflake_client.run_sql(f"CREATE DATABASE IF NOT EXISTS {self.database};")
            self.snowflake_client.run_sql(f"""CREATE SCHEMA IF NOT EXISTS {self.database}.{self.schema};""")
            self.snowflake_client.connect()
        return self

    def _connect_to_bigquery(self, bigquery_connect_params=None):
        if self.populate_bigquery:
            if bigquery_connect_params is None:
                bigquery_connect_params = dict(
                    google_application_credentials=os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'),
                    schema=os.environ.get('BIGQUERY_SCHEMA')
                )

            if bigquery_connect_params['schema'] is None:
                bigquery_connect_params['schema'] = self.schema

            self.bigquery_client = BigQueryConnect(**bigquery_connect_params)
            self.bigquery_client.run_sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema};")
            self.bigquery_client.connect()
        return self

    def _connect_to_mysql(self, mysql_connect_params=None):
        if self.populate_mysql:
            if mysql_connect_params is None:
                mysql_connect_params = \
                    dict(
                        user=os.environ.get('MYSQL_USER'),
                        password=os.environ.get('MYSQL_PASSWORD'),
                        host=os.environ.get('MYSQL_HOST')
                    )

            try:
                self.mysql_client = MySQLConnect(**mysql_connect_params)
                self.mysql_client.run_sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema};")
                self.mysql_client.schema = self.schema
            except:
                try:
                    warnings.warn("""\n
                        Error connecting to empty schema string ''.
                        Connecting to schema 'mysql' instead.
                        To disable this set the schema to some other value when instantiating the object.
                    \n""")

                    mysql_connect_params['schema'] = 'mysql'
                    self.mysql_client = MySQLConnect(**mysql_connect_params)
                    self.mysql_client.run_sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema};")
                    mysql_connect_params['schema'] = self.schema
                    self.mysql_client = MySQLConnect(**mysql_connect_params)
                    self.mysql_client.connect()
                except:
                    raise ValueError("Could not connect to MySQL")

        return self


    def connect_to_dwhs(self):
        """
        Description
        -----------
        Connect to dwhs determined by which populate parameters are true when initializing the class
        """

        if self.populate_mysql:
            self._connect_to_mysql()
            self.dwh_connections['mysql'] = self.mysql_client

        if self.populate_snowflake:
            self._connect_to_snowflake()
            self.dwh_connections['snowflake'] = self.snowflake_client

        if self.populate_bigquery:
            self._connect_to_bigquery()
            self.dwh_connections['bigquery'] = self.bigquery_client

        return self

    def close_dwh_connections(self):
        for con in self.dwh_connections.keys():
            self.dwh_connections[con].disconnect()
        return self


    def etl_stock_tickers(self):
        ticker_downloader = TickerDownloader()
        df_tickers = ticker_downloader.download_valid_tickers()
        print(f'\nOverwriting df_tickers to database(s): {self.dwh_connections.keys()}...\n') if self.verbose else None

        if self.populate_mysql:
            print('\nOverwriting df_tickers to MySQL...\n') if self.verbose else None
            method = 'multi' if self.write_method == 'write_pandas' else self.write_method
            self.mysql_client.connect()
            df_tickers.to_sql('tickers',
                              schema=self.schema,
                              con=self.mysql_client.con,
                              if_exists='replace',
                              index=False,
                              method=method,
                              chunksize=self.to_sql_chunksize)

        if self.populate_bigquery:
            print('\nOverwriting df_tickers to BigQuery...\n') if self.verbose else None

            job_config = bigquery.job.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
            self.bigquery_client.run_sql(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.tickers
                (
                 yahoo_ticker STRING,
                 google_ticker STRING,
                 bloomberg_ticker STRING,
                 numerai_ticker STRING,
                 yahoo_ticker_old STRING,
                 yahoo_valid_pts BOOL,
                 yahoo_valid_numerai BOOL
                )
            """)

            self.bigquery_client. \
                client.load_table_from_dataframe(df_tickers, f'{self.schema}.tickers', job_config=job_config)

            self.bigquery_client.run_sql(f"""
                CREATE OR REPLACE TABLE {self.schema}.tickers AS (
                SELECT 
                  yahoo_ticker,
                  google_ticker,
                  bloomberg_ticker,
                  numerai_ticker,
                  yahoo_ticker_old,
                  yahoo_valid_pts,
                  yahoo_valid_numerai
                FROM 
                  {self.schema}.tickers  
                ORDER BY 
                  1, 2, 3, 4, 5, 6, 7
                );
            """)

        if self.populate_snowflake:
            print('\nOverwriting df_tickers to Snowflake...\n') if self.verbose else None

            if self.write_method in [pd_writer, 'write_pandas']:
                df_tickers.columns = df_tickers.columns.str.upper()

            if self.write_method.lower() != 'write_pandas':
                df_tickers.to_sql('tickers',
                                  con=self.snowflake_client.con,
                                  if_exists='replace',
                                  index=False,
                                  method=self.write_method,
                                  chunksize=self.to_sql_chunksize)
            else:
                original_backend = self.snowflake_client.backend_engine
                self.snowflake_client.backend_engine = 'snowflake_connector'
                self.snowflake_client.connect()
                write_pandas(df=df_tickers,
                             conn=self.snowflake_client.con,
                             database=self.snowflake_client.database.upper(),
                             schema=self.snowflake_client.schema.upper(),
                             table_name='TICKERS',
                             chunk_size=self.to_sql_chunksize,
                             compression='gzip',
                             parallel=self.write_pandas_threads,
                             overwrite=True,
                             auto_create_table=True)
                self.snowflake_client.backend_engine = original_backend

        return self

    def etl_stock_prices(self,
                         intervals_to_download=('1m', '2m', '5m', '1h', '1d'),
                         batch_download=False,
                         write_to_db_after_interval_complete=False,
                         yf_params=None,
                         n_chunks=1):
        """
        Description
        -----------
        Download yahoo finance stock prices via yfinance python library and populate a database with the data

        Parameters
        ----------

        intervals_to_download: tuple or list of intervals to download (e.g. ('1m', '1d') )

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

        write_to_db_after_interval_complete: Bool whether to write to <database> after interval loop is complete
            This is beneficial when bulk downloading as it will likely lead to memory errors when populating the dwh.

            If write_to_db_after_interval_complete is False and batch_download is True:
                then download all tickers and all intervals into a dictionary of dfs, then upload each df to the dwh
            If write_to_db_after_interval_complete is True and batch_download is False:
                then download all tickers with interval i to a dictionary of dfs, then only upload that interval to dwh
                This is more memory efficient as it will wipe the interval df_i after each interval loop
            If write_to_db_after_interval_complete is False and batch_download is False:
                Then upload each ticker interval to the dwh
                This is very slow since it uploads to the dwh after each API request
            If write_to_db_after_interval_complete is True and batch_download is True:
                Raise NotImplementedError

        yf_params: dict of yfinance params pass to yf.Ticker(<ticker>).history(**yf_params)
        n_chunks: int of the number of chunks to download per API request
        """

        if batch_download and write_to_db_after_interval_complete:
            raise NotImplementedError('Cannot set write_to_db_after_interval_complete=True if batch_download=True')

        intervals_to_download = \
            (intervals_to_download,) if isinstance(intervals_to_download, str) else intervals_to_download

        self.db_client = \
            [i for i in [self.mysql_client, self.bigquery_client, self.snowflake_client] if i is not None][0]

        df_tickers = \
            self.db_client.run_sql(f"SELECT yahoo_ticker, bloomberg_ticker, numerai_ticker FROM {self.schema}.tickers;")

        column_order = ['timestamp', 'timestamp_tz_aware', 'timezone', 'yahoo_ticker', 'bloomberg_ticker',
                        'numerai_ticker', 'open', 'high', 'low', 'close', 'volume', 'dividends', 'stock_splits']

        stock_price_getter = \
            YFStockPriceGetter(dwh_conns=self.dwh_connections,
                               convert_tz_aware_to_string=self.convert_tz_aware_to_string,
                               num_workers=self.num_workers,
                               yf_params=yf_params,
                               verbose=self.verbose)

        if not batch_download:
            if write_to_db_after_interval_complete:
                df_interval = pd.DataFrame()

            print('\n*** Running sequential download ***\n')

            yf_params = stock_price_getter.yf_params.copy()

            if n_chunks == 1:
                for i in intervals_to_download:
                    print(f'\nRunning interval {i}\n') if self.verbose else None

                    yf_params['interval'] = i

                    stock_price_getter._get_max_stored_ticker_timestamps(table_name=f'stock_prices_{i}')
                    stored_tickers = stock_price_getter.stored_tickers.copy()
                    stock_price_getter._create_stock_prices_table_if_not_exists(table_name=f'stock_prices_{i}')

                    # for debugging purposes
                    if len(self.debug_tickers):
                        df_tickers = df_tickers[df_tickers['yahoo_ticker'].isin(self.debug_tickers)]

                    for ticker in df_tickers['yahoo_ticker'].tolist():
                        print(f'\nRunning ticker {ticker}\n') if self.verbose else None
                        if ticker in stored_tickers['yahoo_ticker'].tolist():
                            start_date = \
                                stored_tickers[
                                    stored_tickers['yahoo_ticker'] == ticker \
                                    ]['query_start_timestamp'] \
                                    .min() \
                                    .strftime('%Y-%m-%d')
                        else:
                            start_date = '1950-01-01'

                        print(f'\nStart date for ticker {ticker} is {start_date}\n') if self.verbose else None

                        yf_params['start'] = get_valid_yfinance_start_timestamp(interval=i, start=start_date)

                        df = stock_price_getter.download_single_stock_price_history(
                            ticker,
                            yf_history_params=yf_params
                        )

                        if df is None or not df.shape[0]:
                            continue

                        df = pd.merge(df, df_tickers, on='yahoo_ticker', how='left')
                        df = df[column_order].ffill().bfill()

                        print(f'\nConverting dtypes in df_{i} for ticker {ticker}\n') if self.verbose else None
                        for k, v in self.df_dtype_mappings.items():
                            try:
                                df[v] = df[v].astype(k)
                            except:
                                for numeric_col in df.select_dtypes(np.number).columns:
                                    df[numeric_col] = df[numeric_col].bfill().ffill().fillna(0)
                                df[v] = df[v].astype(k)
                        gc.collect()

                        if not write_to_db_after_interval_complete:
                            self._write_df_to_all_dbs(df=df, interval=i)
                        else:
                            df_interval = pd.concat([df_interval, df], axis=0)

                    if write_to_db_after_interval_complete:
                        self._write_df_to_all_dbs(df=df_interval, interval=i)
                        df_interval = pd.DataFrame()

                    gc.collect()

                if self.verbose:
                    for ftd in stock_price_getter.failed_ticker_downloads:
                        stock_price_getter.failed_ticker_downloads[ftd] = \
                            flatten_list(stock_price_getter.failed_ticker_downloads[ftd])
                    print(f"\nFailed ticker downloads:\n{stock_price_getter.failed_ticker_downloads}")

            else:
                raise NotImplementedError("""
                    n_chunks > 1 not implemented for sequential download.
                    Set n_chunks=1 if batch_download=False.
                    """)
        else:
            print('\n*** Running batch download ***\n')

            # for debugging purposes
            if len(self.debug_tickers):
                df_tickers = df_tickers[df_tickers['yahoo_ticker'].isin(self.debug_tickers)]

            stock_price_getter.batch_download_stock_price_history(
                df_tickers['yahoo_ticker'].unique().tolist(),
                intervals_to_download=intervals_to_download
            )
            dfs = stock_price_getter.dict_of_dfs
            gc.collect()

            for i in dfs.keys():
                stock_price_getter._create_stock_prices_table_if_not_exists(table_name=f'stock_prices_{i}')

                print(f'\nMerging df_{i}...\n') if self.verbose else None

                df = pd.merge(dfs[i], df_tickers, on='yahoo_ticker', how='left')

                assert len(np.intersect1d(df.columns, column_order)) == df.shape[1], \
                    'Column mismatch! Review method that downloads yfinance data!'

                print(f'\nBackfilling df_{i}...\n') if self.verbose else None
                df = df.ffill().bfill()

                print(f'\nSorting by date in df_{i}...\n') if self.verbose else None
                df.sort_values(by='timestamp', inplace=True)

                print(f'\nConverting dtypes in df_{i}...\n') if self.verbose else None
                for k, v in self.df_dtype_mappings.items():
                    df[v] = df[v].astype(k)

                df = df[column_order]

                self._write_df_to_all_dbs(df=df, interval=i)
                gc.collect()
        return

    def _write_df_to_all_dbs(self, df, interval):
        """
        Description
        -----------
        Write dataframe of specified interval to the dwhs supplied in initialization (e.g. populate_snowflake, etc).
        Tables will be stored to self.schema.stock_prices_<interval> in the dwh

        Parameters:
            df: pandas df of stock price data
            interval: str of the interval that was pulled from yfinance (e.g. '1m')
        """

        if self.populate_mysql:
            self._write_to_mysql(df=df, interval=interval)
            self._dedupe_mysql_table(interval=interval)

        if self.populate_snowflake:
            self._write_to_snowflake(df=df, interval=interval)
            self._dedupe_snowflake_table(interval=interval)

        if self.populate_bigquery:
            self._write_to_bigquery(df=df, interval=interval)
            self._dedupe_bigquery_table(interval=interval)
        return self

    def _get_query_dtype_fix(self, interval):
        """
        Description
        -----------
        Static query dtype fix that is called by other methods

        Parameters:
            interval: str of the interval that was pulled from yfinance (e.g. '1m')
        """

        query_dtype_fix = f"""
            DROP TABLE IF EXISTS {self.schema}.tmp_table; 
            CREATE TABLE {self.schema}.tmp_table AS 
              SELECT 
                timestamp, 
                CAST(timestamp_tz_aware AS CHAR(32)) AS timestamp_tz_aware, 
                timezone, 
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
                {self.schema}.stock_prices_{interval};
            """

        return query_dtype_fix

    def _write_to_mysql(self, df, interval):
        """
        Description
        -----------
        Write dataframe of specified interval to the MySQL.
        Tables will be stored to self.schema.stock_prices_<interval> in MySQL.

        Parameters:
            df: pandas df of stock price data
            interval: str of the interval that was pulled from yfinance (e.g. '1m')
        """

        print(f'\nWriting to database MySQL...\n') if self.verbose else None
        self._drop_mysql_index_constraint(interval=interval)
        method = 'multi' if self.write_method == 'write_pandas' else self.write_method
        try:
            self.mysql_client.connect()
            df.to_sql(f'stock_prices_{interval}',
                      con=self.mysql_client.con,
                      index=False,
                      if_exists='append',
                      schema=self.schema,
                      method=method,
                      chunksize=self.to_sql_chunksize)
        except:
            warnings.warn("""
                        Could not directly populate database with df.
                        This is likely because of the timestamp_tz_aware column. Converting it to string...
                        """)

            query_dtype_fix = self._get_query_dtype_fix(interval=interval)
            self.mysql_client.run_sql(query_dtype_fix)

            df['timestamp_tz_aware'] = df['timestamp_tz_aware'].astype(str)

            self.mysql_client.connect()
            df.to_sql(f'stock_prices_{interval}',
                      con=self.mysql_client.con,
                      index=False,
                      if_exists='append',
                      schema=self.schema,
                      method=method,
                      chunksize=self.to_sql_chunksize)

        gc.collect()
        return self

    def _dedupe_mysql_table(self, interval):
        """
        Description
        -----------
        Query that dedupes MySQL table stock_price_<interval> by timestamp, yahoo_ticker

        Parameters:
            interval: str of the interval that was pulled from yfinance (e.g. '1m')
        """

        print(f'\nDeduping MySQL table stock_prices_{interval}...\n') if self.verbose else None

        query_statements = f"""
            ALTER TABLE {self.schema}.stock_prices_{interval} ADD COLUMN to_keep BOOLEAN;
            ALTER TABLE {self.schema}.stock_prices_{interval} 
            ADD CONSTRAINT dedupe UNIQUE (timestamp, yahoo_ticker, to_keep);
            UPDATE IGNORE {self.schema}.stock_prices_{interval} SET to_keep = true;
            DELETE FROM {self.schema}.stock_prices_{interval} WHERE to_keep IS NULL;
            ALTER TABLE {self.schema}.stock_prices_{interval} DROP to_keep;
            """

        self.mysql_client.run_sql(query_statements)

        # create index on timestamp column
        idx_cols = self.mysql_client.run_sql(f"""
                SELECT
                  *
                FROM
                  information_schema.statistics
                WHERE
                  table_schema = '{self.mysql_client.schema}'
                  AND table_name = 'stock_prices_{interval}'
            """)

        if 'timestamp' not in idx_cols['COLUMN_NAME'].tolist():
            self.mysql_client.run_sql(f"CREATE INDEX ts ON {self.schema}.stock_prices_{interval} (timestamp);")

        return self

    def _write_to_snowflake(self, df, interval):
        """
        Description
        -----------
        Write dataframe of specified interval to the Snowflake.
        Tables will be stored to self.schema.stock_prices_<interval> in Snowflake.

        Parameters:
            df: pandas df of stock price data
            interval: str of the interval that was pulled from yfinance (e.g. '1m')
        """

        print(f'\nWriting to Snowflake...\n') if self.verbose else None

        try:
            if self.write_method in [pd_writer, 'write_pandas']:
                df.columns = df.columns.str.upper()

            if self.write_method == 'write_pandas':
                original_backend = self.snowflake_client.backend_engine
                del self.snowflake_client
                self._connect_to_snowflake()
                self.dwh_connections['snowflake'] = self.snowflake_client
                self.snowflake_client.backend_engine = 'snowflake_connector'
                self.snowflake_client.connect()
                write_pandas(df=df,
                             conn=self.snowflake_client.con,
                             database=self.snowflake_client.database.upper(),
                             schema=self.snowflake_client.schema.upper(),
                             table_name=f'stock_prices_{interval}'.upper(),
                             chunk_size=self.to_sql_chunksize,
                             compression='gzip',
                             parallel=self.write_pandas_threads,
                             overwrite=False,
                             auto_create_table=True)
                self.snowflake_client.backend_engine = original_backend

            else:
                self.snowflake_client.connect()
                df.to_sql(f'stock_prices_{interval}',
                          con=self.snowflake_client.con,
                          index=False,
                          if_exists='append',
                          schema=self.schema,
                          method=self.write_method,
                          chunksize=self.chunksize)
        except:
            warnings.warn("""
                        Could not directly populate database with df.
                        This is likely because of the timestamp_tz_aware column. Converting it to string...
                        """)

            query_dtype_fix = self._get_query_dtype_fix(interval=interval)
            separate_query_statements = query_dtype_fix.split(';')
            for query in separate_query_statements[0:-1]:
                query = query.replace('\n', '').replace('  ', '') + ';'
                print(f'\n\nquery: {query}\n\n') if self.verbose else None
                self.snowflake_client.run_sql(query)

            df['timestamp_tz_aware'] = df['timestamp_tz_aware'].astype(str)

            if self.write_method in [pd_writer, 'write_pandas']:
                df.columns = df.columns.str.upper()

            if self.write_method == 'write_pandas':
                original_backend = self.snowflake_client.backend_engine
                self.snowflake_client.backend_engine = 'snowflake_connector'
                self.snowflake_client.connect()
                write_pandas(df=df,
                             conn=self.snowflake_client.con,
                             database=self.snowflake_client.database.upper(),
                             schema=self.snowflake_client.schema.upper(),
                             table_name=f'stock_prices_{interval}'.upper(),
                             chunk_size=self.to_sql_chunksize,
                             compression='gzip',
                             parallel=self.write_pandas_threads,
                             overwrite=False,
                             auto_create_table=True)

                self.snowflake_client.backend_engine = original_backend

            else:
                df.to_sql(f'stock_prices_{interval}',
                          con=self.snowflake_client.con,
                          index=False,
                          if_exists='append',
                          schema=self.schema,
                          method=self.write_method,
                          chunksize=self.to_sql_chunksize)
        return self

    def _dedupe_snowflake_table(self, interval):
        """
        Description
        -----------
        Query that dedupes Snowflake table stock_price_<interval> by timestamp, yahoo_ticker

        Parameters:
            interval: str of the interval that was pulled from yfinance (e.g. '1m')
        """

        self.snowflake_client.run_sql(f"USE {self.snowflake_client.database}")
        initial_syntax = f"INSERT OVERWRITE INTO {self.schema}.stock_prices_{interval} "
        query_statements = f"""
                    {initial_syntax}
                    SELECT 
                      timestamp, 
                      timestamp_tz_aware, 
                      timezone, 
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
                      {self.schema}.stock_prices_{interval} 
                    QUALIFY row_number() over (PARTITION BY timestamp, yahoo_ticker ORDER BY timestamp DESC) = 1 
                    ORDER BY 
                      timestamp, yahoo_ticker, bloomberg_ticker, numerai_ticker;
                """

        separate_query_statements = query_statements.split(';')
        for query in separate_query_statements[0:-1]:
            query = query.replace('\n', '').replace('  ', '') + ';'
            print(f'\n\nQuery: {query}\n\n') if self.verbose else None
            self.snowflake_client.run_sql(query)

        self.snowflake_client.disconnect()

        if 'snowflake' in self.create_timestamp_index_dbs:
            raise NotImplementedError('No need to create timestamp indices on bigquery or snowflake.')

        return self

    def _write_to_bigquery(self, df, interval, retry_cache_dir=os.path.expanduser('~/.cache/bigquery')):
        """
        Description
        -----------
        Write dataframe of specified interval to the BigQuery.
        Tables will be stored to self.schema.stock_prices_<interval> in BigQuery.

        Parameters:
            df: pandas df of stock price data
            interval: str of the interval that was pulled from yfinance (e.g. '1m')
        """

        print(f'\nWriting to BigQuery...\n') if self.verbose else None

        try:
            df.to_gbq(f'{self.schema}.stock_prices_{interval}', if_exists='append')
        except:
            print('\nCould not directly upload df to bigquery! '
                  'Dumping to csv, loading, then trying again via bigquery client...\n') if self.verbose else None

            os.makedirs(retry_cache_dir, exist_ok=True)
            df.to_csv(f'{retry_cache_dir}/tmp.csv', index=False)
            df = pd.read_csv(f'{retry_cache_dir}/tmp.csv')
            job_config = bigquery.LoadJobConfig(autodetect=True)
            table_id = f'{self.bigquery_client.client.project}.{self.schema}.stock_prices_{interval}'
            self.bigquery_client.client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
            subprocess.run(f'rm {retry_cache_dir}/tmp.csv', shell=True)
        gc.collect()
        return self

    def _dedupe_bigquery_table(self, interval):
        """
        Description
        -----------
        Query that dedupes BigQuery table stock_price_<interval> by timestamp, yahoo_ticker

        Parameters:
            interval: str of the interval that was pulled from yfinance (e.g. '1m')
        """

        initial_syntax = f"CREATE OR REPLACE TABLE {self.schema}.stock_prices_{interval} AS ("
        query_statements = f"""
            {initial_syntax}
            SELECT 
              timestamp, 
              timestamp_tz_aware, 
              timezone, 
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
              {self.schema}.stock_prices_{interval} 
            QUALIFY row_number() over (PARTITION BY timestamp, yahoo_ticker ORDER BY timestamp DESC) = 1 
            ORDER BY 
              timestamp, yahoo_ticker, bloomberg_ticker, numerai_ticker
            );
        """
        self.bigquery_client.run_sql(query_statements)

        if 'bigquery' in self.create_timestamp_index_dbs:
            raise NotImplementedError('No need to create timestamp indices on bigquery.')
        return self

    def _drop_mysql_index_constraint(self, interval):
        """
        Description
        -----------
        Query that drops MySQL index constraint on table stock_price_<interval>.

        Parameters:
            interval: str of the interval that was pulled from yfinance (e.g. '1m')
        """
        idx_cols = \
            self.mysql_client.run_sql(f"""
                SELECT
                  *
                FROM
                  information_schema.statistics
                WHERE
                  table_schema = '{self.schema}'
                  AND table_name = 'stock_prices_{interval}'
            """)
        if 'dedupe' in idx_cols['INDEX_NAME'].tolist():
            self.mysql_client.run_sql(f"ALTER TABLE {self.schema}.stock_prices_{interval} DROP INDEX dedupe;")
        return

    ### TODO: integrate fix missing tickers: define methods below ###
    def get_market_calendar(self):
        """
        Description
        -----------
        Get pandas market calendar to determine when the market was open / closed.
        """

        date_range = \
            pd.date_range(start='1950-01-01', end=datetime.today().strftime('%Y-%m-%d'))

        self.df_calendar = \
            pmc.get_calendar('24/7') \
                .schedule(start_date=date_range.min().strftime('%Y-%m-%d'),
                          end_date=date_range.max().strftime('%Y-%m-%d')) \
                .drop_duplicates()

        self.market_closed_dates = []
        for idx in range(self.df_calendar.shape[0] - 1):
            close_date = self.df_calendar.iloc[idx]['market_close'].strftime('%Y-%m-%d')
            next_open_date = self.df_calendar.shift(-1).iloc[idx]['market_open'].strftime('%Y-%m-%d')

            if pd.to_datetime(next_open_date) > pd.to_datetime(close_date):
                market_closed_dates_i = \
                    [d.strftime('%Y-%m-%d') for d in pd.date_range(close_date, next_open_date, freq='d').tolist()[1:-1]]

                self.market_closed_dates.append(market_closed_dates_i)
        self.market_closed_dates = sorted(list(set(flatten_list(self.market_closed_dates))))

        return self

    def is_good_dataset(self, interval):
        """
        TODO: this method is not finished
        Return True if the dataset has no missing timestamps for a specific ticker interval
        """

        pass

    def _fix_missing_ticker_interval(self, interval_to_fix, yf_params):
        """
        TODO: this method is not finished
        Logic lives here to fix missing ticker intervals for specific interval
        """

        print(f'\nFixing missing ticker interval {interval_to_fix}...\n') if self.verbose else None

        intervals_to_seconds = {
            '1m': 60,
            '2m': 60 * 2,
            '5m': 60 * 5,
            '15m': 60 * 15,
            '30m': 60 * 30,
            '60m': 60 * 60,
            '1h': 60 * 60,
            '90m': 60 * 90,
            '1d': 60 * 60 * 24,
            '5d': 60 * 60 * 24 * 5,
            '1wk': 60 * 60 * 24 * 7,
            '1mo': 2628000,
            '3mo': 2628000 * 3
        }

        df = self.db_client.run_sql(
            f"select distinct timestamp, yahoo_ticker from {self.schema}.stock_prices_{interval_to_fix}"
        )

        return df

    def fix_missing_ticker_intervals(self, intervals_to_fix, yf_params, max_attempts=3, tickers_to_fix=None):
        """
        TODO: this method is not finished
        For each interval in intervals_to_fix, call self._fix_missing_ticker_interval
        """

        self.get_market_calendar()

        intervals_to_fix = (intervals_to_fix,) if isinstance(intervals_to_fix, str) else intervals_to_fix

        yf_params = {'prepost': True, 'threads': False, 'start': '1950-01-01'} if yf_params is None else yf_params
        if 'prepost' not in yf_params.keys():
            yf_params['prepost'] = True
        if 'threads' not in yf_params.keys():
            yf_params['threads'] = False
        if 'start' not in yf_params.keys():
            yf_params['start'] = '1950-01-01'

        fixed_datasets = {}
        for i in intervals_to_fix:
            if tickers_to_fix is None:
                tickers = self.db_client.run_sql(
                    f"select distinct(yahoo_ticker) from {self.schema}.stock_prices_{i}"
                )['yahoo_ticker'].unique().tolist()
            else:
                tickers = tickers_to_fix.copy()

            n_tries = 0
            while n_tries < max_attempts:
                print(f'Running attempt {n_tries + 1}...')
                df_i = self._fix_missing_ticker_interval(i, tickers_to_fix=tickers)
                n_tries += 1

                good_dataset = self.is_good_dataset(interval=i)

                if good_dataset or n_tries >= max_attempts:
                    fixed_datasets[i] = df_i
                    break

        for k in fixed_datasets.keys():
            print(f'\nWriting fixed dataset {k} to db...\n') if self.verbose else None
            self._write_df_to_all_dbs(fixed_datasets[k], k)

            print(f'\nDeduping fixed dataset {k}...\n') if self.verbose else None

        return


class YFStockPriceGetter:
    """

    Parameters
    ----------
    dwh_conns: dict of db engine connections ( e.g. {'mysql': <mysql-connection>} )
    yf_params: dict - passed to yf.Ticker(<ticker>).history(**yf_params)
            set threads = True for faster performance, but tickers will fail, scipt may hang
            set threads = False for slower performance, but more tickers will succeed
    schema: str of the schema to connect to
    num_workers: int of number of workers to use on machine
    n_chunks: int of number of stock prices to process at once
        IMPORTANT: setting n_chunks > 1 may have some loss of data integrity (yfinance issue)
    yahoo_ticker_colname: str of column name to set of output yahoo ticker columns
    convert_tz_aware_to_string: bool whether to convert tz_aware timestamp to string in the stock price df.
    verbose: bool whether to log most steps to stdout
    """

    def __init__(self,
                 dwh_conns=None,
                 yf_params=None,
                 schema='yfinance',
                 num_workers=1,
                 n_chunks=1,
                 yahoo_ticker_colname='yahoo_ticker',
                 convert_tz_aware_to_string=True,
                 verbose=False):
        self.dwh_conns = dwh_conns
        self.yf_params = {} if yf_params is None else yf_params
        self.schema = schema
        self.num_workers = num_workers
        self.n_chunks = n_chunks
        self.yahoo_ticker_colname = yahoo_ticker_colname
        self.convert_tz_aware_to_string = convert_tz_aware_to_string
        self.verbose = verbose

        ### update yf_params ###

        if 'prepost' not in self.yf_params.keys():
            self.yf_params['prepost'] = True
        if 'start' not in self.yf_params.keys():
            print('*** YF params start set to 1950-01-01! ***') if self.verbose else None
            self.yf_params['start'] = '1950-01-01'
        if 'threads' not in self.yf_params.keys() or not self.yf_params['threads']:
            print('*** YF params threads set to False! ***') if self.verbose else None
            self.yf_params['threads'] = False

        self.yf_params['progress'] = False if not self.verbose else True

        self.start_date = self.yf_params['start']
        assert pd.Timestamp(self.start_date) <= datetime.today(), 'Start date cannot be after the current date!'

        self.column_order = clean_columns(
            pd.DataFrame(
                columns=['timestamp', 'timestamp_tz_aware', 'timezone', self.yahoo_ticker_colname, 'Open', 'High',
                         'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits']
            )
        ).columns.tolist()

        self.n_requests = 0
        self.failed_ticker_downloads = {}

        if self.dwh_conns is None:
            raise ValueError("Parameter dwh_conns is required to run this process.")

        return

    def _request_limit_check(self):
        """
        Description
        -----------
        Check if too many requests were made to yfinance within their allowed number of requests.
        """
        self.request_start_timestamp = datetime.now()
        self.current_runtime_seconds = (datetime.now() - self.request_start_timestamp).seconds

        if self.n_requests > 1900 and self.current_runtime_seconds > 3500:
            if self.verbose:
                print(f'\nToo many requests per hour. Pausing requests for {self.current_runtime_seconds} seconds.\n')
            time.sleep(np.abs(3600 - self.current_runtime_seconds))
        if self.n_requests > 45000 and self.current_runtime_seconds > 85000:
            if self.verbose:
                print(f'\nToo many requests per day. Pausing requests for {self.current_runtime_seconds} seconds.\n')
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

        for dwh_name in self.dwh_conns.keys():
            if dwh_name == 'mysql':
                if self.convert_tz_aware_to_string:
                    tz_aware_col = 'timestamp_tz_aware CHAR(32) NOT NULL'
                else:
                    tz_aware_col = 'timestamp_tz_aware DATETIME NOT NULL'

                self.dwh_conns[dwh_name].run_sql(f"""
                    CREATE TABLE IF NOT EXISTS {self.schema}.{table_name} (
                      timestamp DATETIME NOT NULL,
                      {tz_aware_col},
                      timezone VARCHAR(32),
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

            elif dwh_name == 'bigquery':
                tz_aware_dtype = "STRING" if self.convert_tz_aware_to_string else "TIMESTAMP"

                self.job_config = \
                    bigquery.LoadJobConfig(
                        schema=[
                            bigquery.SchemaField(name="timestamp", field_type="TIMESTAMP"),
                            bigquery.SchemaField(name="timestamp_tz_aware", field_type=tz_aware_dtype),
                            bigquery.SchemaField(name="timezone", field_type="STRING"),
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

            elif dwh_name == 'snowflake':
                if self.convert_tz_aware_to_string:
                    tz_aware_col = 'timestamp_tz_aware STRING NOT NULL'
                else:
                    tz_aware_col = 'timestamp_tz_aware TIMESTAMP_TZ NOT NULL'

                self.dwh_conns[dwh_name].run_sql(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                      timestamp TIMESTAMP_NTZ NOT NULL,
                      {tz_aware_col},
                      timezone STRING,
                      yahoo_ticker STRING,
                      bloomberg_ticker STRING,
                      numerai_ticker STRING,
                      open FLOAT,
                      high FLOAT,
                      low FLOAT,
                      close FLOAT,
                      volume FLOAT,
                      dividends FLOAT,
                      stock_splits FLOAT
                      );
                    """)
        return self

    def download_single_stock_price_history(self, ticker, yf_history_params=None):
        """
        Description
        -----------
        Download a single stock price ticker from the yfinance python library.
        Minor transformations happen:
            - Add column yahoo_ticker to show which ticker has been pulled
            - Set start date to the minimum start date allowed by yfinance for that ticker (passed in yf_history_params)
            - Clean column names
            - Set tz_aware timestamp column to be a string
        """
        yf_history_params = self.yf_params.copy() if yf_history_params is None else yf_history_params.copy()

        assert 'interval' in yf_history_params.keys(), 'must pass interval parameter to yf_history_params'

        if yf_history_params['interval'] not in self.failed_ticker_downloads.keys():
            self.failed_ticker_downloads[yf_history_params['interval']] = []

        if 'start' not in yf_history_params.keys():
            yf_history_params['start'] = '1950-01-01 00:00:00'

        yf_history_params['start'] = \
            get_valid_yfinance_start_timestamp(interval=yf_history_params['interval'], start=yf_history_params['start'])

        t = yf.Ticker(ticker)
        try:
            df = \
                t.history(**yf_history_params) \
                    .rename_axis(index='timestamp') \
                    .pipe(lambda x: clean_columns(x))

            self.n_requests += 1

            if df is not None and not df.shape[0]:
                self.failed_ticker_downloads[yf_history_params['interval']].append(ticker)
                return pd.DataFrame(columns=self.column_order)
        except:
            self.failed_ticker_downloads[yf_history_params['interval']].append(ticker)
            return

        df.loc[:, self.yahoo_ticker_colname] = ticker
        df.reset_index(inplace=True)
        df['timestamp_tz_aware'] = df['timestamp'].copy()
        df.loc[:, 'timezone'] = str(df['timestamp_tz_aware'].dt.tz)
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        if self.convert_tz_aware_to_string:
            df['timestamp_tz_aware'] = df['timestamp_tz_aware'].astype(str)
        df = df[self.column_order]
        return df

    def _get_max_stored_ticker_timestamps(self, table_name):
        self.stored_tickers = pd.DataFrame(columns=['yahoo_ticker'])

        for dwh_name in self.dwh_conns.keys():
            if dwh_name in ['mysql', 'snowflake']:
                existing_tables = \
                    self.dwh_conns[dwh_name].run_sql(
                        f"""
                        SELECT
                          DISTINCT(table_name)
                        FROM
                          information_schema.tables
                        WHERE
                          lower(table_schema) = '{self.dwh_conns[dwh_name].schema}'
                        """
                    ).pipe(lambda x: clean_columns(x))

            elif dwh_name == 'bigquery':
                existing_tables = \
                    self.dwh_conns[dwh_name].run_sql(
                        f"""
                        SELECT
                          DISTINCT(table_name)
                        FROM
                          `{self.dwh_conns[dwh_name].schema}.INFORMATION_SCHEMA.TABLES`;
                        """
                    ).pipe(lambda x: clean_columns(x))

            existing_tables['table_name'] = existing_tables['table_name'].str.lower()

            if f'{table_name}' in existing_tables['table_name'].tolist():
                tmp_stored_tickers = \
                    self.dwh_conns[dwh_name].run_sql(f"""
                        SELECT
                            yahoo_ticker,
                            MAX(timestamp) AS {dwh_name}_max_timestamp
                        FROM
                            {self.dwh_conns[dwh_name].schema}.{table_name}
                        GROUP BY 1
                        """)
                self.stored_tickers = pd.merge(self.stored_tickers, tmp_stored_tickers, how='outer', on='yahoo_ticker')

        self.stored_tickers['query_start_timestamp'] = \
            self.stored_tickers[[i for i in self.stored_tickers.columns if i.endswith('max_timestamp')]]\
                .apply(lambda x: pd.to_datetime(x.fillna('1970-01-01 00:00:00'), utc=True))\
                .min(axis=1)

        return self

    def batch_download_stock_price_history(self,
                                           tickers,
                                           intervals_to_download=('1m', '2m', '5m', '1h', '1d'),
                                           yf_history_params=None,
                                           parallel_backend='dask'):
        if self.num_workers == 1:
            self.dict_of_dfs = \
                self._batch_download(tickers,
                                     intervals_to_download=intervals_to_download,
                                     yf_history_params=yf_history_params)
        else:
            # dask.config.set(scheduler='processes')  # fails because of pickling error
            nunique_tickers = len(set(tickers))
            chunk_size = int(len(tickers) / self.num_workers)

            if nunique_tickers >= self.num_workers:
                ticker_chunks = [tickers[i:i + chunk_size] for i in range(0, len(tickers), chunk_size)]
            else:
                ticker_chunks = [[t] for t in tickers]

            if len(ticker_chunks[-1]) < chunk_size:
                ticker_chunks[-2] = ticker_chunks[-2] + ticker_chunks[-1]
                ticker_chunks.pop(-1)

            assert len(set(flatten_list(ticker_chunks))) == nunique_tickers

            if parallel_backend == 'dask':
                delayed_list = [delayed(self._batch_download)(tickers=ticker_chunk,
                                                              intervals_to_download=intervals_to_download,
                                                              yf_history_params=yf_history_params)
                                for ticker_chunk in ticker_chunks
                                ]

                df_tuple = dask.compute(*delayed_list)
                output_list = [df_tuple[i] for i in range(len(df_tuple))]

                self.dict_of_dfs = {}
                for i in intervals_to_download:
                    self.dict_of_dfs[i] = pd.DataFrame()
                for i in intervals_to_download:
                    for j in range(len(output_list)):
                        self.dict_of_dfs[i] = \
                            pd.concat([
                                self.dict_of_dfs[i].reset_index(drop=True),
                                output_list[j][i].reset_index(drop=True)]) \
                                .reset_index(drop=True)
            elif parallel_backend == 'multiprocessing':
                raise NotImplementedError('\nSetting parallel_backend to multiprocessing is currently unstable.\n')
                # pool = mp.Pool(self.num_workers)
                # pool.apply_async(
                #     self.__batch_download,
                #     args=(tickers, intervals_to_download, yf_history_params)
                # )

            for dwh_name in self.dwh_conns.keys():
                if dwh_name in ['mysql', 'snowflake']:
                    self.dwh_conns[dwh_name].disconnect()

        return self

    def _batch_download(self, tickers, intervals_to_download=('1m', '2m', '5m', '1h', '1d'), yf_history_params=None):
        """
        Parameters
        __________
        See yf.Ticker(<ticker>).history() docs for a detailed description of yf parameters
        tickers: list of tickers to pass to yf.Ticker(<ticker>).history() - it will be parsed to be in the format "AAPL MSFT FB"

        intervals_to_download: list of intervals to download OHLCV data for each stock (e.g. ['1w', '1d', '1h'])

        n_chunks: int number of chunks to pass to yf.Ticker(<ticker>).history()
            1 is the slowest but most reliable because if two are passed and one fails, then both tickers are not returned

        yf_history_params: dict of parameters passed to yf.Ticker(<ticker>).history(**yf_history_params)

        Note: passing some intervals return unreliable stock data (e.g. '3mo' returns many NAs when they should not be)

        Restrictions
        ------------
        Yahoo finance API currently has a 2000 requests per hour or 48000 requests per day limit at the time of writing.
        If the limit hits 1900 requests from the first API call, it will sleep for the remaining time (new hour).
        Similarly, if there are 47000 requests from the first API call it will sleep for the remaining time (new day)
        """

        ### initial setup ###

        start = time.time()
        yf_history_params = self.yf_params.copy() if yf_history_params is None else yf_history_params

        for interval in intervals_to_download:
            self.failed_ticker_downloads[interval] = []

        ### download ticker history ###

        dict_of_dfs = {}
        for i in intervals_to_download:
            self._request_limit_check()

            print(f'\n*** Running interval {i} ***\n')

            self._get_max_stored_ticker_timestamps(table_name=f'stock_prices_{i}')

            yf_history_params['interval'] = i

            if yf_history_params['threads']:

                warnings.warn("""\n*** yf_history_params 'threads' is set to True.
                               This threading parameter relies on yfinance threading, which is likely to return
                               incorrect or buggy results! Consider setting this parameter to False. ***\n""")

                yf_history_params['start'] = \
                    get_valid_yfinance_start_timestamp(interval=i, start=yf_history_params['start'])

                if len(tickers) == 1:
                    t = yf.Ticker(tickers[0])
                else:
                    t = yf.Tickers(tickers)
                df_i = \
                    t.history(**yf_history_params) \
                        .stack() \
                        .rename_axis(index=['timestamp', self.yahoo_ticker_colname]) \
                        .reset_index()

                self.n_requests += 1

                df_i['timestamp_tz_aware'] = df_i['timestamp'].copy()
                df_i.loc[:, 'timezone'] = str(df_i['timestamp_tz_aware'].dt.tz)
                df_i['timestamp'] = pd.to_datetime(df_i['timestamp'], utc=True)
                if self.convert_tz_aware_to_string:
                    df_i['timestamp_tz_aware'] = df_i['timestamp_tz_aware'].astype(str)

                if isinstance(df_i.columns, pd.MultiIndex):
                    df_i.columns = flatten_multindex_columns(df_i)
                df_i = clean_columns(df_i)
                dict_of_dfs[i] = df_i
            else:
                ticker_chunks = [tickers[i:i + self.n_chunks] for i in range(0, len(tickers), self.n_chunks)]
                chunk_dfs_lst = []

                for chunk in ticker_chunks:
                    print(f"Running chunk {chunk}") if self.verbose else None
                    try:
                        if self.n_chunks == 1 or len(chunk) == 1:
                            try:
                                if list(self.dwh_conns.keys())[0] is not None:
                                    if chunk[0] not in self.stored_tickers['yahoo_ticker'].tolist():
                                        yf_history_params['start'] = get_valid_yfinance_start_timestamp(i)
                                    else:
                                        yf_history_params['start'] = \
                                            self.stored_tickers[self.stored_tickers['yahoo_ticker'] == chunk[0]][
                                                'query_start_timestamp'].min().strftime('%Y-%m-%d')

                                yf_history_params['start'] = \
                                    get_valid_yfinance_start_timestamp(interval=i, start=yf_history_params['start'])

                                self._request_limit_check()

                                print(f"\nStart {yf_history_params['start']}\n") if self.verbose else None

                                t = yf.Ticker(chunk[0])
                                df_tmp = \
                                    t.history(**yf_history_params) \
                                        .rename_axis(index='timestamp') \
                                        .pipe(lambda x: clean_columns(x))

                                self.n_requests += 1

                                if not df_tmp.shape[0]:
                                    self.failed_ticker_downloads[i].append(chunk)
                                    continue

                                df_tmp.loc[:, self.yahoo_ticker_colname] = chunk[0]
                                df_tmp.reset_index(inplace=True)
                                df_tmp['timestamp_tz_aware'] = df_tmp['timestamp'].copy()
                                df_tmp.loc[:, 'timezone'] = str(df_tmp['timestamp_tz_aware'].dt.tz)
                                df_tmp['timestamp'] = pd.to_datetime(df_tmp['timestamp'], utc=True)
                                if self.convert_tz_aware_to_string:
                                    df_tmp['timestamp_tz_aware'] = df_tmp['timestamp_tz_aware'].astype(str)
                                df_tmp = df_tmp[self.column_order]

                            except:
                                print(f"Failed download for tickers: {chunk}") if self.verbose else None
                                self.failed_ticker_downloads[i].append(chunk)
                                continue
                        else:
                            if list(self.dwh_conns.keys())[0] is not None:
                                if any(x for x in chunk if x not in self.stored_tickers['yahoo_ticker'].tolist()):
                                    yf_history_params['start'] = get_valid_yfinance_start_timestamp(i)
                                else:
                                    yf_history_params['start'] = \
                                        self.stored_tickers[self.stored_tickers['yahoo_ticker'].isin(chunk)][
                                            'query_start_timestamp'].min().strftime('%Y-%m-%d')

                            yf_history_params['start'] = \
                                get_valid_yfinance_start_timestamp(interval=i, start=yf_history_params['start'])

                            self._request_limit_check()

                            print(f"\nStart date {yf_history_params['start']}\n") if self.verbose else None

                            t = yf.Tickers(chunk)
                            df_tmp = \
                                t.history(**yf_history_params) \
                                    .stack() \
                                    .rename_axis(index=['timestamp', self.yahoo_ticker_colname]) \
                                    .reset_index() \
                                    .pipe(lambda x: clean_columns(x))

                            self.n_requests += 1

                            if not df_tmp.shape[0]:
                                self.failed_ticker_downloads[i].append(chunk)
                                continue

                            df_tmp['timestamp_tz_aware'] = df_tmp['timestamp'].copy()
                            df_tmp.loc[:, 'timezone'] = str(df_tmp['timestamp_tz_aware'].dt.tz)
                            df_tmp['timestamp'] = pd.to_datetime(df_tmp['timestamp'], utc=True)
                            if self.convert_tz_aware_to_string:
                                df_tmp['timestamp_tz_aware'] = df_tmp['timestamp_tz_aware'].astype(str)

                            df_tmp = df_tmp[self.column_order]

                        chunk_dfs_lst.append(df_tmp)

                    except simplejson.errors.JSONDecodeError:
                        print(f"JSONDecodeError for tickers: {chunk}") if self.verbose else None
                        self.failed_ticker_downloads[i].append(chunk)

                    yf_history_params['start'] = self.start_date

                try:
                    df_i = pd.concat(chunk_dfs_lst)
                    dict_of_dfs[i] = df_i
                    del chunk_dfs_lst, df_i
                except ValueError:
                    print('\n*** ValueError occurred when running df_i = pd.concat(chunk_dfs_lst) ***\n')

            if self.verbose:
                for ftd in self.failed_ticker_downloads:
                    self.failed_ticker_downloads[ftd] = flatten_list(self.failed_ticker_downloads[ftd])
                print(f"\nFailed ticker downloads:\n{self.failed_ticker_downloads}")

        if self.verbose:
            print("\nDownloading yfinance data took %s minutes\n" % round((time.time() - start) / 60, 3))
        return dict_of_dfs


class TickerDownloader:
    """
    Description
    -----------
    Class to download PyTickerSymbols, Yahoo, and Numerai ticker symbols into a single dataframe.
    A mapping between all symbols is returned when calling the method download_valid_tickers().
    """

    def __init__(self):
        pass

    @staticmethod
    def download_pts_tickers():
        """
        Description
        -----------
        Download py-ticker-symbols tickers
        """
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
            all_tickers \
                .rename(columns={'yahoo_tickers': 'yahoo_ticker', 'google_tickers': 'google_ticker'}) \
                .sort_values(by=['yahoo_ticker', 'google_ticker']) \
                .drop_duplicates()
        return all_tickers

    @staticmethod
    def download_numerai_signals_ticker_map(
            napi=SignalsAPI(),
            numerai_ticker_link='https://numerai-signals-public-data.s3-us-west-2.amazonaws.com/signals_ticker_map_w_bbg.csv',
            yahoo_ticker_colname='yahoo',
            verbose=True):
        """
        Description
        -----------
        Download numerai to yahoo ticker mapping
        """

        ticker_map = pd.read_csv(numerai_ticker_link)
        eligible_tickers = pd.Series(napi.ticker_universe(), name='bloomberg_ticker')
        ticker_map = pd.merge(ticker_map, eligible_tickers, on='bloomberg_ticker', how='right')

        print(f"Number of eligible tickers in map: {len(ticker_map)}") if verbose else None

        # Remove null / empty tickers from the yahoo tickers
        valid_tickers = [i for i in ticker_map[yahoo_ticker_colname]
                         if not pd.isnull(i)
                         and not str(i).lower() == 'nan' \
                         and not str(i).lower() == 'null' \
                         and i is not None \
                         and not str(i).lower() == '' \
                         and len(i) > 0]
        print('tickers before cleaning:', ticker_map.shape) if verbose else None

        ticker_map = ticker_map[ticker_map[yahoo_ticker_colname].isin(valid_tickers)]

        print('tickers after cleaning:', ticker_map.shape) if verbose else None

        return ticker_map

    @classmethod
    def download_valid_tickers(cls):
        """
        Description
        -----------
        Download the valid tickers from py-ticker-symbols
        """
        # napi = numerapi.SignalsAPI(os.environ.get('NUMERAI_PUBLIC_KEY'), os.environ.get('NUMERAI_PRIVATE_KEY'))

        df_pts_tickers = cls.download_pts_tickers()

        numerai_yahoo_tickers = \
            cls.download_numerai_signals_ticker_map() \
                .rename(columns={'yahoo': 'yahoo_ticker', 'ticker': 'numerai_ticker'})

        df1 = pd.merge(df_pts_tickers, numerai_yahoo_tickers, on='yahoo_ticker', how='left').set_index('yahoo_ticker')
        df2 = pd.merge(numerai_yahoo_tickers, df_pts_tickers, on='yahoo_ticker', how='left').set_index('yahoo_ticker')
        df3 = pd.merge(df_pts_tickers, numerai_yahoo_tickers, left_on='yahoo_ticker', right_on='numerai_ticker',
                       how='left') \
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
                .sort_values(
                by=['yahoo_ticker', 'google_ticker', 'bloomberg_ticker', 'numerai_ticker', 'yahoo_ticker_old']) \
                .drop_duplicates()

        df_tickers.loc[:, 'yahoo_valid_pts'] = False
        df_tickers.loc[:, 'yahoo_valid_numerai'] = False

        df_tickers.loc[
            df_tickers['yahoo_ticker'].isin(df_pts_tickers['yahoo_ticker'].tolist()), 'yahoo_valid_pts'] = True

        df_tickers.loc[
            df_tickers['yahoo_ticker'].isin(numerai_yahoo_tickers['numerai_ticker'].tolist()), 'yahoo_valid_numerai'
        ] = True

        return df_tickers


###### functions ######

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
    start = start.strftime('%Y-%m-%d')  # yfinance doesn't like strftime with hours minutes or seconds

    return start
