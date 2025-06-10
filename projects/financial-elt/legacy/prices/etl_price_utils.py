import traceback

import pandas as pd
import pandas_market_calendars as pmc
import yfinance as yf
from ds_core.db_connectors import *
from ds_core.ds_utils import *
from numerapi import SignalsAPI
from pytickersymbols import PyTickerSymbols


class YFPriceGetter:
    """
    Parameters
    ----------
    yf_params: dict - passed to yf.Ticker(<ticker>).history(**yf_params) - see docs for yfinance ticker.history() params
    yahoo_ticker_colname: str of column name to set of output yahoo ticker columns
    verbose: bool whether to log most steps to stdout
    """

    def __init__(
        self,
        asset_class,
        yf_params=None,
        yahoo_ticker_colname="yahoo_ticker",
        verbose=False,
    ):
        self.asset_class = asset_class
        self.yf_params = {} if yf_params is None else yf_params
        self.yahoo_ticker_colname = yahoo_ticker_colname
        self.verbose = verbose

        ### update yf_params ###

        if "prepost" not in self.yf_params.keys():
            self.yf_params["prepost"] = True
        if "start" not in self.yf_params.keys():
            (
                print("*** YF params start set to 1950-01-01! ***")
                if self.verbose
                else None
            )
            self.yf_params["start"] = "1950-01-01"

        self.start_date = self.yf_params["start"]
        assert (
            pd.Timestamp(self.start_date) <= datetime.today()
        ), "Start date cannot be after the current date!"

        assert (
            "stocks" in self.asset_class
            or "forex" in self.asset_class
            or "crypto" in self.asset_class
        ), "self.asset_class must be set to either 'stocks', 'forex', or 'crypto'"

        if self.asset_class == "stocks":
            self.column_order = [
                "timestamp",
                "timestamp_tz_aware",
                "timezone",
                "yahoo_ticker",
                "bloomberg_ticker",
                "numerai_ticker",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "dividends",
                "stock_splits",
            ]

        elif asset_class == "forex":
            self.column_order = [
                "timestamp",
                "timestamp_tz_aware",
                "timezone",
                "yahoo_ticker",
                "bloomberg_ticker",
                "open",
                "high",
                "low",
                "close",
                "volume",
            ]

        elif asset_class == "crypto":
            self.column_order = [
                "timestamp",
                "timestamp_tz_aware",
                "timezone",
                "yahoo_ticker",
                "yahoo_name",
                "open",
                "high",
                "low",
                "close",
                "volume",
            ]

        self.n_requests = 0
        self.failed_ticker_downloads = {}

        return

    def _request_limit_check(self):
        """
        Description
        -----------
        Check if too many requests were made to yfinance within their allowed number of requests.
        """
        self.request_start_timestamp = datetime.now()
        self.current_runtime_seconds = (
            datetime.now() - self.request_start_timestamp
        ).seconds

        if self.n_requests > 1900 and self.current_runtime_seconds > 3500:
            if self.verbose:
                print(
                    f"\nToo many requests per hour. Pausing requests for {self.current_runtime_seconds} seconds.\n"
                )
            time.sleep(np.abs(3600 - self.current_runtime_seconds))
        if self.n_requests > 45000 and self.current_runtime_seconds > 85000:
            if self.verbose:
                print(
                    f"\nToo many requests per day. Pausing requests for {self.current_runtime_seconds} seconds.\n"
                )
            time.sleep(np.abs(86400 - self.current_runtime_seconds))
        return

    def download_single_symbol_price_history(
        self, ticker, yf_history_params=None
    ) -> pd.DataFrame():
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
        yf_history_params = (
            self.yf_params.copy()
            if yf_history_params is None
            else yf_history_params.copy()
        )

        assert (
            "interval" in yf_history_params.keys()
        ), "must pass interval parameter to yf_history_params"

        if yf_history_params["interval"] not in self.failed_ticker_downloads.keys():
            self.failed_ticker_downloads[yf_history_params["interval"]] = []

        if "start" not in yf_history_params.keys():
            yf_history_params["start"] = "1950-01-01 00:00:00"

        yf_history_params["start"] = get_valid_yfinance_start_timestamp(
            interval=yf_history_params["interval"], start=yf_history_params["start"]
        )

        t = yf.Ticker(ticker)
        try:
            df = (
                t.history(**yf_history_params)
                .rename_axis(index="timestamp")
                .pipe(lambda x: clean_columns(x))
            )

            self.n_requests += 1

            if df is not None and not df.shape[0]:
                self.failed_ticker_downloads[yf_history_params["interval"]].append(
                    ticker
                )
                return pd.DataFrame(columns=self.column_order)

            df.loc[:, self.yahoo_ticker_colname] = ticker
            df.reset_index(inplace=True)
            df["timestamp_tz_aware"] = df["timestamp"].copy()
            df.loc[:, "timezone"] = str(df["timestamp_tz_aware"].dt.tz)
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            return df

        except Exception:
            self.failed_ticker_downloads[yf_history_params["interval"]].append(ticker)
            return

    def batch_download_price_history(
        self,
        tickers,
        table_prefix,
        intervals_to_download=("1m", "2m", "5m", "1h", "1d"),
        yf_history_params=None,
    ):
        """
        NOTE: At the time of writing, this code fails with segmentation fault after upgrading to yfinance version 2.
              This will likely be fixed soon, so for now turn off batch download.
        """
        self.dict_of_dfs = self._batch_download(
            tickers,
            table_prefix=table_prefix,
            intervals_to_download=intervals_to_download,
            yf_history_params=yf_history_params,
        )

        return self

    def _batch_download(
        self,
        tickers,
        table_prefix,
        intervals_to_download=("1m", "2m", "5m", "1h", "1d"),
        yf_history_params=None,
    ):
        """
        NOTE: Version 2 of yfinance package has bugs when pulling multiple tickers at once.
              For reliable data turn off batch mode.
        Parameters
        __________
        See yf.Ticker(<ticker>).history() docs for a detailed description of yf parameters
        tickers: list of tickers to pass to yf.Ticker(<ticker>).history() - it will be parsed to be in the format "AAPL MSFT FB"

        intervals_to_download: list of intervals to download OHLCV data for each stock (e.g. ['1w', '1d', '1h'])

        yf_history_params: dict of parameters passed to yf.Ticker(<ticker>).history(**yf_history_params)

        Note: passing some intervals return unreliable stock data (e.g. '3mo' returns many NAs when they should not be)

        Restrictions
        ------------
        Yahoo finance API currently has a 2000 requests per hour or 48000 requests per day limit at the time of writing.
        If the limit hits 1900 requests from the first API call, it will sleep for the remaining time (new hour).
        Similarly, if there are 47000 requests from the first API call it will sleep for the remaining time (new day)
        """

        start = time.time()
        yf_history_params = (
            self.yf_params.copy() if yf_history_params is None else yf_history_params
        )

        for interval in intervals_to_download:
            self.failed_ticker_downloads[interval] = []

        ### download ticker history ###

        dict_of_dfs = {}
        for i in intervals_to_download:
            self._request_limit_check()

            print(f"\n*** Running {table_prefix} interval {i} ***\n")

            yf_history_params["interval"] = i

            if len(tickers) == 1:
                t = yf.Ticker(tickers[0])
            else:
                t = yf.Tickers(tickers)

            conflict_params = []
            params = list(yf_history_params)
            for param in params:
                if param not in inspect.getfullargspec(t.history).args:
                    conflict_params.append(param)
                    yf_history_params.pop(param)
            if len(conflict_params):
                warnings.warn(
                    f"""\n
                  *** Removing conflicting parameters passed to yf.download in batch_download mode {conflict_params} ***
                  \n"""
                )

            df_i = (
                t.history(**yf_history_params)
                .stack()
                .rename_axis(index=["timestamp", self.yahoo_ticker_colname])
                .reset_index()
            )

            gc.collect()

            self.n_requests += 1

            df_i["timestamp_tz_aware"] = df_i["timestamp"].copy()
            if df_i.shape[0]:
                df_i.loc[:, "timezone"] = str(df_i["timestamp_tz_aware"].dt.tz)
                df_i["timestamp"] = pd.to_datetime(df_i["timestamp"], utc=True)

                if isinstance(df_i.columns, pd.MultiIndex):
                    df_i.columns = flatten_multindex_columns(df_i)
                df_i = clean_columns(df_i)
                dict_of_dfs[i] = df_i
            else:
                warnings.warn(f"\n*** Could not pull data for interval {i} ***\n")

            if self.verbose:
                for ftd in self.failed_ticker_downloads:
                    self.failed_ticker_downloads[ftd] = flatten_list(
                        self.failed_ticker_downloads[ftd]
                    )
                print(f"\nFailed ticker downloads:\n{self.failed_ticker_downloads}")

        if self.verbose:
            print(
                "\nDownloading yfinance data took %s minutes\n"
                % round((time.time() - start) / 60, 3)
            )

        return dict_of_dfs


class YFPriceETL(YFPriceGetter):
    """
    NOTE: Version 2 of yfinance package has bugs when pulling multiple tickers at once.
          For reliable data turn off batch mode.
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
    write_method: str of the method to write the stock price df to the db / dwh
    to_sql_chunksize: int of chunksize to use when writing the df to the db / dwh
    write_pandas_threads: only used if write_method='write_pandas' - the number of threads to use called in write_pandas
    verbose: bool to print steps in stdout
    """

    def __init__(
        self,
        schema="yfinance_el_dev",
        database="financial_db",
        populate_mysql=False,
        populate_bigquery=False,
        populate_snowflake=False,
        dedupe_after_populate=False,
        convert_tz_aware_to_string=True,
        write_method="multi",
        to_sql_chunksize=16000,
        write_pandas_threads=6,
        verbose=True,
    ):
        self.database = database
        self.schema = schema
        self.populate_mysql = populate_mysql
        self.populate_bigquery = populate_bigquery
        self.populate_snowflake = populate_snowflake
        self.dedupe_after_populate = dedupe_after_populate
        self.convert_tz_aware_to_string = convert_tz_aware_to_string
        self.write_method = write_method
        self.to_sql_chunksize = to_sql_chunksize
        self.write_pandas_threads = write_pandas_threads
        self.verbose = verbose

        self.ticker_downloader = TickerDownloader()
        self.db_client = None

        self.dwh_connections = dict()
        self.mysql_client = None
        self.snowflake_client = None
        self.bigquery_client = None

        self.create_timestamp_index_dbs = ("mysql",)
        self.yf_params = None
        self.df_dtype_mappings = None
        self.df_calendar = None
        self.market_closed_dates = None

    ###### dwh connections ######

    def _connect_to_snowflake(self, snowflake_connect_params=None):
        if self.populate_snowflake:
            if self.database is None:
                self.database = "FINANCIAL_DB"

            if snowflake_connect_params is None:
                snowflake_connect_params = dict(
                    user=os.getenv("SNOWFLAKE_USER"),
                    password=os.getenv("SNOWFLAKE_PASSWORD"),
                    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                    account=os.getenv("SNOWFLAKE_ACCOUNT"),
                    database=self.database,
                    schema=self.schema,
                )

            assert all(
                [v is not None for v in snowflake_connect_params.values()]
            ), f"No value can be None in snowflake_connect_params. snowflake_connect_params: {snowflake_connect_params}"

            snowflake_connect_params.update(dict(database=self.database))
            self.snowflake_client = SnowflakeConnect(**snowflake_connect_params)
            self.snowflake_client.run_sql(
                f"CREATE DATABASE IF NOT EXISTS {self.database};"
            )
            self.snowflake_client.run_sql(
                f"""CREATE SCHEMA IF NOT EXISTS {self.database}.{self.schema};"""
            )
            self.snowflake_client.connect()
        return self

    def _connect_to_bigquery(self, bigquery_connect_params=None):
        if self.populate_bigquery:
            if bigquery_connect_params is None:
                bigquery_connect_params = dict(
                    google_application_credentials=os.environ.get(
                        "GOOGLE_APPLICATION_CREDENTIALS"
                    ),
                    schema=os.environ.get("BIGQUERY_SCHEMA"),
                )

            if bigquery_connect_params["schema"] is None:
                bigquery_connect_params["schema"] = self.schema

            self.bigquery_client = BigQueryConnect(**bigquery_connect_params)
            self.bigquery_client.run_sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema};")
            self.bigquery_client.connect()
        return self

    def _connect_to_mysql(self, mysql_connect_params=None):
        if self.populate_mysql:
            if mysql_connect_params is None:
                mysql_connect_params = dict(
                    user=os.environ.get("MYSQL_USER"),
                    password=os.environ.get("MYSQL_PASSWORD"),
                    host=os.environ.get("MYSQL_HOST"),
                )

            try:
                self.mysql_client = MySQLConnect(**mysql_connect_params)
                self.mysql_client.run_sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema};")
                self.mysql_client.schema = self.schema
            except Exception:
                try:
                    warnings.warn(
                        """\n
                        Error connecting to empty schema string ''.
                        Connecting to schema 'mysql' instead.
                        To disable this set the schema to some other value when instantiating the object.
                    \n"""
                    )

                    mysql_connect_params["schema"] = "mysql"
                    self.mysql_client = MySQLConnect(**mysql_connect_params)
                    self.mysql_client.run_sql(
                        f"CREATE SCHEMA IF NOT EXISTS {self.schema};"
                    )
                    mysql_connect_params["schema"] = self.schema
                    self.mysql_client = MySQLConnect(**mysql_connect_params)
                    self.mysql_client.connect()
                except Exception:
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
            self.dwh_connections["mysql"] = self.mysql_client

        if self.populate_snowflake:
            self._connect_to_snowflake()
            self.dwh_connections["snowflake"] = self.snowflake_client

        if self.populate_bigquery:
            self._connect_to_bigquery()
            self.dwh_connections["bigquery"] = self.bigquery_client

        return self

    def close_dwh_connections(self):
        for con in self.dwh_connections.keys():
            self.dwh_connections[con].disconnect()
        return self

    ###### stocks ######

    def etl_stock_tickers(self, table_name="stock_tickers"):
        df_tickers = self.ticker_downloader.download_valid_stock_tickers()
        (
            print(
                f"\nOverwriting stock_tickers to database(s): {self.dwh_connections.keys()}...\n"
            )
            if self.verbose
            else None
        )

        df_tickers["batch_timestamp"] = datetime.utcnow()

        if self.populate_mysql:
            print("\nOverwriting stock_tickers to MySQL...\n") if self.verbose else None
            method = (
                "multi" if self.write_method == "write_pandas" else self.write_method
            )
            self.mysql_client.connect()
            df_tickers.to_sql(
                table_name,
                schema=self.schema,
                con=self.mysql_client.con,
                if_exists="replace",
                index=False,
                method=method,
                chunksize=self.to_sql_chunksize,
            )

        if self.populate_bigquery:
            (
                print("\nOverwriting stock_tickers to BigQuery...\n")
                if self.verbose
                else None
            )

            job_config = bigquery.job.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )

            self.bigquery_client.run_sql(
                f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.{table_name}
                (
                 yahoo_ticker STRING,
                 google_ticker STRING,
                 bloomberg_ticker STRING,
                 numerai_ticker STRING,
                 yahoo_ticker_old STRING,
                 yahoo_valid_pts BOOL,
                 yahoo_valid_numerai BOOL,
                 batch_timestamp STRING
                )
            """
            )

            self.bigquery_client.client.load_table_from_dataframe(
                df_tickers, f"{self.schema}.{table_name}", job_config=job_config
            )

            ### comment out the below because bigquery charges significantly more money to insert-replace tables ###

            # time.sleep(5)  # without sleeping sometimes the data does not populate.
            #
            # self.bigquery_client.run_sql(f"""
            #     CREATE OR REPLACE TABLE {self.schema}.{table_name} AS (
            #     SELECT
            #       yahoo_ticker,
            #       google_ticker,
            #       bloomberg_ticker,
            #       numerai_ticker,
            #       yahoo_ticker_old,
            #       yahoo_valid_pts,
            #       yahoo_valid_numerai,
            #       batch_timestamp
            #     FROM
            #       {self.schema}.{table_name}
            #     ORDER BY
            #       1, 2, 3, 4, 5, 6, 7, 8
            #     );
            # """)

        if self.populate_snowflake:
            (
                print("\nOverwriting stock_tickers to Snowflake...\n")
                if self.verbose
                else None
            )

            if self.write_method in [pd_writer, "write_pandas"]:
                df_tickers.columns = df_tickers.columns.str.upper()

            if self.write_method.lower() != "write_pandas":
                df_tickers.to_sql(
                    table_name,
                    con=self.snowflake_client.con,
                    if_exists="replace",
                    index=False,
                    method=self.write_method,
                    chunksize=self.to_sql_chunksize,
                )
            else:
                original_backend = self.snowflake_client.backend_engine
                self.snowflake_client.backend_engine = "snowflake_connector"
                self.snowflake_client.connect()
                write_pandas(
                    df=df_tickers,
                    conn=self.snowflake_client.con,
                    database=self.snowflake_client.database.upper(),
                    schema=self.snowflake_client.schema.upper(),
                    table_name=table_name.upper(),
                    chunk_size=self.to_sql_chunksize,
                    compression="gzip",
                    parallel=self.write_pandas_threads,
                    overwrite=True,
                    auto_create_table=True,
                )

                self.snowflake_client.backend_engine = original_backend

        return self

    def etl_prices(self, asset_class, debug_tickers=None, **kwargs):
        """
        Parameters
        ----------
        asset_class: str of the asset class (e.g. stocks, forex, or crypto).
        debug_tickers: array or tuple of tickers to debug in debug mode
        """

        self.db_client = [
            i
            for i in [self.mysql_client, self.bigquery_client, self.snowflake_client]
            if i is not None
        ][0]
        debug_tickers = () if debug_tickers is None else debug_tickers
        assert isinstance(debug_tickers, (tuple, list))

        if asset_class == "stocks":
            table_prefix = "stock_prices"

            # debug_tickers = ['AAPL', 'AMZN', 'GOOG', 'META', 'NFLX', 'SQ', 'BGA.AX', 'EUTLF', 'ZZZ.TO']

            self.df_dtype_mappings = {
                str: ["yahoo_ticker", "numerai_ticker", "bloomberg_ticker"],
                np.number: [
                    "open",
                    "high",
                    "low",
                    "close",
                    "dividends",
                    "stock_splits",
                ],
                np.integer: ["volume"],
            }

            df_tickers = self.db_client.run_sql(
                f"select yahoo_ticker, bloomberg_ticker, numerai_ticker FROM {self.schema}.stock_tickers order by 1;"
            )

        elif asset_class == "forex":
            table_prefix = "forex_prices"

            # debug_tickers = ['EURUSD=X', 'RUB=X', 'JPY=X']

            self.df_dtype_mappings = {
                str: ["yahoo_ticker", "bloomberg_ticker"],
                np.number: ["open", "high", "low", "close"],
                np.integer: ["volume"],
            }

            df_tickers = self.db_client.run_sql(
                f"select yahoo_ticker, bloomberg_ticker FROM {self.schema}.forex_pairs order by 1;"
            )

        elif asset_class == "crypto":
            table_prefix = "crypto_prices"

            # debug_tickers = ['BTC-USD', 'ETH-USD']

            self.df_dtype_mappings = {
                str: ["yahoo_ticker", "yahoo_name"],
                np.number: ["open", "high", "low", "close"],
                np.integer: ["volume"],
            }

            df_tickers = self.db_client.run_sql(
                f"select yahoo_ticker, yahoo_name FROM {self.schema}.crypto_pairs_top_250 order by 1;"
            )
        else:
            raise ValueError(
                "Non-deterministic value set for df_tickers or asset_class."
            )

        self._etl_prices(
            asset_class=asset_class,
            table_prefix=table_prefix,
            df_tickers=df_tickers,
            debug_tickers=debug_tickers,
            **kwargs,
        )
        return self

    def _etl_prices(
        self,
        asset_class,
        table_prefix,
        df_tickers,
        intervals_to_download=("1m", "2m", "5m", "1h", "1d"),
        batch_download=False,
        write_to_db_after_interval_completes=True,
        write_to_db_after_n_tickers=False,
        debug_tickers=None,
        yf_params=None,
    ):
        """
        Description
        -----------
        Download yahoo finance stock prices via yfinance python library and populate a database with the data

        Parameters
        ----------

        table_prefix: table name prefix of the interval (e.g. table_prefix='stock_prices' => table_name=stock_prices_1m)
        intervals_to_download: tuple or list of intervals to download (e.g. ('1m', '1d') )
        batch_download: bool whether to download all tickers in batch and then populate the database
            default is False because batch downloading many tickers and populating a db has many downsides:
                - if working on the cloud, it's significantly more expensive because a bigger instance is needed
                - unknown errors populating the db don't appear until all tickers have been downloaded (could take days)

            if False then the steps are as follows:
                1. download a single tickers price history (start date = max start date from the previous run)
                    if there was no previous run, default to 1950-01-
                2. if dealing with data more granular than one day, check to see which timestamps are already in the db
                    if there exists timestamps in the db then delete them from the pandas df
                3. append the db with that single tickers price data from the pandas df
                4. alter db table: sort the db by timestamp and ticker

        write_to_db_after_interval_completes: Bool whether to write to <database> after interval loop is complete
            This is beneficial when bulk downloading as it will likely lead to memory errors when populating the dwh.

            If write_to_db_after_interval_completes is False and batch_download is True:
                then download all tickers and all intervals into a dictionary of dfs, then upload each df to the dwh
            If write_to_db_after_interval_completes is True and batch_download is False:
                then download all tickers with interval i to a dictionary of dfs, then only upload that interval to dwh
                This is more memory efficient as it will wipe the interval df_i after each interval loop
            If write_to_db_after_interval_completes is False and batch_download is False:
                Then upload each ticker interval to the dwh
                This is very slow since it uploads to the dwh after each API request
            If write_to_db_after_interval_completes is True and batch_download is True:
                Raise NotImplementedError

        write_to_db_after_n_tickers: int > 0 to write to <database> when this number of tickers have been pulled.
            Used to reduce memory usage. Cannot be used with batch_download=True or write_to_db_after_interval_completes=True

        yf_params: dict of yfinance params pass to yf.Ticker(<ticker>).history(**yf_params)
        """

        if batch_download and write_to_db_after_interval_completes:
            raise NotImplementedError(
                "Cannot set write_to_db_after_interval_completes=True if batch_download=True"
            )

        if batch_download and write_to_db_after_n_tickers:
            raise NotImplementedError(
                "Cannot use parameter write_to_db_after_n_tickers if batch_download=True"
            )

        if write_to_db_after_n_tickers and write_to_db_after_interval_completes:
            raise NotImplementedError(
                """
                Cannot set write_to_db_after_interval_completes=True if write_to_db_after_n_tickers != False (not None).
                Try setting write_to_db_after_n_tickers to an integer and write_to_db_after_interval_completes=False or
                write_to_db_after_interval_completes=True and write_to_db_after_n_tickers=False.
                """
            )

        intervals_to_download = (
            (intervals_to_download,)
            if isinstance(intervals_to_download, str)
            else intervals_to_download
        )

        super().__init__(
            asset_class=asset_class, yf_params=yf_params, verbose=self.verbose
        )

        if not batch_download:
            if write_to_db_after_interval_completes or write_to_db_after_n_tickers:
                df_interval = pd.DataFrame()

            print(f"\n*** Running sequential download for {table_prefix} ***\n")

            yf_params = self.yf_params.copy()

            for i in intervals_to_download:
                (
                    print(f"\nRunning {asset_class} interval {i}\n")
                    if self.verbose
                    else None
                )

                n_tickers_counter = 0
                yf_params["interval"] = i

                self._get_max_stored_ticker_timestamps(table_name=f"{table_prefix}_{i}")
                stored_tickers = self.stored_tickers.copy()
                self.create_prices_table_if_not_exists(
                    table_name=f"{table_prefix}_{i}", asset_class=asset_class
                )

                # for debugging
                if len(debug_tickers):
                    df_tickers = df_tickers[
                        df_tickers["yahoo_ticker"].isin(debug_tickers)
                    ]

                for ticker in df_tickers["yahoo_ticker"].tolist():
                    print(f"\nRunning ticker {ticker}\n") if self.verbose else None

                    n_tickers_counter += 1
                    print(f"Ticker count: {n_tickers_counter}")

                    if ticker in stored_tickers["yahoo_ticker"].tolist():
                        start_date = (
                            stored_tickers[stored_tickers["yahoo_ticker"] == ticker][
                                "query_start_timestamp"
                            ]
                            .min()
                            .strftime("%Y-%m-%d")
                        )
                    else:
                        start_date = "1950-01-01"

                    (
                        print(f"\nStart date for ticker {ticker} is {start_date}\n")
                        if self.verbose
                        else None
                    )

                    yf_params["start"] = get_valid_yfinance_start_timestamp(
                        interval=i, start=start_date
                    )

                    df = self.download_single_symbol_price_history(
                        ticker, yf_history_params=yf_params
                    )

                    if df is None or not df.shape[0]:
                        continue

                    df = pd.merge(df, df_tickers, on="yahoo_ticker", how="left")
                    df = df[self.column_order].ffill().bfill()

                    (
                        print(f"\nConverting dtypes in df_{i} for ticker {ticker}\n")
                        if self.verbose
                        else None
                    )
                    for k, v in self.df_dtype_mappings.items():
                        try:
                            df[v] = df[v].astype(k)
                        except Exception:
                            for numeric_col in df.select_dtypes(np.number).columns:
                                df[numeric_col] = (
                                    df[numeric_col].bfill().ffill().fillna(0)
                                )
                            df[v] = df[v].astype(k)

                    if self.convert_tz_aware_to_string:
                        df["timestamp_tz_aware"] = df["timestamp_tz_aware"].astype(str)

                    gc.collect()

                    if (
                        not write_to_db_after_interval_completes
                        and not write_to_db_after_n_tickers
                    ):
                        self._write_df_to_all_dbs(
                            df=df,
                            table_name=f"{table_prefix}_{i}",
                            asset_class=asset_class,
                        )
                    else:
                        df_interval = pd.concat([df_interval, df], axis=0)

                    if write_to_db_after_n_tickers and (
                        n_tickers_counter >= write_to_db_after_n_tickers
                        or ticker == df_tickers["yahoo_ticker"].tolist()[-1]
                    ):
                        self._write_df_to_all_dbs(
                            df=df_interval,
                            table_name=f"{table_prefix}_{i}",
                            asset_class=asset_class,
                        )
                        n_tickers_counter = 0
                        df_interval = pd.DataFrame()
                        gc.collect()

                if (
                    write_to_db_after_interval_completes
                    and not write_to_db_after_n_tickers
                ):
                    self._write_df_to_all_dbs(
                        df=df_interval,
                        table_name=f"{table_prefix}_{i}",
                        asset_class=asset_class,
                    )
                    df_interval = pd.DataFrame()
                    gc.collect()

                gc.collect()

            if self.verbose:
                for ftd in self.failed_ticker_downloads:
                    self.failed_ticker_downloads[ftd] = flatten_list(
                        self.failed_ticker_downloads[ftd]
                    )
                print(f"\nFailed ticker downloads:\n{self.failed_ticker_downloads}")

        else:
            print("\n*** Running batch download ***\n")

            # for debugging purposes
            if len(debug_tickers):
                df_tickers = df_tickers[df_tickers["yahoo_ticker"].isin(debug_tickers)]

            max_timestamps = []
            for i in intervals_to_download:
                self._get_max_stored_ticker_timestamps(table_name=f"{table_prefix}_{i}")
                if self.stored_tickers.shape[0]:
                    query_start_timestamp = (
                        self.stored_tickers["query_start_timestamp"]
                        .min()
                        .strftime("%Y-%m-%d")
                    )
                else:
                    query_start_timestamp = "1950-01-01"
                max_timestamps.append(query_start_timestamp)

            yf_params["start"] = min(max_timestamps)

            self.batch_download_price_history(
                df_tickers["yahoo_ticker"].unique().tolist(),
                table_prefix=table_prefix,
                intervals_to_download=intervals_to_download,
                yf_history_params=yf_params,
            )

            dfs = self.dict_of_dfs
            gc.collect()

            for i in dfs.keys():
                self.create_prices_table_if_not_exists(
                    table_name=f"{table_prefix}_{i}", asset_class=asset_class
                )

                print(f"\nMerging df_{i}...\n") if self.verbose else None

                df = pd.merge(dfs[i], df_tickers, on="yahoo_ticker", how="left")
                if yf_params["repair"] and "repaired" in df.columns:
                    df = df.drop("repaired", axis=1)

                assert (
                    len(np.intersect1d(df.columns, self.column_order)) == df.shape[1]
                ), "Column mismatch! Review method that downloads yfinance data!"

                print(f"\nBackfilling df_{i}...\n") if self.verbose else None
                df = df.ffill().bfill()

                print(f"\nSorting by date in df_{i}...\n") if self.verbose else None
                df.sort_values(by="timestamp", inplace=True)

                print(f"\nConverting dtypes in df_{i}...\n") if self.verbose else None
                for k, v in self.df_dtype_mappings.items():
                    df[v] = df[v].astype(k)

                df = df[self.column_order]

                if self.convert_tz_aware_to_string:
                    df["timestamp_tz_aware"] = df["timestamp_tz_aware"].astype(str)

                self._write_df_to_all_dbs(
                    df=df, table_name=f"{table_prefix}_{i}", asset_class=asset_class
                )
                gc.collect()
        return

    def get_market_calendar(self):
        """
        Description
        -----------
        Get pandas market calendar to determine when the market was open / closed.
        """

        date_range = pd.date_range(
            start="1950-01-01", end=datetime.today().strftime("%Y-%m-%d")
        )

        self.df_calendar = (
            pmc.get_calendar("24/7")
            .schedule(
                start_date=date_range.min().strftime("%Y-%m-%d"),
                end_date=date_range.max().strftime("%Y-%m-%d"),
            )
            .drop_duplicates()
        )

        self.market_closed_dates = []
        for idx in range(self.df_calendar.shape[0] - 1):
            close_date = self.df_calendar.iloc[idx]["market_close"].strftime("%Y-%m-%d")
            next_open_date = (
                self.df_calendar.shift(-1).iloc[idx]["market_open"].strftime("%Y-%m-%d")
            )

            if pd.to_datetime(next_open_date) > pd.to_datetime(close_date):
                market_closed_dates_i = [
                    d.strftime("%Y-%m-%d")
                    for d in pd.date_range(
                        close_date, next_open_date, freq="d"
                    ).tolist()[1:-1]
                ]

                self.market_closed_dates.append(market_closed_dates_i)
        self.market_closed_dates = sorted(
            list(set(flatten_list(self.market_closed_dates)))
        )

        return self

    ###### crypto ######

    def etl_top_250_crypto_tickers(self):
        df_top_crypto_tickers_new = (
            self.ticker_downloader.download_top_250_crypto_tickers()
        )

        if self.verbose:
            print(
                f"""
                \nOverwriting top_250_crypto_tickers to database(s): {self.dwh_connections.keys()}...
                \nIf a ticker has been a part of the top 250 crypto tickers (>= min date) it will be a part of this table.\n
            """
            )

        column_order = [
            "yahoo_ticker",
            "yahoo_name",
            "price_intraday",
            "change",
            "pct_change",
            "market_cap",
            "volume_in_currency_since_0_00_utc",
            "volume_in_currency_24_hr",
            "total_volume_all_currencies_24_hr",
            "circulating_supply",
            "batch_timestamp",
        ]

        df_top_crypto_tickers_new["batch_timestamp"] = datetime.utcnow()
        df_top_crypto_tickers_new = df_top_crypto_tickers_new[column_order]

        if self.populate_mysql:
            (
                print("\nOverwriting top_250_crypto_tickers to MySQL...\n")
                if self.verbose
                else None
            )
            method = (
                "multi" if self.write_method == "write_pandas" else self.write_method
            )

            tables = self.mysql_client.run_sql(
                f"""
                select
                  table_name
                from
                  information_schema.tables
                where
                  table_schema = '{self.schema}'
                """
            )

            table_exists = (
                True
                if "crypto_pairs_top_250" in flatten_list(tables.values.tolist())
                else False
            )

            if table_exists:
                df_top_crypto_tickers_prev = self.mysql_client.run_sql(
                    f"select * from {self.schema}.crypto_pairs_top_250"
                )
                df_top_crypto_tickers_prev = df_top_crypto_tickers_prev[column_order]
                df_top_crypto_tickers = (
                    pd.concat(
                        [df_top_crypto_tickers_prev, df_top_crypto_tickers_new],
                        ignore_index=True,
                    )
                    .drop_duplicates(subset=["yahoo_ticker"], keep="last")
                    .reset_index(drop=True)
                )
            else:
                df_top_crypto_tickers = df_top_crypto_tickers_new

            self.mysql_client.connect()

            df_top_crypto_tickers.to_sql(
                "crypto_pairs_top_250",
                schema=self.schema,
                con=self.mysql_client.con,
                if_exists="replace",
                index=False,
                method=method,
                chunksize=self.to_sql_chunksize,
            )

        if self.populate_bigquery:
            (
                print("\nOverwriting crypto_pairs_top_250 to BigQuery...\n")
                if self.verbose
                else None
            )

            job_config = bigquery.job.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )

            table_generator = self.bigquery_client.client.list_tables(
                f"{self.bigquery_client.client.project}.{self.schema}"
            )

            table_exists = False
            for t in table_generator:
                if t.table_id == "crypto_pairs_top_250":
                    table_exists = True
                    break

            self.bigquery_client.run_sql(
                f"""
                            CREATE TABLE IF NOT EXISTS {self.schema}.crypto_pairs_top_250
                            (
                             yahoo_ticker STRING,
                             name STRING,
                             price_intraday FLOAT64,
                             change FLOAT64,
                             pct_change STRING,
                             market_cap STRING,
                             volume_in_currency_since_0_00_utc STRING,
                             volume_in_currency_24_hr STRING,
                             total_volume_all_currencies_24_hr STRING,
                             circulating_supply STRING,
                             batch_timestamp TIMESTAMP
                            )
                        """
            )

            if table_exists:
                df_top_crypto_tickers_prev = self.bigquery_client.run_sql(
                    f"select * from {self.schema}.crypto_pairs_top_250"
                )

                df_top_crypto_tickers_prev = df_top_crypto_tickers_prev[column_order]
                df_top_crypto_tickers = (
                    pd.concat(
                        [df_top_crypto_tickers_prev, df_top_crypto_tickers_new],
                        ignore_index=True,
                    )
                    .drop_duplicates(subset=["yahoo_ticker"], keep="last")
                    .reset_index(drop=True)
                )
            else:
                df_top_crypto_tickers = df_top_crypto_tickers_new

            self.bigquery_client.client.load_table_from_dataframe(
                df_top_crypto_tickers,
                f"{self.schema}.crypto_pairs_top_250",
                job_config=job_config,
            )

            time.sleep(5)
            self.bigquery_client.run_sql(
                f"""
                CREATE OR REPLACE TABLE {self.schema}.crypto_pairs_top_250 AS (
                  SELECT * FROM  {self.schema}.crypto_pairs_top_250 ORDER BY yahoo_ticker
                );
            """
            )

        if self.populate_snowflake:
            (
                print("\nOverwriting crypto_pairs_top_250 to Snowflake...\n")
                if self.verbose
                else None
            )

            tables = self.snowflake_client.run_sql(
                f"""
                            select
                              table_name
                            from
                              information_schema.tables
                            where
                              lower(table_catalog) = '{self.database.lower()}'
                              and lower(table_schema) = '{self.schema.lower()}'
                            """
            )

            table_exists = (
                True
                if "crypto_pairs_top_250".upper()
                in [i.upper() for i in flatten_list(tables.values.tolist())]
                else False
            )

            if table_exists:
                df_top_crypto_tickers_prev = self.snowflake_client.run_sql(
                    f"""
                        select
                          yahoo_ticker,
                          yahoo_name,
                          price_intraday,
                          change,
                          pct_change,
                          market_cap,
                          volume_in_currency_since_0_00_utc,
                          volume_in_currency_24_hr,
                          total_volume_all_currencies_24_hr,
                          circulating_supply,
                          batch_timestamp
                        from
                          {self.schema}.crypto_pairs_top_250
                        """
                )

                df_top_crypto_tickers = (
                    pd.concat(
                        [df_top_crypto_tickers_prev, df_top_crypto_tickers_new],
                        ignore_index=True,
                    )
                    .drop_duplicates(subset=["yahoo_ticker"], keep="last")
                    .reset_index(drop=True)
                )
            else:
                df_top_crypto_tickers = df_top_crypto_tickers_new

            df_top_crypto_tickers["batch_timestamp"] = datetime.utcnow()

            if self.write_method.lower() != "write_pandas":
                self.snowflake_client.connect()

                # table name needs to be lower case for snowflake sqlalchemy to_sql (sqlalchemy.exc.InvalidRequestError)
                df_top_crypto_tickers.to_sql(
                    "crypto_pairs_top_250",
                    con=self.snowflake_client.con,
                    if_exists="replace",
                    index=False,
                    method=self.write_method,
                    chunksize=self.to_sql_chunksize,
                )
            else:
                original_backend = self.snowflake_client.backend_engine
                self.snowflake_client.backend_engine = "snowflake_connector"
                self.snowflake_client.connect()
                wp_df_top250_crypto = df_top_crypto_tickers.copy()
                wp_df_top250_crypto["batch_timestamp"] = wp_df_top250_crypto[
                    "batch_timestamp"
                ].astype(str)
                wp_df_top250_crypto.columns = wp_df_top250_crypto.columns.str.upper()
                write_pandas(
                    df=wp_df_top250_crypto,
                    conn=self.snowflake_client.con,
                    database=self.snowflake_client.database.upper(),
                    schema=self.snowflake_client.schema.upper(),
                    table_name="crypto_pairs_top_250".upper(),
                    chunk_size=self.to_sql_chunksize,
                    compression="gzip",
                    parallel=self.write_pandas_threads,
                    overwrite=True,
                    auto_create_table=True,
                )
                self.snowflake_client.backend_engine = original_backend
        return self

    ###### forex ######

    def etl_forex_pairs(self):
        """
        ELT for forex pair ticker names.
        """
        df_forex_pairs_new = self.ticker_downloader.download_forex_pairs()

        if self.verbose:
            print(
                f"""\nOverwriting forex_pairs to database(s): {self.dwh_connections.keys()}..."""
            )

        if self.populate_mysql:
            print("\nOverwriting forex_pairs to MySQL...\n") if self.verbose else None
            method = (
                "multi" if self.write_method == "write_pandas" else self.write_method
            )

            tables = self.mysql_client.run_sql(
                f"""
                        select
                          table_name
                        from
                          information_schema.tables
                        where
                          table_schema = '{self.schema}'
                        """
            )

            table_exists = (
                True if "forex_pairs" in flatten_list(tables.values.tolist()) else False
            )

            if table_exists:
                df_forex_pairs_prev = self.mysql_client.run_sql(
                    f"select * from {self.schema}.forex_pairs"
                )
                df_forex_pairs = (
                    pd.concat(
                        [df_forex_pairs_prev, df_forex_pairs_new], ignore_index=True
                    )
                    .drop_duplicates()
                    .reset_index(drop=True)
                )
            else:
                df_forex_pairs = df_forex_pairs_new

            self.mysql_client.connect()

            df_forex_pairs["batch_timestamp"] = datetime.utcnow()
            df_forex_pairs.to_sql(
                "forex_pairs",
                schema=self.schema,
                con=self.mysql_client.con,
                if_exists="replace",
                index=False,
                method=method,
                chunksize=self.to_sql_chunksize,
            )

        if self.populate_bigquery:
            (
                print("\nOverwriting forex_pairs to BigQuery...\n")
                if self.verbose
                else None
            )

            job_config = bigquery.job.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )

            table_generator = self.bigquery_client.client.list_tables(
                f"{self.bigquery_client.client.project}.{self.schema}"
            )

            table_exists = False
            for t in table_generator:
                if t.table_id == "forex_pairs":
                    table_exists = True
                    break

            self.bigquery_client.run_sql(
                f"CREATE TABLE IF NOT EXISTS {self.schema}.forex_pairs (forex_pair STRING)"
            )

            if table_exists:
                df_forex_pairs_prev = self.bigquery_client.run_sql(
                    f"select * from {self.schema}.forex_pairs"
                )
                df_forex_pairs = (
                    pd.concat(
                        [df_forex_pairs_prev, df_forex_pairs_new], ignore_index=True
                    )
                    .drop_duplicates()
                    .reset_index(drop=True)
                )
            else:
                df_forex_pairs = df_forex_pairs_new

            df_forex_pairs["batch_timestamp"] = datetime.utcnow()
            self.bigquery_client.client.load_table_from_dataframe(
                df_forex_pairs, f"{self.schema}.forex_pairs", job_config=job_config
            )

            time.sleep(5)

            self.bigquery_client.run_sql(
                f"""
                        CREATE OR REPLACE TABLE {self.schema}.forex_pairs AS (
                          SELECT * FROM  {self.schema}.forex_pairs ORDER BY 1
                        );
                    """
            )

        if self.populate_snowflake:
            (
                print("\nOverwriting forex_pairs to Snowflake...\n")
                if self.verbose
                else None
            )

            tables = self.snowflake_client.run_sql(
                f"""
                                    select
                                      table_name
                                    from
                                      information_schema.tables
                                    where
                                      lower(table_catalog) = '{self.database.lower()}'
                                      and lower(table_schema) = '{self.schema.lower()}'
                                    """
            )

            table_exists = (
                True if "FOREX_PAIRS" in flatten_list(tables.values.tolist()) else False
            )

            if table_exists:
                df_forex_pairs_prev = self.snowflake_client.run_sql(
                    f"""
                     select
                       *
                     from
                       {self.schema}.forex_pairs
                    """
                )

                df_forex_pairs_prev = df_forex_pairs_prev[
                    [i for i in df_forex_pairs_prev.columns]
                ]
                df_forex_pairs = (
                    pd.concat(
                        [df_forex_pairs_prev, df_forex_pairs_new], ignore_index=True
                    )
                    .drop_duplicates(subset=["yahoo_ticker"], keep="last")
                    .reset_index(drop=True)
                )
            else:
                df_forex_pairs = df_forex_pairs_new

            if self.write_method.lower() != "write_pandas":
                df_forex_pairs["batch_timestamp"] = datetime.utcnow()
                self.snowflake_client.connect()

                # must grant privileges SNOWFLAKE_ROLE in .env or the below code chunk will break.
                df_forex_pairs.to_sql(
                    "forex_pairs",
                    con=self.snowflake_client.con,
                    if_exists="replace",
                    method=self.write_method,
                    index=False,
                    chunksize=self.to_sql_chunksize,
                )
            else:
                original_backend = self.snowflake_client.backend_engine
                self.snowflake_client.backend_engine = "snowflake_connector"
                self.snowflake_client.connect()
                wp_df_forex = df_forex_pairs.copy()
                wp_df_forex["batch_timestamp"] = wp_df_forex["batch_timestamp"].astype(
                    str
                )
                wp_df_forex.columns = wp_df_forex.columns.str.upper()
                write_pandas(
                    df=wp_df_forex,
                    conn=self.snowflake_client.con,
                    database=self.snowflake_client.database.upper(),
                    schema=self.snowflake_client.schema.upper(),
                    table_name="FOREX_PAIRS",
                    chunk_size=self.to_sql_chunksize,
                    compression="gzip",
                    parallel=self.write_pandas_threads,
                    overwrite=True,
                    auto_create_table=True,
                )
                self.snowflake_client.backend_engine = original_backend

        return self

    ###### dwh attribute methods ######

    def create_prices_table_if_not_exists(self, table_name, asset_class):
        """
        Description
        -----------
        create a stock_price_{interval} table within a specific schema (e.g. yfinance)

        Parameters
        ----------
        table_name: str name of the table to be created
        asset_class: str of the asset class to run queries on
        """

        for dwh_name in self.dwh_connections.keys():
            if dwh_name == "mysql":
                if self.convert_tz_aware_to_string:
                    tz_aware_col = "timestamp_tz_aware CHAR(32) NOT NULL"
                else:
                    tz_aware_col = "timestamp_tz_aware DATETIME NOT NULL"

                if asset_class == "stocks":
                    query = f"""
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
                          stock_splits DECIMAL(38, 12),
                          batch_timestamp DATETIME NOT NULL
                          )
                          ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
                        """

                elif asset_class == "forex":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {self.schema}.{table_name} (
                          timestamp DATETIME NOT NULL,
                          {tz_aware_col},
                          timezone VARCHAR(32),
                          yahoo_ticker VARCHAR(32),
                          bloomberg_ticker VARCHAR(32),
                          open DECIMAL(38, 12),
                          high DECIMAL(38, 12),
                          low DECIMAL(38, 12),
                          close DECIMAL(38, 12),
                          volume DECIMAL(38, 12),
                          batch_timestamp DATETIME NOT NULL
                          )
                          ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
                        """

                elif asset_class == "crypto":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {self.schema}.{table_name} (
                          timestamp DATETIME NOT NULL,
                          {tz_aware_col},
                          timezone VARCHAR(32),
                          yahoo_ticker VARCHAR(32),
                          yahoo_name VARCHAR(32),
                          open DECIMAL(38, 12),
                          high DECIMAL(38, 12),
                          low DECIMAL(38, 12),
                          close DECIMAL(38, 12),
                          volume DECIMAL(38, 12),
                          batch_timestamp DATETIME NOT NULL
                          )
                          ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
                        """
                else:
                    raise RuntimeError(
                        "asset_class must be set to either 'stocks', 'forex', or 'crypto'"
                    )

                self.dwh_connections[dwh_name].run_sql(query)

            elif dwh_name == "bigquery":
                tz_aware_dtype = (
                    "STRING" if self.convert_tz_aware_to_string else "TIMESTAMP"
                )

                if asset_class == "stocks":
                    self.job_config = bigquery.LoadJobConfig(
                        schema=[
                            bigquery.SchemaField(
                                name="timestamp", field_type="TIMESTAMP"
                            ),
                            bigquery.SchemaField(
                                name="timestamp_tz_aware", field_type=tz_aware_dtype
                            ),
                            bigquery.SchemaField(name="timezone", field_type="STRING"),
                            bigquery.SchemaField(
                                name="yahoo_ticker", field_type="STRING"
                            ),
                            bigquery.SchemaField(
                                name="bloomberg_ticker", field_type="STRING"
                            ),
                            bigquery.SchemaField(
                                name="numerai_ticker", field_type="STRING"
                            ),
                            bigquery.SchemaField(name="open", field_type="NUMERIC"),
                            bigquery.SchemaField(name="high", field_type="NUMERIC"),
                            bigquery.SchemaField(name="low", field_type="NUMERIC"),
                            bigquery.SchemaField(name="close", field_type="NUMERIC"),
                            bigquery.SchemaField(name="volume", field_type="INTEGER"),
                            bigquery.SchemaField(
                                name="dividends", field_type="NUMERIC"
                            ),
                            bigquery.SchemaField(
                                name="stock_splits", field_type="NUMERIC"
                            ),
                            bigquery.SchemaField(
                                name="batch_timestamp", field_type="TIMESTAMP"
                            ),
                        ],
                        autodetect=False,
                    )
                elif asset_class == "forex":
                    self.job_config = bigquery.LoadJobConfig(
                        schema=[
                            bigquery.SchemaField(
                                name="timestamp", field_type="TIMESTAMP"
                            ),
                            bigquery.SchemaField(
                                name="timestamp_tz_aware", field_type=tz_aware_dtype
                            ),
                            bigquery.SchemaField(name="timezone", field_type="STRING"),
                            bigquery.SchemaField(
                                name="yahoo_ticker", field_type="STRING"
                            ),
                            bigquery.SchemaField(
                                name="bloomberg_ticker", field_type="STRING"
                            ),
                            bigquery.SchemaField(name="open", field_type="NUMERIC"),
                            bigquery.SchemaField(name="high", field_type="NUMERIC"),
                            bigquery.SchemaField(name="low", field_type="NUMERIC"),
                            bigquery.SchemaField(name="close", field_type="NUMERIC"),
                            bigquery.SchemaField(name="volume", field_type="INTEGER"),
                            bigquery.SchemaField(
                                name="batch_timestamp", field_type="TIMESTAMP"
                            ),
                        ],
                        autodetect=False,
                    )
                elif asset_class == "crypto":
                    self.job_config = bigquery.LoadJobConfig(
                        schema=[
                            bigquery.SchemaField(
                                name="timestamp", field_type="TIMESTAMP"
                            ),
                            bigquery.SchemaField(
                                name="timestamp_tz_aware", field_type=tz_aware_dtype
                            ),
                            bigquery.SchemaField(name="timezone", field_type="STRING"),
                            bigquery.SchemaField(
                                name="yahoo_ticker", field_type="STRING"
                            ),
                            bigquery.SchemaField(
                                name="yahoo_name", field_type="STRING"
                            ),
                            bigquery.SchemaField(name="open", field_type="NUMERIC"),
                            bigquery.SchemaField(name="high", field_type="NUMERIC"),
                            bigquery.SchemaField(name="low", field_type="NUMERIC"),
                            bigquery.SchemaField(name="close", field_type="NUMERIC"),
                            bigquery.SchemaField(name="volume", field_type="INTEGER"),
                            bigquery.SchemaField(
                                name="batch_timestamp", field_type="TIMESTAMP"
                            ),
                        ],
                        autodetect=False,
                    )

            elif dwh_name == "snowflake":
                if self.convert_tz_aware_to_string:
                    tz_aware_col = "timestamp_tz_aware STRING NOT NULL"
                else:
                    tz_aware_col = "timestamp_tz_aware TIMESTAMP_TZ NOT NULL"

                if asset_class == "stocks":
                    query = f"""
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
                          stock_splits FLOAT,
                          batch_timestamp TIMESTAMP_NTZ NOT NULL
                          );
                        """
                elif asset_class == "forex":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                          timestamp TIMESTAMP_NTZ NOT NULL,
                          {tz_aware_col},
                          timezone STRING,
                          yahoo_ticker STRING,
                          bloomberg_ticker STRING,
                          open FLOAT,
                          high FLOAT,
                          low FLOAT,
                          close FLOAT,
                          volume FLOAT,
                          batch_timestamp TIMESTAMP_NTZ NOT NULL
                          );
                        """
                elif asset_class == "crypto":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                          timestamp TIMESTAMP_NTZ NOT NULL,
                          {tz_aware_col},
                          timezone STRING,
                          yahoo_ticker STRING,
                          yahoo_name STRING,
                          open FLOAT,
                          high FLOAT,
                          low FLOAT,
                          close FLOAT,
                          volume FLOAT,
                          batch_timestamp TIMESTAMP_NTZ NOT NULL
                          );
                        """
                else:
                    raise RuntimeError(
                        "asset_class must be set to either 'stocks', 'forex', or 'crypto'"
                    )

                self.dwh_connections[dwh_name].run_sql(query)
        return self

    def _get_max_stored_ticker_timestamps(self, table_name):
        self.stored_tickers = pd.DataFrame(columns=["yahoo_ticker"])

        for dwh_name in self.dwh_connections.keys():
            if dwh_name in ["mysql", "snowflake"]:
                existing_tables = (
                    self.dwh_connections[dwh_name]
                    .run_sql(
                        f"""
                        SELECT
                          DISTINCT(table_name)
                        FROM
                          information_schema.tables
                        WHERE
                          lower(table_schema) = '{self.dwh_connections[dwh_name].schema}'
                        """
                    )
                    .pipe(lambda x: clean_columns(x))
                )

                stored_tickers_table = (
                    f"{self.dwh_connections[dwh_name].schema}.{table_name}"
                )

            elif dwh_name == "bigquery":
                existing_tables = (
                    self.dwh_connections[dwh_name]
                    .run_sql(
                        f"""
                        SELECT
                          DISTINCT(table_name)
                        FROM
                          `{self.dwh_connections[dwh_name].schema}.INFORMATION_SCHEMA.TABLES`;
                        """
                    )
                    .pipe(lambda x: clean_columns(x))
                )

                stored_tickers_table = f"`{self.dwh_connections[dwh_name].client.project}.{self.dwh_connections[dwh_name].schema}.{table_name}`"
            else:
                raise RuntimeError(
                    "dwh_name must be set to either 'mysql', 'bigquery', or 'snowflake'"
                )

            existing_tables["table_name"] = existing_tables["table_name"].str.lower()

            if f"{table_name}".lower() in existing_tables["table_name"].tolist():
                tmp_stored_tickers = self.dwh_connections[dwh_name].run_sql(
                    f"""
                        SELECT
                            yahoo_ticker,
                            MAX(timestamp) AS {dwh_name}_max_timestamp
                        FROM
                            {stored_tickers_table}
                        GROUP BY 1
                        """
                )
                self.stored_tickers = pd.merge(
                    self.stored_tickers,
                    tmp_stored_tickers,
                    how="outer",
                    on="yahoo_ticker",
                )

        self.stored_tickers["query_start_timestamp"] = (
            self.stored_tickers[
                [i for i in self.stored_tickers.columns if i.endswith("max_timestamp")]
            ]
            .apply(lambda x: pd.to_datetime(x.fillna("1970-01-01 00:00:00"), utc=True))
            .min(axis=1)
        )

        return self

    def _write_df_to_all_dbs(self, df, table_name, asset_class):
        """
        Description
        -----------
        Write dataframe of specified interval to the dwhs supplied in initialization (e.g. populate_snowflake, etc).
        Tables will be stored to self.schema.<table_name> in the dwh

        Parameters:
            df: pandas df of stock price data
            table_name: str of the table name to populate bigquery with (e.g. <schema>.<table_name>)
            asset_class: str of the asset class (e.g. 'stocks', 'forex', or 'crypto')
        """

        try:
            if self.populate_mysql:
                self._write_to_mysql(
                    df=df, table_name=table_name, asset_class=asset_class
                )
                if self.dedupe_after_populate:
                    self._dedupe_mysql_table(table_name=table_name)

            if self.populate_snowflake:
                self._write_to_snowflake(
                    df=df, table_name=table_name, asset_class=asset_class
                )
                if self.dedupe_after_populate:
                    self._dedupe_snowflake_table(
                        table_name=table_name, asset_class=asset_class
                    )

            if self.populate_bigquery:
                self._write_to_bigquery(df=df, table_name=table_name)
                if self.dedupe_after_populate:
                    self._dedupe_bigquery_table(
                        table_name=table_name, asset_class=asset_class
                    )
        except Exception as e:
            print(traceback.format_exc())
            print(f"\n{e}\n")
            pd.options.display.max_columns = (
                20  # so the full df is printed in the error
            )
            print(f"\n{df}\n")
            print(f"\n{df.dtypes}\n")
        return self

    def _get_query_dtype_fix(self, table_name, asset_class):
        """
        Description
        -----------
        Static query dtype fix that is called by other methods

        Parameters:
            table_name: str of the table name that was pulled from yfinance (e.g. 'stock_prices_1m')
        """

        if asset_class == "stocks":
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
                    stock_splits,
                    batch_timestamp
                  FROM
                    {self.schema}.{table_name};
                """
        elif asset_class == "forex":
            query_dtype_fix = f"""
                DROP TABLE IF EXISTS {self.schema}.tmp_table;
                CREATE TABLE {self.schema}.tmp_table AS
                  SELECT
                    timestamp,
                    CAST(timestamp_tz_aware AS CHAR(32)) AS timestamp_tz_aware,
                    timezone,
                    yahoo_ticker,
                    bloomberg_ticker,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    batch_timestamp
                  FROM
                    {self.schema}.{table_name};
                """
        elif asset_class == "crypto":
            query_dtype_fix = f"""
                DROP TABLE IF EXISTS {self.schema}.tmp_table;
                CREATE TABLE {self.schema}.tmp_table AS
                  SELECT
                    timestamp,
                    CAST(timestamp_tz_aware AS CHAR(32)) AS timestamp_tz_aware,
                    timezone,
                    yahoo_ticker,
                    yahoo_name,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    batch_timestamp
                  FROM
                    {self.schema}.{table_name};
                """
        else:
            raise RuntimeError(
                "asset_class must be set to either 'stocks', 'forex', or 'crypto'"
            )

        return query_dtype_fix

    def _write_to_mysql(self, df, table_name, asset_class):
        """
        Description
        -----------
        Write dataframe of specified interval to the MySQL.
        Tables will be stored to self.schema.<table_name> in MySQL.

        Parameters:
            df: pandas df of stock price data
            table_name: str of the interval that was pulled from yfinance (e.g. '1m')
        """

        print("\nWriting to database MySQL...\n") if self.verbose else None
        self._drop_mysql_index_constraint(table_name=table_name)
        method = "multi" if self.write_method == "write_pandas" else self.write_method
        try:
            self.mysql_client.connect()
            df["batch_timestamp"] = datetime.utcnow()
            df.to_sql(
                table_name,
                con=self.mysql_client.con,
                index=False,
                if_exists="append",
                schema=self.schema,
                method=method,
                chunksize=self.to_sql_chunksize,
            )
        except Exception:
            warnings.warn(
                """
                        Could not directly populate database with df.
                        This is likely because of the timestamp_tz_aware column. Converting it to string...
                        """
            )

            query_dtype_fix = self._get_query_dtype_fix(
                table_name=table_name, asset_class=asset_class
            )
            self.mysql_client.run_sql(query_dtype_fix)

            df["timestamp_tz_aware"] = df["timestamp_tz_aware"].astype(str)

            self.mysql_client.connect()
            df["batch_timestamp"] = datetime.utcnow()
            df.to_sql(
                table_name,
                con=self.mysql_client.con,
                index=False,
                if_exists="append",
                schema=self.schema,
                method=method,
                chunksize=self.to_sql_chunksize,
            )

        gc.collect()
        return self

    def _dedupe_mysql_table(self, table_name):
        """
        Description
        -----------
        Query that dedupes MySQL table stock_price_<interval> by timestamp, yahoo_ticker
        Note: The table MUST have timestamp and yahoo_ticker as columns

        Parameters:
            table_name: str of the table name to populate MySQL
        """

        print(f"\nDeduping MySQL table {table_name}...\n") if self.verbose else None

        query_statements = f"""
            ALTER TABLE {self.schema}.{table_name} ADD COLUMN to_keep BOOLEAN;
            ALTER TABLE {self.schema}.{table_name}
            ADD CONSTRAINT dedupe UNIQUE (timestamp, yahoo_ticker, to_keep);
            UPDATE IGNORE {self.schema}.{table_name} SET to_keep = true;
            DELETE FROM {self.schema}.{table_name} WHERE to_keep IS NULL;
            ALTER TABLE {self.schema}.{table_name} DROP to_keep;
            """

        self.mysql_client.run_sql(query_statements)

        # create index on timestamp column
        idx_cols = self.mysql_client.run_sql(
            f"""
                SELECT
                  *
                FROM
                  information_schema.statistics
                WHERE
                  table_schema = '{self.mysql_client.schema}'
                  AND table_name = '{table_name}'
            """
        )

        if "timestamp" not in idx_cols["COLUMN_NAME"].tolist():
            self.mysql_client.run_sql(
                f"CREATE INDEX ts ON {self.schema}.{table_name} (timestamp);"
            )

        return self

    def _write_to_snowflake(self, df, table_name, asset_class):
        """
        Description
        -----------
        Write dataframe of specified interval to the Snowflake.
        Tables will be stored to self.schema.<table_name> in Snowflake.

        Parameters:
            df: pandas df of stock price data
            table_name: str of the table name to populate snowflake with
            asset_class: str of the asset class (e.g. 'stocks')
        """

        print("\nWriting to Snowflake...\n") if self.verbose else None

        try:
            if self.write_method in [pd_writer, "write_pandas"]:
                df.columns = df.columns.str.upper()

            if self.write_method == "write_pandas":
                original_backend = self.snowflake_client.backend_engine
                del self.snowflake_client
                self._connect_to_snowflake()
                self.dwh_connections["snowflake"] = self.snowflake_client
                self.snowflake_client.backend_engine = "snowflake_connector"
                self.snowflake_client.connect()
                write_pandas(
                    df=df,
                    conn=self.snowflake_client.con,
                    database=self.snowflake_client.database.upper(),
                    schema=self.snowflake_client.schema.upper(),
                    table_name=table_name.upper(),
                    chunk_size=self.to_sql_chunksize,
                    compression="gzip",
                    parallel=self.write_pandas_threads,
                    overwrite=False,
                    auto_create_table=True,
                )
                self.snowflake_client.backend_engine = original_backend

            else:
                self.snowflake_client.connect()
                df["batch_timestamp"] = datetime.utcnow()
                df.to_sql(
                    table_name,
                    con=self.snowflake_client.con,
                    index=False,
                    if_exists="append",
                    schema=self.schema,
                    method=self.write_method,
                    chunksize=self.to_sql_chunksize,
                )
        except Exception:
            warnings.warn(
                """
                        Could not directly populate database with df.
                        This is likely because of the timestamp_tz_aware column. Converting it to string...
                        """
            )

            query_dtype_fix = self._get_query_dtype_fix(
                table_name=table_name, asset_class=asset_class
            )
            separate_query_statements = query_dtype_fix.split(";")
            for query in separate_query_statements[0:-1]:
                query = query.replace("\n", "").replace("  ", "") + ";"
                print(f"\n\nquery: {query}\n\n") if self.verbose else None
                self.snowflake_client.run_sql(query)

            df["timestamp_tz_aware"] = df["timestamp_tz_aware"].astype(str)

            if self.write_method in [pd_writer, "write_pandas"]:
                df.columns = df.columns.str.upper()

            if self.write_method == "write_pandas":
                original_backend = self.snowflake_client.backend_engine
                self.snowflake_client.backend_engine = "snowflake_connector"
                self.snowflake_client.connect()
                write_pandas(
                    df=df,
                    conn=self.snowflake_client.con,
                    database=self.snowflake_client.database.upper(),
                    schema=self.snowflake_client.schema.upper(),
                    table_name=table_name.upper(),
                    chunk_size=self.to_sql_chunksize,
                    compression="gzip",
                    parallel=self.write_pandas_threads,
                    overwrite=False,
                    auto_create_table=True,
                )

                self.snowflake_client.backend_engine = original_backend

            else:
                df["batch_timestamp"] = datetime.utcnow()
                df.to_sql(
                    table_name,
                    con=self.snowflake_client.con,
                    index=False,
                    if_exists="append",
                    schema=self.schema,
                    method=self.write_method,
                    chunksize=self.to_sql_chunksize,
                )
        return self

    def _dedupe_snowflake_table(self, table_name, asset_class):
        """
        Description
        -----------
        Query that dedupes Snowflake table stock_price_<interval> by timestamp, yahoo_ticker

        Parameters:
            table_name: str of the table name to dedupe in Snowflake.
        """

        print(f"\nDeduping Snowflake table {table_name}...\n") if self.verbose else None

        if asset_class == "stocks":
            columns = """
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
              stock_splits,
              batch_timestamp
            """

            order_by_columns = "timestamp, yahoo_ticker, bloomberg_ticker, numerai_ticker, batch_timestamp desc"

        elif asset_class == "forex":
            columns = """
              timestamp,
              timestamp_tz_aware,
              timezone,
              yahoo_ticker,
              bloomberg_ticker,
              open,
              high,
              low,
              close,
              volume,
              batch_timestamp
            """

            order_by_columns = (
                "timestamp, yahoo_ticker, bloomberg_ticker, batch_timestamp desc"
            )

        elif asset_class == "crypto":
            columns = """
              timestamp,
              timestamp_tz_aware,
              timezone,
              yahoo_ticker,
              yahoo_name,
              open,
              high,
              low,
              close,
              volume,
              batch_timestamp
            """

            order_by_columns = (
                "timestamp, yahoo_ticker, yahoo_name, batch_timestamp desc"
            )

        else:
            raise RuntimeError("Columns not successfully parsed.")

        self.snowflake_client.run_sql(f"USE {self.snowflake_client.database}")

        initial_syntax = f"INSERT OVERWRITE INTO {self.schema}.{table_name} "
        query_statements = f"""
                    {initial_syntax}
                    SELECT
                      {columns}
                    FROM
                      {self.schema}.{table_name}
                    QUALIFY row_number() over (PARTITION BY timestamp, yahoo_ticker ORDER BY timestamp DESC, batch_timestamp DESC) = 1
                    ORDER BY
                      {order_by_columns};
                """

        separate_query_statements = query_statements.split(";")
        for query in separate_query_statements[0:-1]:
            query = query.replace("\n", "").replace("  ", "") + ";"
            print(f"\n\nQuery: {query}\n\n") if self.verbose else None
            self.snowflake_client.run_sql(query)

        self.snowflake_client.disconnect()

        if "snowflake" in self.create_timestamp_index_dbs:
            raise NotImplementedError(
                "No need to create timestamp indices on bigquery or snowflake."
            )

        return self

    def _write_to_bigquery(
        self, df, table_name, retry_cache_dir=os.path.expanduser("~/.cache/bigquery")
    ):
        """
        Description
        -----------
        Write dataframe of specified interval to the BigQuery.
        Tables will be stored to self.schema.<table_name> in BigQuery.

        Parameters:
            df: pandas df of stock price data
            table_name: str of the table name to write to BigQuery.
        """

        print("\nWriting to BigQuery...\n") if self.verbose else None

        try:
            df.to_gbq(f"{self.schema}.{table_name}", if_exists="append")
        except Exception:
            (
                print(
                    "\nCould not directly upload df to bigquery! "
                    "Dumping to csv, loading, then trying again via bigquery client...\n"
                )
                if self.verbose
                else None
            )

            os.makedirs(retry_cache_dir, exist_ok=True)
            df.to_csv(f"{retry_cache_dir}/tmp.csv", index=False)
            df = pd.read_csv(f"{retry_cache_dir}/tmp.csv")
            job_config = bigquery.LoadJobConfig(autodetect=True)
            table_id = (
                f"{self.bigquery_client.client.project}.{self.schema}.{table_name}"
            )
            self.bigquery_client.client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            ).result()
            subprocess.run(f"rm {retry_cache_dir}/tmp.csv", shell=True)
        gc.collect()
        return self

    def _dedupe_bigquery_table(self, table_name, asset_class):
        """
        Description
        -----------
        Query that dedupes BigQuery table stock_price_<interval> by timestamp, yahoo_ticker

        Parameters:
            table_name: str of the table name
            asset_class: str of the asset class to run queries on
        """

        print(f"\nDeduping BigQuery table {table_name}...\n") if self.verbose else None

        initial_syntax = f"CREATE OR REPLACE TABLE {self.schema}.{table_name} AS ("

        if asset_class == "stocks":
            query = f"""
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
                  stock_splits,
                  batch_timestamp
                FROM
                  {self.schema}.{table_name}
                QUALIFY row_number() over (PARTITION BY timestamp, yahoo_ticker ORDER BY timestamp DESC, batch_timestamp DESC) = 1
                ORDER BY
                  timestamp, yahoo_ticker, bloomberg_ticker, numerai_ticker
                );
            """

        elif asset_class == "forex":
            query = f"""
                {initial_syntax}
                SELECT
                  timestamp,
                  timestamp_tz_aware,
                  timezone,
                  yahoo_ticker,
                  bloomberg_ticker,
                  open,
                  high,
                  low,
                  close,
                  volume,
                  batch_timestamp
                FROM
                  {self.schema}.{table_name}
                QUALIFY row_number() over (PARTITION BY timestamp, yahoo_ticker ORDER BY timestamp DESC, batch_timestamp DESC) = 1
                ORDER BY
                  timestamp, yahoo_ticker
                );
            """

        elif asset_class == "crypto":
            query = f"""
                {initial_syntax}
                SELECT
                  timestamp,
                  timestamp_tz_aware,
                  timezone,
                  yahoo_ticker,
                  yahoo_name,
                  open,
                  high,
                  low,
                  close,
                  volume,
                  batch_timestamp
                FROM
                  {self.schema}.{table_name}
                QUALIFY row_number() over (PARTITION BY timestamp, yahoo_ticker ORDER BY timestamp DESC, batch_timestamp DESC) = 1
                ORDER BY
                  timestamp, yahoo_ticker, yahoo_name
                );
            """
        else:
            raise RuntimeError(
                "asset_class must be set to either 'stocks', 'forex', or 'crypto'"
            )

        self.bigquery_client.run_sql(query)

        if "bigquery" in self.create_timestamp_index_dbs:
            raise NotImplementedError(
                "No need to create timestamp indices on bigquery."
            )
        return self

    def _drop_mysql_index_constraint(self, table_name):
        """
        Description
        -----------
        Query that drops MySQL index constraint on table stock_price_<interval>.

        Parameters:
            table_name: str of the table name that was pulled from yfinance (e.g. 'stock_prices_1m')
        """
        idx_cols = self.mysql_client.run_sql(
            f"""
                SELECT
                  *
                FROM
                  information_schema.statistics
                WHERE
                  table_schema = '{self.schema}'
                  AND table_name = '{table_name}'
            """
        )
        if "dedupe" in idx_cols["INDEX_NAME"].tolist():
            self.mysql_client.run_sql(
                f"ALTER TABLE {self.schema}.{table_name} DROP INDEX dedupe;"
            )
        return


class TickerDownloader:
    """
    Description
    -----------
    Class to download PyTickerSymbols, Yahoo, and Numerai ticker symbols into a single dataframe.
    A mapping between all symbols is returned when calling the method download_valid_stock_tickers().
    """

    def __init__(self):
        pass

    @staticmethod
    def download_pts_stock_tickers():
        """
        Description
        -----------
        Download py-ticker-symbols tickers
        """
        pts = PyTickerSymbols()
        all_getters = list(
            filter(
                lambda x: (
                    x.endswith("_yahoo_tickers") or x.endswith("_google_tickers")
                ),
                dir(pts),
            )
        )

        all_tickers = {"yahoo_tickers": [], "google_tickers": []}
        for t in all_getters:
            if t.endswith("google_tickers"):
                all_tickers["google_tickers"].append((getattr(pts, t)()))
            elif t.endswith("yahoo_tickers"):
                all_tickers["yahoo_tickers"].append((getattr(pts, t)()))
        all_tickers["google_tickers"] = flatten_list(all_tickers["google_tickers"])
        all_tickers["yahoo_tickers"] = flatten_list(all_tickers["yahoo_tickers"])
        if len(all_tickers["yahoo_tickers"]) == len(all_tickers["google_tickers"]):
            all_tickers = pd.DataFrame(all_tickers)
        else:
            all_tickers = pd.DataFrame(
                dict([(k, pd.Series(v)) for k, v in all_tickers.items()])
            )

        all_tickers = (
            all_tickers.rename(
                columns={
                    "yahoo_tickers": "yahoo_ticker",
                    "google_tickers": "google_ticker",
                }
            )
            .sort_values(by=["yahoo_ticker", "google_ticker"])
            .drop_duplicates()
        )
        return all_tickers

    @staticmethod
    def download_top_250_crypto_tickers(num_currencies=250):
        """
        Description
        -----------
        Download the top 250 cryptocurrencies
        Note: At the time of coding, setting num_currencies higher than 250 results in only 25 crypto tickers returned.
        """

        from requests_html import HTMLSession

        session = HTMLSession()
        resp = session.get(
            f"https://finance.yahoo.com/crypto?offset=0&count={num_currencies}"
        )
        tables = pd.read_html(resp.html.raw_html)
        session.close()

        df = clean_columns(tables[0].copy())
        df.rename(
            columns={
                "symbol": "yahoo_ticker",
                "name": "yahoo_name",
                "%_change": "pct_change",
            },
            inplace=True,
        )

        # Add Decentral-Games tickers

        missing_dg_tickers = [
            i
            for i in ["ICE13133-USD", "DG15478-USD", "XDG-USD"]
            if i not in df["yahoo_ticker"]
        ]
        if len(missing_dg_tickers):
            df_dg = pd.DataFrame(
                {
                    "yahoo_ticker": missing_dg_tickers,
                    "yahoo_name": missing_dg_tickers,
                    "price_intraday": np.nan,
                    "change": np.nan,
                    "pct_change": np.nan,
                    "market_cap": np.nan,
                    "volume_in_currency_since_0_00_utc": np.nan,
                    "volume_in_currency_24_hr": np.nan,
                    "total_volume_all_currencies_24_hr": np.nan,
                    "circulating_supply": np.nan,
                    "52_week_range": np.nan,
                    "day_chart": np.nan,
                }
            )

            df = pd.concat([df, df_dg], axis=0).reset_index(drop=True)

        df = df.dropna(how="all", axis=1)
        return df

    @staticmethod
    def download_forex_pairs():
        forex_pairs = dict(
            yahoo_ticker=[
                "EURUSD=X",
                "JPY=X",
                "GBPUSD=X",
                "AUDUSD=X",
                "NZDUSD=X",
                "EURJPY=X",
                "GBPJPY=X",
                "EURGBP=X",
                "EURCAD=X",
                "EURSEK=X",
                "EURCHF=X",
                "EURHUF=X",
                "CNY=X",
                "HKD=X",
                "SGD=X",
                "INR=X",
                "MXN=X",
                "PHP=X",
                "IDR=X",
                "THB=X",
                "MYR=X",
                "ZAR=X",
                "RUB=X",
            ],
            yahoo_name=[
                "USD/EUR",
                "USD/JPY",
                "USD/GBP",
                "USD/AUD",
                "USD/NZD",
                "EUR/JPY",
                "GBP/JPY",
                "EUR/GBP",
                "EUR/CAD",
                "EUR/SEK",
                "EUR/CHF",
                "EUR/HUF",
                "USD/CNY",
                "USD/HKD",
                "USD/SGD",
                "USD/INR",
                "USD/MXN",
                "USD/PHP",
                "USD/IDR",
                "USD/THB",
                "USD/MYR",
                "USD/ZAR",
                "USD/RUB",
            ],
        )
        # TODO
        # Difficulty downloading forex pairs so just returning the forex_pairs input for now.
        df_forex_pairs = pd.DataFrame(forex_pairs)
        df_forex_pairs.loc[:, "bloomberg_ticker"] = df_forex_pairs["yahoo_name"].apply(
            lambda x: f"{x[4:]}-{x[0:3]}"
        )

        return df_forex_pairs

    @staticmethod
    def download_numerai_signals_ticker_map(
        napi=SignalsAPI(),
        numerai_ticker_link="https://numerai-signals-public-data.s3-us-west-2.amazonaws.com/signals_ticker_map_w_bbg.csv",
        yahoo_ticker_colname="yahoo",
        verbose=True,
    ):
        """
        Description
        -----------
        Download numerai to yahoo ticker mapping
        """

        ticker_map = pd.read_csv(numerai_ticker_link)
        eligible_tickers = pd.Series(napi.ticker_universe(), name="bloomberg_ticker")
        ticker_map = pd.merge(
            ticker_map, eligible_tickers, on="bloomberg_ticker", how="right"
        )

        (
            print(f"Number of eligible tickers in map: {len(ticker_map)}")
            if verbose
            else None
        )

        # Remove null / empty tickers from the yahoo tickers
        valid_tickers = [
            i
            for i in ticker_map[yahoo_ticker_colname]
            if not pd.isnull(i)
            and not str(i).lower() == "nan"
            and not str(i).lower() == "null"
            and i is not None
            and not str(i).lower() == ""
            and len(i) > 0
        ]
        print("tickers before cleaning:", ticker_map.shape) if verbose else None

        ticker_map = ticker_map[ticker_map[yahoo_ticker_colname].isin(valid_tickers)]

        print("tickers after cleaning:", ticker_map.shape) if verbose else None

        return ticker_map

    @classmethod
    def download_valid_stock_tickers(cls):
        """
        Description
        -----------
        Download the valid tickers from py-ticker-symbols
        """
        # napi = numerapi.SignalsAPI(os.environ.get('NUMERAI_PUBLIC_KEY'), os.environ.get('NUMERAI_PRIVATE_KEY'))

        df_pts_tickers = cls.download_pts_stock_tickers()

        numerai_yahoo_tickers = cls.download_numerai_signals_ticker_map().rename(
            columns={"yahoo": "yahoo_ticker", "ticker": "numerai_ticker"}
        )

        df1 = pd.merge(
            df_pts_tickers, numerai_yahoo_tickers, on="yahoo_ticker", how="left"
        ).set_index("yahoo_ticker")
        df2 = pd.merge(
            numerai_yahoo_tickers, df_pts_tickers, on="yahoo_ticker", how="left"
        ).set_index("yahoo_ticker")
        df3 = (
            pd.merge(
                df_pts_tickers,
                numerai_yahoo_tickers,
                left_on="yahoo_ticker",
                right_on="numerai_ticker",
                how="left",
            )
            .rename(
                columns={
                    "yahoo_ticker_x": "yahoo_ticker",
                    "yahoo_ticker_y": "yahoo_ticker_old",
                }
            )
            .set_index("yahoo_ticker")
        )
        df4 = (
            pd.merge(
                df_pts_tickers,
                numerai_yahoo_tickers,
                left_on="yahoo_ticker",
                right_on="bloomberg_ticker",
                how="left",
            )
            .rename(
                columns={
                    "yahoo_ticker_x": "yahoo_ticker",
                    "yahoo_ticker_y": "yahoo_ticker_old",
                }
            )
            .set_index("yahoo_ticker")
        )

        df_tickers_wide = clean_columns(pd.concat([df1, df2, df3, df4], axis=1))

        for col in df_tickers_wide.columns:
            suffix = col[-1]
            if suffix.isdigit():
                root_col = col.strip("_" + suffix)
                df_tickers_wide[root_col] = df_tickers_wide[root_col].fillna(
                    df_tickers_wide[col]
                )

        df_tickers = (
            df_tickers_wide.reset_index()[
                [
                    "yahoo_ticker",
                    "google_ticker",
                    "bloomberg_ticker",
                    "numerai_ticker",
                    "yahoo_ticker_old",
                ]
            ]
            .sort_values(
                by=[
                    "yahoo_ticker",
                    "google_ticker",
                    "bloomberg_ticker",
                    "numerai_ticker",
                    "yahoo_ticker_old",
                ]
            )
            .drop_duplicates()
        )

        df_tickers.loc[:, "yahoo_valid_pts"] = False
        df_tickers.loc[:, "yahoo_valid_numerai"] = False

        df_tickers.loc[
            df_tickers["yahoo_ticker"].isin(df_pts_tickers["yahoo_ticker"].tolist()),
            "yahoo_valid_pts",
        ] = True

        df_tickers.loc[
            df_tickers["yahoo_ticker"].isin(
                numerai_yahoo_tickers["numerai_ticker"].tolist()
            ),
            "yahoo_valid_numerai",
        ] = True

        return df_tickers


###### functions ######


def get_valid_yfinance_start_timestamp(interval, start="1950-01-01 00:00:00"):
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

    valid_intervals = [
        "1m",
        "2m",
        "5m",
        "15m",
        "30m",
        "60m",
        "1h",
        "90m",
        "1d",
        "5d",
        "1wk",
        "1mo",
        "3mo",
    ]
    assert interval in valid_intervals, f"must pass a valid interval {valid_intervals}"

    if interval == "1m":
        updated_start = max(
            (datetime.today() - timedelta(days=5)).date(), pd.to_datetime(start).date()
        )
    elif interval in ["2m", "5m", "15m", "30m", "90m"]:
        updated_start = max(
            (datetime.today() - timedelta(days=58)).date(), pd.to_datetime(start).date()
        )
    elif interval in ["60m", "1h"]:
        updated_start = max(
            (datetime.today() - timedelta(days=728)).date(),
            pd.to_datetime(start).date(),
        )
    else:
        updated_start = pd.to_datetime(start)

    updated_start = updated_start.strftime(
        "%Y-%m-%d"
    )  # yfinance doesn't like strftime with hours, minutes, or seconds

    return updated_start
