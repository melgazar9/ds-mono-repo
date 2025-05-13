# flake8: noqa

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError

from ds_core.ds_utils import *  # noqa: F403

# from pymongo import MongoClient  # remove requirement for now
# import snowflake.connector  # remove requirement for now
# from snowflake.connector.pandas_tools import write_pandas, pd_writer  # remove requirement for now


class RDBMSConnect:
    def __init__(self):
        pass

    def connect(self):
        raise ValueError("method connect must be overridden.")

    def run_sql(self):
        raise ValueError("method run_sql must be overridden.")


rdbms_method_enforcer = MetaclassMethodEnforcer(
    required_methods=["connect", "run_sql"], parent_class="RDBMSConnect"
)
MetaclassRDBMSEnforcer = rdbms_method_enforcer.enforce()


class MySQLConnect(metaclass=MetaclassRDBMSEnforcer):
    """
    Description
    -----------
    Connect to MySQL database and run sql queries.
    Default settings uses mysql-client as the backend because it has shown to be the fastest amongst the MySQL python libraries.
    Must have methods named "connect" and "run_sql"

    Example
    -------
    db = MySQLConnect()
    df = db.run_sql('select * from <my_table> limit 1;')
    """

    def __init__(
        self,
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        host=os.getenv("MYSQL_HOST"),
        schema=os.getenv("MYSQL_SCHEMA"),
        drivername="mysql",
        engine_string=None,
        keep_session_alive=False,
    ):
        """
        Description
        -----------
        Connection to MySQL db via python sqlalchemy package.
        If engine_string is provided all other parameters are ignored.
        Must have methods named "connect" and "run_sql"

        Parameters
        ----------
        user: MySQL username
        password: MySQL password
        host: MySQL host
        drivername: backend to use (e.g. mysqldb)
        engine_string: str of the full extension URL. If this is provided all other parameters are ignored.
        keep_session_alive: bool to keep session open after query executes
        """

        self.user = os.getenv("MYSQL_USER") if user is None else user
        self.password = os.getenv("MYSQL_PASSWORD") if password is None else password
        self.host = os.getenv("MYSQL_HOST") if host is None else host
        self.schema = schema
        self.drivername = drivername
        self.engine_string = engine_string
        self.keep_session_alive = keep_session_alive

        self.dwh_name = "mysql"
        self.engine = None
        self.con = None

    def connect(self, **kwargs):
        if self.engine_string is not None:
            self.engine_string = self.engine_string
        else:
            mysql_connection_params = {
                "drivername": self.drivername,
                "username": self.user,
                "password": self.password,
                "host": self.host,
                "query": {},
            }

            self.engine_string = URL(**mysql_connection_params)

        self.engine = create_engine(self.engine_string, **kwargs)
        self.con = self.engine.connect()

        return self

    def disconnect(self):
        self.con.close()
        self.engine.dispose()
        return self

    def run_sql(self, query, df_type="pandas", **read_sql_kwargs):
        self.connect()
        if df_type == "pandas":
            if query.strip().lower().startswith("select"):
                df = pd.read_sql(query, con=self.con, **read_sql_kwargs)
            elif df_type == "polars":
                df = pl.read_database(query=query, connection=self.con, **read_sql_kwargs)
            else:
                raise ValueError(
                    f"df_type is set to {df_type} but only pandas and polars are supported."
                )

            if not self.keep_session_alive:
                self.disconnect()
            return df
        else:
            query = text(query)
            self.con.execute(query)
            if not self.keep_session_alive:
                self.disconnect()
        return self


class PostgresConnect(metaclass=MetaclassRDBMSEnforcer):
    """
    Description
    -----------
    Connect to PostgreSQL database and run sql queries.

    NOTE - When you use an object in a with statement, Python looks for these two methods:
        __enter__: called when the with block is entered. When running "with PostgresConnect(...) as db:" the "db" value refers to the connected PostgresConnect instance.
        __exit__: called when the with block is exited.

    USAGE
    -----
    This class must be called within a "with" clause to prevent open connection db leakage.

    Example
    -------
    db = PostgresConnect()
    df = db.run_sql('SELECT * FROM my_table LIMIT 10')
    """

    def __init__(
        self,
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT", 5432),
        database=os.getenv("POSTGRES_DB"),
        drivername="postgresql",
        engine_string=None,
        keep_session_alive=False,  # less relevant with context manager, but kept for compatibility with non-context usage.
    ):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.drivername = drivername
        self.engine_string = engine_string
        self.keep_session_alive = keep_session_alive

        self.db_name = "postgres"
        self.engine = None
        self.con = None

    def connect(self, **kwargs):
        """Establishes a connection, re-using if active."""
        if self.con is not None and not self.con.closed:
            return self  # Connection is already open and usable

        if self.engine_string is None:
            connection_params = {
                "drivername": self.drivername,
                "username": self.user,
                "password": self.password,
                "host": self.host,
                "port": self.port,
                "database": self.database,
                "query": {},
            }
            self.engine_string = URL(**connection_params)

        if self.engine is None or not self.engine.pool.initialized():
            self.engine = create_engine(self.engine_string, **kwargs)

        try:
            self.con = self.engine.connect()
        except SQLAlchemyError as e:
            print(f"Database connection failed: {e}")
            self.engine = None
            raise

        return self

    def disconnect(self):
        """Closes the connection and disposes the engine."""
        if self.con is not None and not self.con.closed:
            try:
                self.con.close()
            except SQLAlchemyError as e:
                print(f"Error closing connection: {e}")
            finally:
                self.con = None

        if self.engine is not None:
            try:
                self.engine.dispose()
            except SQLAlchemyError as e:
                print(f"Error disposing engine: {e}")
            finally:
                self.engine = None

        return self

    def run_sql(self, query, df_type="pandas", **read_sql_kwargs):
        """
        Runs a SQL query. Use as a context manager for batched reads.
        Params
        ------
        query: str of postgres sql query
        df_type: if returning a df then it will return a df of type df_type (currently only supports pandas and polars).
        **read_sql_kwargs: kwargs passed to pd.read_sql or pl.read_database.
        """
        is_batched_read = df_type == "polars" and read_sql_kwargs.get(
            "iter_batches", False
        )

        if is_batched_read and (self.con is None or self.con.closed):
            # For batched reads, connection MUST be managed by context manager
            # or manually kept alive. Raise error if connection is not active.
            raise RuntimeError(
                "For batched reads (iter_batches=True), the PostgresConnect instance must be used within a 'with' statement or connect() must be called and managed manually."
            )

        if not is_batched_read:
            self.connect()

        if df_type in ["pandas", "polars"]:
            if df_type == "pandas":
                try:
                    df = pd.read_sql(query, con=self.con, **read_sql_kwargs)
                finally:
                    if not self.keep_session_alive and not is_batched_read:
                        self.disconnect()
                return df
            elif df_type == "polars":
                try:
                    df = pl.read_database(
                        query=query, connection=self.con, **read_sql_kwargs
                    )
                finally:
                    if not self.keep_session_alive and not is_batched_read:
                        self.disconnect()
                return df
            else:
                if not self.keep_session_alive and not is_batched_read:
                    self.disconnect()
                raise ValueError(
                    f"df_type is set to {df_type} but only pandas and polars are supported."
                )
        else:
            try:
                query_obj = text(query)
                self.con.execute(query_obj)
            except SQLAlchemyError as e:
                print(f"Error executing query: {e}")
                raise
            finally:
                if not self.keep_session_alive and not is_batched_read:
                    self.disconnect()
            return self

    def __enter__(self):
        """Enter the context, ensuring a connection is open."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context, ensuring the connection is closed."""
        self.disconnect()
        return False


### Cloud DWH Connectors ###


class BigQueryConnect(metaclass=MetaclassRDBMSEnforcer):
    """
    Description
    -----------
    Connect to Google BigQuery database and run queries.
    Must have methods named "connect" and "run_sql"

    Example
    -------
    db = BigQueryConnect()
    df = db.run_sql('select * from <my_table> limit 1;')

    Prerequisites
    -------------
    Log into Google BigQuery and create a service account for a specific project here:
        https://console.cloud.google.com/iam-admin/serviceaccounts/
        Make sure the proper permissions are granted! (e.g. grant both read and write permissions!)
            roles/storage.objectCreator or storage admin works
    Once the credentials json file is downloaded, copy it to a safe location (e.g. ~/.credentials/)
    Open ~/.bashrc (or ~/.zshrc) and add the following line
        export GOOGLE_APPLICATION_CREDENTIALS=<path to credentials json file>
    Run source ~/.bashrc (or source ~/.zshrc); source venv/bin/activate;
    When running pandas gbq it will probably have you log into your Google account via web browser
    At this point you should be connected to Google BigQuery!

    Parameters
    ----------
    google_application_credentials: str of path where json credentials are stored from creating a service account
    database: str of the database or schema name
    job_config_params: dict of params passed to bigquery.QueryJobConfig.
    """

    def __init__(
        self,
        google_application_credentials=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        schema=os.getenv("BIGQUERY_SCHEMA"),
        job_config_params=None,
    ):
        self.google_application_credentials = google_application_credentials
        self.schema = schema
        self.job_config_params = {} if job_config_params is None else job_config_params

        self.dwh_name = "bigquery"

    def connect(self):
        self.client = bigquery.Client()
        self.job_config = bigquery.QueryJobConfig(**self.job_config_params)
        return self

    def disconnect(self):
        self.client.close()
        return self

    def run_sql(self, query, return_result=True, use_pd_gbq=True):
        self.connect()
        if query.strip().lower().startswith("select") and use_pd_gbq:
            df = pdg.read_gbq(query)
            self.disconnect()
            return df
        else:
            job = self.client.query(query, job_config=self.job_config)
            if return_result:
                result = job.result()
                self.disconnect()
                return result
        return


class SnowflakeConnect(metaclass=MetaclassRDBMSEnforcer):
    """
    Description
    -----------
    Connect to snowflake database and run sql queries.
    Must have methods named "connect" and "run_sql"

    Example
    -------
    db = SnowflakeConnect()
    df = db.run_sql('select * from <my_table> limit 1;')
    """

    def __init__(
        self,
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        backend_engine="sqlalchemy",
        engine_string=None,
        connect_args=None,
    ):

        self.user = os.getenv("SNOWFLAKE_USER") if user is None else user
        self.password = os.getenv("SNOWFLAKE_PASSWORD") if password is None else password
        self.account = os.getenv("SNOWFLAKE_ACCOUNT") if account is None else account
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.role = role
        self.backend_engine = backend_engine
        self.engine_string = engine_string
        self.connect_args = connect_args

        self.dwh_name = "snowflake"
        self.con = None
        self.engine = None

    def connect(self):
        snowflake_connection_params = {
            "user": self.user,
            "password": self.password,
            "account": self.account,
            "database": self.database,
            "schema": self.schema,
            "warehouse": self.warehouse,
            "role": self.role,
        }

        snowflake_connection_params = {
            k: v for k, v in snowflake_connection_params.items() if v is not None
        }

        if self.connect_args:
            snowflake_connection_params.update(self.connect_args)
        else:
            self.connect_args = {}

        if self.backend_engine == "sqlalchemy":
            if self.engine_string is not None:
                self.engine_string = self.engine_string
            else:
                self.engine_string = sqlalchemy_snowflake_url(
                    **snowflake_connection_params
                )

            self.engine = create_engine(
                self.engine_string, connect_args=self.connect_args
            )
            self.con = self.engine.connect()
        else:
            self.con = snowflake.connector.connect(**snowflake_connection_params)

        return self

    def disconnect(self):
        self.con.close()
        self.engine.dispose()
        return self

    def run_sql(self, query, df_type="pandas", **read_sql_kwargs):
        self.connect()
        if self.backend_engine == "sqlalchemy":
            if query.strip().lower().startswith(
                "select"
            ) or query.strip().lower().startswith("with"):
                if df_type == "pandas":
                    df = pd.read_sql(query, con=self.con, **read_sql_kwargs)
                elif df_type == "polars":
                    df = pl.read_database(
                        query=query, connection=self.con, **read_sql_kwargs
                    )
                else:
                    raise ValueError(
                        f"df_type is set to {df_type} but only pandas and polars are supported."
                    )
                self.disconnect()
                return df
            else:
                query = text(query)
                self.con.execute(query)
                self.disconnect()
        else:
            cur = self.con.cursor()
            cur.execute(query)
            if query.strip().lower().startswith(
                "select"
            ) or query.strip().lower().startswith("with"):
                df = cur.fetch_pandas_all()
                self.disconnect()
                return df
        self.disconnect()
        return self


###### NoSQL Connectors ######


class NoSQLConnect:
    def __init__(self):
        pass

    def connect(self):
        raise ValueError("method connect must be overridden.")

    def disconnect(self):
        raise ValueError("method disconnect must be overridden.")

    def find(self):
        raise ValueError("method run_sql must be overridden.")


nosql_method_enforcer = MetaclassMethodEnforcer(
    required_methods=["connect", "find"], parent_class="NoSQLConnect"
)
MetaclassNoSQLEnforcer = nosql_method_enforcer.enforce()


class MongoDBConnect(metaclass=MetaclassNoSQLEnforcer):
    """
    Description
    -----------
    Connect to MongoDB database and pull data

    Example
    -------
    db = MongoDBConnect(env=<env>, database=<db>, collection=<collection>)
    df = db.find_one()
    """

    def __init__(
        self,
        env="production",
        database=None,
        collection=None,
        mongodb_connection_string=None,
        keep_session_alive=False,
    ):
        self.env = env
        self.database = database
        self.collection = collection
        self.keep_session_alive = keep_session_alive

        if mongodb_connection_string is None:
            if self.env.lower().startswith("dev"):
                self.mongodb_connection_string = os.getenv("MONGODB_DEV_STRING")
            elif self.env.lower().startswith("stag"):
                self.mongodb_connection_string = os.getenv("MONGODB_STAGING_STRING")
            elif self.env.lower().startswith("test"):
                self.mongodb_connection_string = os.getenv("MONGODB_TESTING_STRING")
            elif self.env.lower().startswith("prod"):
                self.mongodb_connection_string = os.getenv("MONGODB_PRODUCTION_STRING")
        else:
            self.mongodb_connection_string = mongodb_connection_string

        self.dwh_name = "mongodb"
        self.data = None
        self.client = None

    def connect(self):
        self.client = MongoClient(self.mongodb_connection_string)
        self.client.connect()
        return self

    def disconnect(self):
        self.client.close()
        return self

    def find(self, *args):
        assert (
            self.database is not None and self.collection is not None
        ), "Init params for database and collection cannot be None!"

        self.connect()

        db = self.client[self.database]
        _collection = db[self.collection]

        self.data = [i for i in _collection.find(*args)]

        if not self.keep_session_alive:
            self.disconnect()

        return self
