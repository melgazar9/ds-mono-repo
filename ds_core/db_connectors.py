from ds_core.ds_utils import *
from sqlalchemy import create_engine, text
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas_gbq as pdg
from google.cloud import bigquery
from pymongo import MongoClient


class RDBMSConnect:

    def __init__(self):
        pass

    def connect(self):
        raise ValueError('method connect must be overridden.')

    def run_sql(self):
        raise ValueError('method run_sql must be overridden.')


rdbms_method_enforcer = MetaclassMethodEnforcer(required_methods=['connect', 'run_sql'], parent_class='RDBMSConnect')
MetaclassRDBMSEnforcer = rdbms_method_enforcer.enforce()


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

    def __init__(self,
                 google_application_credentials=os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'),
                 schema=os.environ.get('BIGQUERY_SCHEMA'),
                 job_config_params=None):
        self.google_application_credentials = google_application_credentials
        self.schema = schema
        self.job_config_params = {} if job_config_params is None else job_config_params

        self.dwh_name = 'bigquery'

    def connect(self):
        self.client = bigquery.Client()
        self.job_config = bigquery.QueryJobConfig(**self.job_config_params)
        return self

    def run_sql(self, query, return_result=True, use_pd_gbq=True):
        self.connect()
        if query.strip().lower().startswith('select') and use_pd_gbq:
            df = pdg.read_gbq(query)
            return df
        else:
            job = self.client.query(query, job_config=self.job_config)
            if return_result:
                return job.result()
        return


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

    def __init__(self,
                 user=os.environ.get('MYSQL_USER'),
                 password=os.environ.get('MYSQL_PASSWORD'),
                 host=os.environ.get('MYSQL_HOST'),
                 schema=os.environ.get('MYSQL_SCHEMA'),
                 charset='utf8',
                 backend_url='mysqldb',
                 string_extension='mb4&binary_prefix=true',
                 engine_string=None,
                 keep_session_alive=False):

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
        charset: MySQL charset
        backend_url: backend to use (e.g. mysqldb)
        string_extension: string to append the MySQL engine_string
        engine_string: str of the full extension URL. If this is provided all other parameters are ignored.
        keep_session_alive: bool to keep session open after query executes
        """

        self.user = os.environ.get('MYSQL_USER') if user is None else user
        self.password = os.environ.get('MYSQL_PASSWORD') if password is None else password
        self.host = os.environ.get('MYSQL_HOST') if host is None else host
        self.schema = schema
        self.charset = charset
        self.backend_url = backend_url
        self.string_extension = string_extension
        self.engine_string = engine_string
        self.keep_session_alive = keep_session_alive

        self.dwh_name = 'mysql'

    def connect(self, **kwargs):
        if self.engine_string is not None:
            self.engine_string = self.engine_string
        else:
            self.engine_string = "mysql://"

            if self.backend_url is not None:
                self.engine_string = self.engine_string.replace('mysql://', f'mysql+{self.backend_url}://')
            if self.user is not None:
                self.engine_string = self.engine_string + self.user
            if self.password is not None:
                self.engine_string = self.engine_string + f':{self.password}'
            if self.host is not None:
                self.engine_string = self.engine_string + f'@{self.host}'
            if self.schema is not None and self.schema != '':
                self.engine_string = self.engine_string + f'/{self.schema}'
            if self.charset is not None:
                self.engine_string = self.engine_string + f'?charset={self.charset}'
            if self.string_extension is not None:
                self.engine_string = self.engine_string + self.string_extension

        engine = create_engine(self.engine_string, **kwargs)
        self.con = engine.connect()

        return self

    def run_sql(self, query, **read_sql_kwargs):
        self.connect()
        if query.strip().lower().startswith('select'):
            df = pd.read_sql(query, con=self.con, **read_sql_kwargs)
            if not self.keep_session_alive:
                self.con.close()
            return df
        else:
            query = text(query)
            self.con.execute(query)
            if not self.keep_session_alive:
                self.con.close()
        return self


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

    def __init__(self,
                 user=os.environ.get('SNOWFLAKE_USER'),
                 password=os.environ.get('SNOWFLAKE_PASSWORD'),
                 database=os.environ.get('SNOWFLAKE_DATABASE'),
                 schema=os.environ.get('SNOWFLAKE_SCHEMA'),
                 warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'),
                 account=os.environ.get('SNOWFLAKE_ACCOUNT'),
                 role=os.environ.get('SNOWFLAKE_ROLE'),
                 backend_engine='sqlalchemy',
                 engine_string=None,
                 connect_args=None):

        self.user = os.environ.get('SNOWFLAKE_USER') if user is None else user
        self.password = os.environ.get('SNOWFLAKE_PASSWORD') if password is None else password
        self.account = os.environ.get('SNOWFLAKE_ACCOUNT') if account is None else account
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.role = role
        self.backend_engine = backend_engine
        self.engine_string = engine_string
        self.dwh_name = 'snowflake'
        self.connect_args = connect_args

    def connect(self):
        snowflake_connection_params = {
            'user': self.user,
            'password': self.password,
            'account': self.account,
            'database': self.database,
            'schema': self.schema,
            'warehouse': self.warehouse,
            'role': self.role
        }

        if self.connect_args:
            snowflake_connection_params.update(self.connect_args)
        else:
            self.connect_args = {}

        if self.backend_engine == 'sqlalchemy':
            if self.engine_string is not None:
                self.engine_string = self.engine_string
            else:
                self.engine_string = "snowflake://"

                if self.user is not None:
                    self.engine_string = self.engine_string + self.user
                if self.password is not None:
                    self.engine_string = self.engine_string + f':{self.password}'
                if self.account is not None:
                    self.engine_string = self.engine_string + f'@{self.account}'
                if self.database is not None and self.database != '':
                    self.engine_string = self.engine_string + f'/{self.database}'
                if self.schema is not None and self.schema != '':
                    self.engine_string = self.engine_string + f'/{self.schema}'
                if self.warehouse is not None:
                    self.engine_string = self.engine_string + f'?warehouse={self.warehouse}'
                if self.role is not None:
                    self.engine_string = self.engine_string = self.engine_string + f'?role={self.role}'

            engine = create_engine(self.engine_string, connect_args=self.connect_args)
            self.con = engine.connect()
        else:
            self.con = snowflake.connector.connect(**snowflake_connection_params)
        return self

    def run_sql(self, query, **read_sql_kwargs):
        self.connect()
        if self.backend_engine == 'sqlalchemy':
            if query.strip().lower().startswith('select') or query.strip().lower().startswith('with'):
                df = pd.read_sql(query, con=self.con, **read_sql_kwargs)
                return df
            else:
                query = text(query)
                self.con.execute(query)
        else:
            cur = self.con.cursor()
            cur.execute(query)
            if query.strip().lower().startswith('select') or query.strip().lower().startswith('with'):
                df = cur.fetch_pandas_all()
                return df
        self.con.close()
        return self


###### NoSQL Connectors ######

class NoSQLConnect:

    def __init__(self):
        pass

    def connect(self):
        raise ValueError('method connect must be overridden.')

    def find(self):
        raise ValueError('method run_sql must be overridden.')


nosql_method_enforcer = MetaclassMethodEnforcer(required_methods=['connect', 'find'], parent_class='NoSQLConnect')
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

    def __init__(self,
                 env='production',
                 database=None,
                 collection=None,
                 mongodb_connection_string=None,
                 keep_session_alive=False):
        self.env = env
        self.database = database
        self.collection = collection
        self.keep_session_alive = keep_session_alive

        if mongodb_connection_string is None:
            if self.env.lower().startswith('dev'):
                self.mongodb_connection_string = os.environ.get("MONGODB_DEV_STRING")
            elif self.env.lower().startswith('stag'):
                self.mongodb_connection_string = os.environ.get("MONGODB_STAGING_STRING")
            elif self.env.lower().startswith('test'):
                self.mongodb_connection_string = os.environ.get("MONGODB_TESTING_STRING")
            elif self.env.lower().startswith('prod'):
                self.mongodb_connection_string = os.environ.get("MONGODB_PRODUCTION_STRING")
        else:
            self.mongodb_connection_string = mongodb_connection_string

        self.dwh_name = 'mongodb'
        self.data = None

    def connect(self):
        self.client = MongoClient(self.mongodb_connection_string)
        self.client.connect()
        return self

    def find(self, *args):
        assert self.database is not None and self.collection is not None, \
            'Init params for database and collection cannot be None!'

        self.connect()

        db = self.client[self.database]
        _collection = db[self.collection]

        self.data = [i for i in _collection.find(*args)]

        if not self.keep_session_alive:
            self.client.close()

        return self
