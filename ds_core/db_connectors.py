from ds_core.ds_utils import *
import snowflake.connector
from pymongo import MongoClient

from sqlalchemy import create_engine, text


class RDBMSConnect:

    def __init__(self):
        pass

    def connect(self):
        raise ValueError('method must be overridden.')

    def run_sql(self):
        raise ValueError('method must be overridden.')

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
        Make sure the proper permissions are granted (e.g. grant both read and write permissions)
    Once the credentials json file is downloaded, copy it to a safe location (e.g. ~/.credentials/)
    Open ~/.bashrc (or ~/.zshrc) and add the following line
        export GOOGLE_APPLICATION_CREDENTIALS=<path to credentials json file>
    Run source ~/.bashrc (or source ~/.zshrc); source venv/bin/activate;
    When running pandas gbq it will probably have you log into your Google account via web browser
    At this point you should be connected to Google BigQuery!
    """

    def __init__(self,
                 project_id,
                 user=os.environ.get('MYSQL_USER'),
                 password=os.environ.get('MYSQL_PASSWORD'),
                 host=os.environ.get('MYSQL_HOST'),
                 database=os.environ.get('MYSQL_DATABASE')):
        self.project_id = project_id
        self.user = user
        self.password = password
        self.host = host
        self.database = database

    def connect(self):
        pass

    def run_sql(self, query):
        if query.strip().lower().startswith('select'):
            df = pdg.read_gbq(query, project_id=self.project_id)
            return df
        else:
            pass


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
                 database=os.environ.get('MYSQL_DATABASE'),
                 charset='utf8',
                 backend_url='mysqldb',
                 string_extension='mb4&binary_prefix=true',
                 engine_string=None):

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
        """

        self.user = os.environ.get('MYSQL_USER') if user is None else user
        self.password = os.environ.get('MYSQL_PASSWORD') if password is None else password
        self.host = os.environ.get('MYSQL_HOST') if host is None else host
        self.database = database
        self.charset = charset
        self.backend_url = backend_url
        self.string_extension = string_extension
        self.engine_string = engine_string

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
            if self.database is not None and self.database != '':
                self.engine_string = self.engine_string + f'/{self.database}'
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
            self.con.close()
            return df
        else:
            query = text(query)
            self.con.execute(query)
            self.con.close()
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

    def __init__(self,
                 database,
                 schema,
                 warehouse,
                 username=None,
                 account=None,
                 password=None):

        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.username = os.environ.get('SNOWFLAKE_USERNAME') if username is None else username
        self.account = os.environ.get('SNOWFLAKE_ACCOUNT') if account is None else account
        self.password = os.environ.get('SNOWFLAKE_PASSWORD') if password is None else password

    def connect(self):
        snowflake_connection_params = {
            'user': self.username,
            'account': self.account,
            'password': self.password,
            'database': self.database,
            'schema': self.schema,
            'warehouse': self.warehouse
        }

        self.con = snowflake.connector.connect(**snowflake_connection_params)
        # del self.username, self.account, self.password
        return self

    def run_sql(self, query):
        self.connect()
        cur = self.con.cursor()
        cur.execute(query)

        if query.strip().lower().startswith('select'):
            df = cur.fetch_pandas_all()
            self.con.close()
            return df
        self.con.close()
        return


###### NoSQL Connectors ######

class NoSQLConnect:

    def __init__(self):
        pass

    def connect(self):
        raise ValueError('method must be overridden.')

    def find(self):
        raise ValueError('method must be overridden.')


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
                 mongodb_connection_string=None):
        self.env = env
        self.database = database
        self.collection = collection

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
        output = [i for i in _collection.find(*args)]
        self.client.close()
        return output