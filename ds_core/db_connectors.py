from ds_core.ds_imports import *
import snowflake.connector
from pymongo import MongoClient

from sqlalchemy import create_engine, text

class MySQLConnect:

    """
    Description
    -----------
    Connect to MySQL database and run sql queries.
    Default settings uses mysql-client as the backend because it has shown to be the fastest amongst the MySQL python libraries.

    Example
    -------
    df = MySQLConnect().run_sql('select * from <my_table> limit 1;')
    """

    def __init__(self, database='', host='localhost', user='root', password='', charset='utf8', backend_url='mysqldb'):
        self.database = database
        self.host = os.environ.get('MYSQL_HOST') if host is None else host
        self.user = os.environ.get('MYSQL_USER') if user is None else user
        self.password = os.environ.get('MYSQL_PASSWORD') if password is None else password
        self.charset = charset
        self.backend_url = backend_url

    def connect(self, **kwargs):
        engine_string = f"mysql://{self.user}:{self.password}@{self.host}/{self.database}?charset={self.charset}mb4&binary_prefix=true"
        if self.backend_url is not None:
            engine_string = engine_string.replace('mysql://', f'mysql+{self.backend_url}://')
        engine = create_engine(engine_string, **kwargs)
        con = engine.connect()
        return con

    def run_sql(self, query, **read_sql_kwargs):
        con = self.connect()
        if query.strip().lower().startswith('select'):
            df = pd.read_sql(query, con=con, **read_sql_kwargs)
            con.close()
            return df
        else:
            query = text(query)
            con.execute(query)
            con.close()
        return


class SnowflakeConnect:

    """
    Description
    -----------
    Connect to snowflake database and run sql queries.

    Example
    -------
    df = SnowflakeConnect().run_sql('select * from <my_table> limit 1;')
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

        con = snowflake.connector.connect(**snowflake_connection_params)
        # del self.username, self.account, self.password
        return con

    def run_sql(self, query):
        con = self.connect()
        cur = con.cursor()
        cur.execute(query)
        df = cur.fetch_pandas_all()
        return df


class MongoDBConnect:

    """
    Description
    -----------
    Connect to MongoDB database and pull data

    Example
    -------
    # df = MongoDBConnect(env=<env>,
    #                     database=<db>,
    #                     collection=<collection>)\
    #     .find_one()
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
        client = MongoClient(self.mongodb_connection_string)
        # del self.mongodb_connection_string
        return client

    def find(self, *args):
        assert self.database is not None and self.collection is not None, \
            'Init params for database and collection cannot be None!'

        client = self.connect()
        db = client[self.database]
        _collection = db[self.collection]
        output = [i for i in _collection.find(*args)]
        return output