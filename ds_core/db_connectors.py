from ds_imports import *
import snowflake.connector
from pymongo import MongoClient

class SnowflakeConnect:

    """
    Description
    -----------
    Connect to snowflake database and run sql

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