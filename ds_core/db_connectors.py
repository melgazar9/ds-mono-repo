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
    df = SnowflakeConnect().run_sql('select * from <my_table> limit 10;')

    """

    def __init__(self,
                 database,
                 schema,
                 warehouse,
                 credentials_loc):

        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.credentials_loc = credentials_loc

    def connect(self):

        config = configparser.ConfigParser()
        config.read(self.credentials_loc)

        snowflake_connection_params = {
            'user': config['CREDENTIALS']['user'],
            'password': config['CREDENTIALS']['password'],
            'account': config['CREDENTIALS']['account'],
            'database': self.database,
            'schema': self.schema,
            'warehouse': self.warehouse
        }

        con = snowflake.connector.connect(**snowflake_connection_params)
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
                 env,
                 database,
                 collection,
                 credentials_yaml_file):
        self.env = env
        self.database = database
        self.collection = collection
        self.credentials_yaml_file = credentials_yaml_file

    def connect(self):
        connection_strings = yaml.safe_load(open(self.credentials_yaml_file, 'r'))
        client = MongoClient(connection_strings[self.env])
        return client

    def find(self, *args):

        assert self.database is not None and self.collection is not None, \
            'Init params for database and collection cannot be None!'

        client = self.connect()
        db = client[self.database]
        _collection = db[self.collection]
        output = [i for i in _collection.find(*args)]
        # if json_normalize_output:
        #     return json_normalize(output)

        return output
