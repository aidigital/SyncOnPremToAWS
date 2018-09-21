import sqlalchemy
from pprint import pprint
import pandas as pd; pd.set_option('display.width', 1000)
import logging
import inspect
import numpy as np
from config import config

class OnPremise_Connecter(object):
    def __init__(self, server, database):
        # super().__init__()

        # ===== METHOD 1: SQL SERVER Authentication ===== -- WITH THIS METHOD,BULK INSERT doesn't work (so far...)
            # a) Had to enable login to SQL Server with the `sa` account
            # b) And create a System Data Source (DSN) (`custom_connection`) by typing `ODBC Data Sources` and going in the tab `System DSN` -> add -> localhost

        # self.engine = sqlalchemy.create_engine('mssql://ppt_reporting:ppt_reporting@DB03')  # DB03 was manually created in Windows
        # self.connection = self.engine.connect()

        user = config['On-Premise']['user']
        password = config['On-Premise']['password']
        port = config['On-Premise']['port']
        driver = 'ODBC+Driver+13+for+SQL+Server'

        connection_string = 'mssql+pyodbc://{user}:{password}@{server}:{port}/{db}?driver={driver}'.format(user=user, password=password, server=server, db=database, port=port, driver=driver)
        self.engine = sqlalchemy.create_engine(connection_string)
        self.connection = self.engine.connect()

        # ===== METHOD 2: Windows Authentication =====
            # a) I found the name `ODBC Driver 13 for SQL Server` also in `ODBC Data Sources` -> System DSN -> Add (and there are 4 choices)
            # b) Need to comment the line: self.table_column_names = connection.execute(sql).keys() --> AttributeError: 'pyodbc.Cursor' object has no attribute 'keys'

        # import pyodbc
        # self.connection = pyodbc.connect(r'Driver={ODBC Driver 13 for SQL Server};Server=' + server + ';Trusted_Connection=yes;')

        logging.info('Instantiated: {} for {} db'.format(__class__.__name__, database))

    def execute_sql(self, sql_statement):
        logger = logging.getLogger(__name__); logger.info('calling {}'.format(inspect.stack()[0][3]))
        result = self.connection.execute(sql_statement)
        #print(result)
        return result

    def fetch_results(self, sql_statement):
        logger = logging.getLogger(__name__); logger.info('calling {}'.format(inspect.stack()[0][3]))
        #connection = self.conn
        # self.table_column_names = connection.execute(sql_statement).keys()

        results = self.execute_sql(sql_statement)
        fetch_results = results.fetchall()

        df = pd.DataFrame(fetch_results)
        #connection.close()  # without this, it was failing to read the does_not_work after the table was dropped and re-created by Ruby script
        return df

    def fetch_to_pandas(self, sql_statement):
        #print(sql_statement)
        """ Shorter than `fetch_results()` (which didn't even work in this case)"""
        logger = logging.getLogger(__name__); logger.info('calling {}'.format(inspect.stack()[0][3]))
        df = pd.read_sql(sql_statement, self.connection)
        #df.columns = [str(_) for _ in range(0, len(df.columns))]  # instead of the actual column names, put: 0, 1, 2 ...
        #df.replace(to_replace=np.nan, value=0, inplace=True)
        #print(df.columns)

        #df = df.where(pd.notnull(df), None)  # fucking magic! Changes the dtype to object 'though'. Without this: cx_Oracle.DatabaseError: DPI-1055: value is not a number (NaN) and cannot be used in Oracle numbers
        #df.fillna(0)
        #print(df)
        # df.to_csv('C:/Users/adrian_iordache/Desktop/dataDump.csv')
        return df

    def get_columns_deprecated(self, table):
        logger = logging.getLogger(__name__); logger.info('calling {}'.format(inspect.stack()[0][3]))

        t = self.connection.execute('SELECT TOP(1) * FROM' + table)
        print(t.description)
        print('\n\n\n')
        columns = [column[0:2] for column in t.description]
        return columns

    def _get_columns(self, table):
        logger = logging.getLogger(__name__); logger.info('calling {}'.format(inspect.stack()[0][3]))
        #metadata = sqlalchemy.MetaData(self.engine)
        inspector = sqlalchemy.inspect(self.engine)
        x = [tuple(d.values()) for d in inspector.get_columns(table)]
        print(x)
        return x

    def _get_tables(self):
        logger = logging.getLogger(__name__); logger.info('calling {}'.format(inspect.stack()[0][3]))
        inspector = sqlalchemy.inspect(self.engine)
        print('TABLES:\n', inspector.get_table_names())
        print('\nVIEWS:\n', inspector.get_view_names())  # doesn't work, fix it!

if __name__ == "__main__":
    onPremise = OnPremise_Connecter(server='TVHA-UH-DB03', database='Fizzy')
    x = onPremise.fetch_to_pandas(sql_statement="""SELECT Count(*) from Area""")
    print(x)

    #print( onPremise.fetch_results(sql_statement='SELECT * FROM [Fizzy].[dbo].[Area]') )
    #print(onPremise.fetch_to_pandas(sql_statement='SELECT [Property Code], [Property Name] FROM [Fizzy].[dbo].[pexPropertyIndex]'))

    #print(onPremise.get_columns_deprecated("[Fizzy].[dbo].[pexPropertyIndex]"))
    #onPremise._get_columns('pexPropertyIndex')
    #onPremise._get_tables()
