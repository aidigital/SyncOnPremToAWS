import sqlalchemy
import logging
import inspect
import cx_Oracle
import pandas as pd; pd.set_option('display.width', 1000)
from pandas import DataFrame
from typing import Any, List, Tuple

from config import config

class OnPremise_Connecter2():
    """ This class connects both to SQL Server & Oracle, because it needs from both MTH and MTH On-Prem DBs"""
    def __init__(self, server, database, company='MTH') -> None:

        # Credentials
        self.server = server
        self.database = database

        self.user = config['On-Premise']['user'] if company == 'TVH' else config['On-Premise-MTH']['user']
        self.password = config['On-Premise']['password'] if company == 'TVH' else config['On-Premise-MTH']['password']
        self.port = config['On-Premise']['port'] if company == 'TVH' else config['On-Premise-MTH']['port']

        self.driver = 'ODBC+Driver+13+for+SQL+Server'
        self.host = config['servers']['MET-PRD-VM-DB02']

        # Logic for connecting to SQL Server:
        def _connect_to_SQL_Server() -> None:
            connection_string = 'mssql+pyodbc://{user}:{password}@{server}:{port}/{db}?driver={driver}'.format(user=self.user, password=self.password, server=self.server, db=self.database, port=self.port, driver=self.driver)
            self.engine = sqlalchemy.create_engine(connection_string)
            self.connection = self.engine.connect()

        # Logic for connecting to Oracle:
        def _connect_to_Oracle() -> None:
            self.DSN = cx_Oracle.makedsn(self.host, self.port, service_name=self.database)  # dsn is an invalid keyword
            self.connection = cx_Oracle.connect(user=self.user, password=self.password, dsn=self.DSN)  # sid, tns -> invalid arguments

        if company == 'TVH':
            _connect_to_SQL_Server()
        elif company == 'MTH':
            _connect_to_Oracle()
        else:
            print("On-Prem did not try to connect to anything!")

        logging.info('Instantiated: {} for {} db from {}'.format(__class__.__name__, database, company))


    def execute_sql(self, sql_statement: str) -> Any:
        logger = logging.getLogger(__name__); logger.info('calling {}'.format(inspect.stack()[0][3]))
        result = self.connection.execute(sql_statement)
        #print(result)
        return result

    def fetch_results(self, sql_statement: str) -> DataFrame:
        logger = logging.getLogger(__name__); logger.info('calling {}'.format(inspect.stack()[0][3]))
        #connection = self.conn
        # self.table_column_names = connection.execute(sql_statement).keys()

        results = self.execute_sql(sql_statement)
        fetch_results = results.fetchall()

        df: DataFrame = pd.DataFrame(fetch_results)
        #connection.close()  # without this, it was failing to read the does_not_work after the table was dropped and re-created by Ruby script
        return df

    def fetch_to_pandas(self, sql_statement: str) -> DataFrame:
        #print(sql_statement)
        """ Shorter than `fetch_results()` (which didn't even work in this case)"""
        logger = logging.getLogger(__name__); logger.info('calling {}'.format(inspect.stack()[0][3]))
        df: DataFrame = pd.read_sql(sql_statement, self.connection)
        #df.columns = [str(_) for _ in range(0, len(df.columns))]  # instead of the actual column names, put: 0, 1, 2 ...
        #df.replace(to_replace=np.nan, value=0, inplace=True)
        #print(df.columns)

        #df = df.where(pd.notnull(df), None)  # fucking magic! Changes the dtype to object 'though'. Without this: cx_Oracle.DatabaseError: DPI-1055: value is not a number (NaN) and cannot be used in Oracle numbers
        #df.fillna(0)
        #print(df)
        # df.to_csv('C:/Users/adrian_iordache/Desktop/dataDump.csv')
        return df

    def get_columns_deprecated(self, table: str) -> List[str]:
        logger = logging.getLogger(__name__); logger.info('calling {}'.format(inspect.stack()[0][3]))

        t = self.connection.execute('SELECT TOP(1) * FROM' + table)
        print(t.description)
        print('\n\n\n')
        columns = [column[0:2] for column in t.description]
        return columns

    def _get_columns(self, table: str) -> List[Tuple]:
        logger = logging.getLogger(__name__); logger.info('calling {}'.format(inspect.stack()[0][3]))
        #metadata = sqlalchemy.MetaData(self.engine)
        inspector = sqlalchemy.inspect(self.engine)
        x = [tuple(d.values()) for d in inspector.get_columns(table)]
        print(x)
        return x

    def _get_tables(self) -> None:
        logger = logging.getLogger(__name__); logger.info('calling {}'.format(inspect.stack()[0][3]))
        inspector = sqlalchemy.inspect(self.engine)
        print('TABLES:\n', inspector.get_table_names())
        print('\nVIEWS:\n', inspector.get_view_names())  # doesn't work, fix it!

if __name__ == "__main__":
    # onPremise = OnPremise_Connecter(server=config['servers']['TVHA-UH-DB03'], database='clearview', company='TVH')
    # x = onPremise.fetch_to_pandas(sql_statement='SELECT count(*) from Elysium')

    onPremise = OnPremise_Connecter2(server=config['servers']['MET-PRD-VM-DB02'], database=config['On-Premise-MTH']['db'], company='MTH')
    #x = onPremise.fetch_to_pandas(sql_statement='SELECT count(*) from tvh_sa_communication_test')

    VIEWS = ['semarchy_blocks', 'semarchy_local_authority', 'semarchy_lookup', 'semarchy_patch_lookup', 'semarchy_scheme',
            'semarchy_staff', 'semarchy_subblocks', 'semarchy_tenure_types', 'semarchy_units']

    for view in VIEWS:
         #result = onPremise.fetch_to_pandas(sql_statement = 'SELECT count(*) FROM semarchy_blocks FETCH FIRST 10 ROWS ONLY')
         # df: DataFrame = onPremise.fetch_to_pandas(sql_statement='SELECT count(*) FROM {}'.format(view))
         # print('{} -> {} {}'.format(view, df.iloc[0,0], df.columns.values))  # df.iloc[0,0] = value in column 1, row 1
         df: DataFrame = onPremise.fetch_to_pandas(sql_statement='SELECT * FROM {}'.format(view))
         print(f'{view}: {df.shape[0]} rows | {df.shape[1]} columns: {df.columns.values}\n')


    #print('successful connection to On-Prem')

    #print( onPremise.fetch_results(sql_statement='SELECT * FROM [Fizzy].[dbo].[Area]') )
    #print(onPremise.fetch_to_pandas(sql_statement='SELECT [Property Code], [Property Name] FROM [Fizzy].[dbo].[pexPropertyIndex]'))

    #print(onPremise.get_columns_deprecated("[Fizzy].[dbo].[pexPropertyIndex]"))
    #onPremise._get_columns('pexPropertyIndex')
    #onPremise._get_tables()
