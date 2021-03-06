import sqlalchemy
import logging
import inspect
import cx_Oracle
import pandas as pd; pd.set_option('display.width', 1000)
from pandas import DataFrame
from typing import Any, List, Tuple

from config import config

class OnPremise_Connecter2():
    """ This class connects both to SQL Server & Oracle, because it needs to read from both TVH & MTH On-Prem DBs"""
    def __init__(self, server, database, company='MTH Test') -> None:

        # Credentials
        self.server = server
        self.database = database
        self.driver = 'ODBC+Driver+13+for+SQL+Server'

        self.user = config['On-Premise']['user'] if company == 'TVH' else config['On-Premise-MTH']['user'] if company == 'MTH Test' else config['On-Premise-MTH-Live']['user']
        self.password = config['On-Premise']['password'] if company == 'TVH' else config['On-Premise-MTH']['password'] if company == 'MTH Test' else config['On-Premise-MTH-Live']['password']
        self.port = config['On-Premise']['port'] if company == 'TVH' else config['On-Premise-MTH']['port'] if company == 'MTH Test' else config['On-Premise-MTH-Live']['port']
        self.host = config['servers']['MET-PRD-VM-DB02'] if company == 'MTH Test' else config['servers']['MET-PRD-VM-DB01']

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
        elif company in ['MTH Test', 'MTH Live']:
            _connect_to_Oracle()
        else:
            print(f'you have provided company={company}. That is wrong, it needs to be one of these 3: "TVH", "MTH Test", "MTH Live"')

        logging.info(f'Instantiated: {__class__.__name__} for {database} db from {company}')


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
        #print(df)
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

    onPremise = OnPremise_Connecter2(server=config['servers']['MET-PRD-VM-DB01'], database=config['On-Premise-MTH-Live']['db'], company='MTH Live')
    #x = onPremise.fetch_to_pandas(sql_statement='SELECT count(*) from tvh_sa_communication_test')

    # VIEWS = ['semarchy_blocks', 'semarchy_local_authority', 'semarchy_lookup', 'semarchy_patch_lookup',
    #          'semarchy_scheme', 'semarchy_staff', 'semarchy_subblocks', 'semarchy_tenure_types', 'semarchy_units']
    #
    # for view in VIEWS:
    #      #result = onPremise.fetch_to_pandas(sql_statement = 'SELECT count(*) FROM semarchy_blocks FETCH FIRST 10 ROWS ONLY')
    #      # df: DataFrame = onPremise.fetch_to_pandas(sql_statement='SELECT count(*) FROM {}'.format(view))
    #      # print('{} -> {} {}'.format(view, df.iloc[0,0], df.columns.values))  # df.iloc[0,0] = value in column 1, row 1
    #      df: DataFrame = onPremise.fetch_to_pandas(sql_statement='SELECT * FROM {}'.format(view))
    #      print(f'{view}: {df.shape[0]} rows | {df.shape[1]} columns: {df.columns.values}\n')

    #df: DataFrame = onPremise.fetch_to_pandas(sql_statement='select name, created from v$database@HOUTEST')  # proof we can run from HOULIVE queries against HOUTEST db
    #df: DataFrame = onPremise.fetch_to_pandas(sql_statement='SELECT * FROM semarchy_residents')  # this also works: "select * from semarchy_residents@HOUTEST"
    #print(f' {df.shape[0]} rows | {df.shape[1]} columns: {df.columns.values}\n')
    #print(df)

    # trying to use the 'to_date()' function for date conversion
    verbose_script: str = """SELECT HOUSE_SIZE AS HOUSE_SIZE, 
                                              RESIDENTS_ID AS RESIDENTS_ID, 
                                              HOUSE_REF AS HOUSE_REF, 
                                              AGREEMENT_REF AS AGREEMENT_REF, 
                                              AGREEMENT_DESC AS AGREEMENT_DESC, 
                                              to_date(START_OF_TERM,'DD-MON-RRRR HH:MI:SS') AS START_OF_TERM, 
                                              to_date(END_OF_TERM,'DD-MON-RRRR HH:MI:SS') AS END_OF_TERM, 
                                              CURRENT_OCCUPANT AS CURRENT_OCCUPANT, 
                                              STOCK_GROUP AS STOCK_GROUP, 
                                              OCCUPANCY_TERMINATED AS OCCUPANCY_TERMINATED, 
                                              RENT_VALUE AS RENT_VALUE, 
                                              OCCUPANCY_STATUS AS OCCUPANCY_STATUS, 
                                              CURRENT_BALANCE AS CURRENT_BALANCE, 
                                              SCH_VALUE AS SCH_VALUE, 
                                              RESIDENT_TYPE AS RESIDENT_TYPE, 
                                              F_RENT_GRP_REF AS F_RENT_GRP_REF, 
                                              F_UNITS AS F_UNITS, 
                                              F_PROPERTY_TYPE AS F_PROPERTY_TYPE, 
                                              F_TENURE_TYPE AS F_TENURE_TYPE, 
                                              b_classname AS B_CLASSNAME, 
                                              to_date(B_CREDATE,'DD-MON-RRRR HH:MI:SS') AS B_CREDATE, 
                                              B_CREATOR AS B_CREATOR, 
                                              F_SOURCE_SYSTEM AS F_SOURCE_SYSTEM, 
                                              F_DATA_OWNERSHIP AS F_DATA_OWNERSHIP, 
                                              hash_value AS HASH_VALUE
                                          FROM semarchy_residents"""
    verbose_script_2: str = """select X.*
/*AA 12.02.2019
 Test DBs down and time is of the essence
 Sending SELECT to Adrian and Alice to use on HOULIVE
 If they approve then submit rfc to create it as a view and
 ask Adrian and Alice to update their scripts*/
, standard_hash(VALUE||COMMUNICATION_NAME||DEFAULT_COMMUNICATION||F_PERSON||F_COMMUNICATION_TYPE||OTHER_INFO||COMMUNICATION_ID,'SHA1') hash_value
from 
(
select cde_contact_value VALUE
,  cde_contact_name COMMUNICATION_NAME
,  case when cde_precedence='1' then '001'
        when cde_precedence>1 then '002'
        else '000' end DEFAULT_COMMUNICATION
,  par_refno F_PERSON
,  case when cde_frv_cme_code='TELEPHONE' then 'ZCM-MT1'
        when cde_frv_cme_code='EMAIL' then 'ZCM-UE1'
        when cde_frv_cme_code='WORK' then 'ZCM-WT1'
        when cde_frv_cme_code='HOMETEL' then 'ZCM-T'
        when cde_frv_cme_code='CONTACTTEL' then 'ZCM-T2'
        when cde_frv_cme_code='FAX, EBILL,FACEBOOK,TWITTER' then 'ZCM-O'
        when cde_frv_cme_code='INTERNAT' then 'ZCM-T3'
        when cde_frv_cme_code='LETTER' then 'ZCM-L' else 'ZCM-O' end F_COMMUNICATION_TYPE
, null OTHER_INFO        
, cde_refno COMMUNICATION_ID  /*this is the pk in the Northgate contact_details table*/
, 'Communication' B_CLASSNAME
, to_char(sysdate,'YYYY-MM-DD HH:MI:SS') B_CREDATE
, 'Northgate Integration' B_CREATOR
,  21 F_DATA_OWNERSHIP
,  21 F_SOURCE_SYSTEM
from contact_details
join parties on par_refno=cde_par_refno
where (
       exists (select null 
              from lease_parties /*Include ended lease parties if there is still a current lease assignment. Note to review with Amy*/
              join lease_assignments on las_lea_pro_refno=lpt_las_lea_pro_refno
              where sysdate between las_start_date and nvl(las_end_date,sysdate)
              and lpt_par_refno=par_refno)
   OR exists (select null
              from tenancy_instances /*Include ended tenants if there is still a current tenancy. Note to review with Amy*/
              join household_persons on hop_refno=tin_hop_refno
              join tenancies on tcy_refno=tin_tcy_refno
              where hop_par_refno=par_refno
              and sysdate between tcy_act_start_date and nvl(tcy_act_end_date,sysdate))
       )
) X
"""

    verbose_script_3: str = """select * from hou.wip_semarchy_communication """
    df: DataFrame = onPremise.fetch_to_pandas(sql_statement=verbose_script_3)
    print(f' {df.shape[0]} rows | {df.shape[1]} columns: {df.columns.values}\n')
    print(df.head)

    #print('successful connection to On-Prem')

    #print( onPremise.fetch_results(sql_statement='SELECT * FROM [Fizzy].[dbo].[Area]') )
    #print(onPremise.fetch_to_pandas(sql_statement='SELECT [Property Code], [Property Name] FROM [Fizzy].[dbo].[pexPropertyIndex]'))

    #print(onPremise.get_columns_deprecated("[Fizzy].[dbo].[pexPropertyIndex]"))
    #onPremise._get_columns('pexPropertyIndex')
    #onPremise._get_tables()
