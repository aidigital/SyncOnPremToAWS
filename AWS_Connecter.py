import inspect
import logging
import cx_Oracle
import sqlalchemy
import pandas as pd; pd.set_option('display.width', 1000)
from pandas import DataFrame
from typing import Any, List, Tuple

from UDFs import all_next_words_after_word, modify_script
from config import config
from singleton import Singleton
from onPremise_Connecter2 import OnPremise_Connecter2

i = -1

class AWS_Connecter():
    def __init__(self, environment: str, host: str=None, user: str=None, password: str=None) -> None:
        """AWS_Connecter class calls OnPremise_Connecter2 class directly"""
        self.environment: str = environment  # this should be passed as one of 2 values: 'Dev' / 'Prod'
        self.machine_running_script: str = config['Identifier']['machine_running_script']  # this is to distinguish between running from on the AWS EC2, or the TVHA VN On-Prem

        # Dynamically determine the credentials based on environment
        self.host: str = host if host != None else config['AWS-Dev']['host'] if self.environment == 'Dev' else config['AWS-Prod']['host'] if self.environment == 'Prod' else 'idiot'
        self.user: str = user if user != None else config['AWS-Dev']['user'] if self.environment == 'Dev' else config['AWS-Prod']['user'] if self.environment == 'Prod' else 'idiot'
        self.password: str = password if password != None else config['AWS-Dev']['password'] if self.environment == 'Dev' else config['AWS-Prod']['password'] if self.environment == 'Prod' else 'idiot'

        self.port: int = config['AWS-Dev']['port'] if self.environment == 'Dev' else config['AWS-Prod']['port'] if self.environment == 'Prod' else 'idiot'
        self.sid: str = config['AWS-Dev']['sid'] if self.environment == 'Dev' else config['AWS-Prod']['sid'] if self.environment == 'Prod' else 'idiot'

        # the 2 functions MTA_GET_NEW_LOADID (which gives the LOAD_ID that we need to use) & MTA_SUBMIT_LOAD (which pushes SA to GD) are taken from here
        self.SemarchyFunctions_host: str = config['SemarchyFunctions-Dev']['host'] if self.environment == 'Dev' else config['SemarchyFunctions-Prod']['host'] if self.environment == 'Prod' else 'idiot'
        self.SemarchyFunctions_user: str = config['SemarchyFunctions-Dev']['user'] if self.environment == 'Dev' else config['SemarchyFunctions-Prod']['user'] if self.environment == 'Prod' else 'idiot'
        self.SemarchyFunctions_password: str = config['SemarchyFunctions-Dev']['password'] if self.environment == 'Dev' else config['SemarchyFunctions-Prod']['password'] if self.environment == 'Prod' else 'idiot'

        self.SID = cx_Oracle.makedsn(self.host, self.port, sid=self.sid)
        connection_string: str = 'oracle://{user}:{password}@{sid}'.format(user=self.user, password=self.password, sid=self.SID)
        #execution_options = {"timeout": 10000, "statement_timeout": 10000, "query_timeout": 10000, "execution_timeout": 10000}
        self.engine = sqlalchemy.create_engine(connection_string, convert_unicode=False, pool_recycle=1000, pool_size=1000, echo=False)  # pool_pre_ping=True

        # used for inserting
        self.connection = cx_Oracle.Connection("{}/{}@{}".format(self.user, self.password, self.SID))
        self.cursor = cx_Oracle.Cursor(self.connection)

        logging.info('Instantiated: {}'.format(__class__.__name__))

    def execute_sql(self, sql_statement: str) -> Any:
        logger = logging.getLogger(__name__); logger.info('calling {} with statement: {}'.format(inspect.stack()[0][3], sql_statement))
        result = self.engine.execute(sql_statement)
        print(result)
        return result

    def fetch_results(self, sql_statement: str) -> DataFrame:
        logger = logging.getLogger(__name__); logger.info('calling {}'.format(inspect.stack()[0][3]))
        results = self.execute_sql(sql_statement)
        fetch_results = results.fetchall()
        df: DataFrame = pd.DataFrame(fetch_results)
        return df

    def fetch_to_pandas(self, sql_statement: str) -> DataFrame:
        """ Shorter than `fetch_results()` """
        logger = logging.getLogger(__name__); logger.info('calling {}'.format(inspect.stack()[0][3]))
        df: DataFrame = pd.read_sql(sql_statement, self.engine)
        return df

    def create_table_manual_columns(self) -> None:
        logger = logging.getLogger(__name__); logger.info('calling {}'.format(inspect.stack()[0][3]))
        # Create a metadata instance
        metadata = sqlalchemy.MetaData(self.engine)
        #print(metadata.tables)  # immutabledict({})

        # Reflect db schema to MetaData
        #metadata.reflect(bind=self.engine)
        #pprint(metadata.tables)  # print all tables, all columns (super nice)

        # Register t1 to metadata
        t1 = sqlalchemy.Table('DUMMY_INSERT', metadata,
                                       sqlalchemy.Column('COL_1', sqlalchemy.String(100)),  #
                                       sqlalchemy.Column('COL_2', sqlalchemy.String(100)),
                                       #sqlalchemy.Column('Salary', sqlalchemy.Float, primary_key=True),
                                       #sqlalchemy.Column('Date', sqlalchemy.DateTime),
                                       #sqlalchemy.Column('xxx_n_var_char', sqlalchemy.NVARCHAR(length=255))
                                      #,sqlalchemy.Column('xxx_big_int', sqlalchemy.BIGINT)  # -> fail (no BIGINT in Oracle)
                            )

        metadata.create_all()

    @staticmethod
    def _change_type(x):
        if isinstance(x, sqlalchemy.BIGINT):
            return sqlalchemy.Numeric(19)

        elif isinstance(x, sqlalchemy.NVARCHAR) and x.length > 1000:
            return sqlalchemy.NVARCHAR(length=1000)

        else:
            return x

    def create_table_from_columns(self, table_name: str, columns_description: str) -> None:
        logger = logging.getLogger(__name__); logger.info('calling {}'.format(inspect.stack()[0][3]))
        metadata = sqlalchemy.MetaData(self.engine)

        _ = sqlalchemy.Table(table_name,
                              metadata,
                              *(sqlalchemy.Column(column_name,
                                                   self._change_type(column_type),
                                                   nullable=column_nullable,
                                                   default=column_default,
                                                   autoincrement=column_autoincrement
                                                   )

                                for column_name, column_type, column_nullable, column_default, column_autoincrement
                                in columns_description
                                )
            )

        metadata.create_all()  # can also use: _.create(self.engine)

    def create_table_from_name(self, server: str, db: str, table_name: str) -> None:
        logger = logging.getLogger(__name__); logger.info('calling {}'.format(inspect.stack()[0][3]))
        columns = OnPremise_Connecter2(server=server, database=db)._get_columns(table=table_name)
        self.create_table_from_columns(table_name='_' + table_name, columns_description=columns)

    @staticmethod
    def make_string(x: int) -> str:
        """ :param x: 5
            :return: :0, :1, :2, :3, :4
        """
        L = list(range(0, x))
        L = list(map(str, L))
        L = [':' + x for x in L]
        L = ', '.join(L)
        return L

    def insert_to_oracle(self, oracle_table: str, server: str, on_prem_database: str, sql_statement: str) -> None:
        """
        :param oracle_table: the oracle_table in which to insert the results
        :param server: on-prep server to connect to
        :param on_prem_database: on-prem db to connect to
        :param sql_statement: the sql code that retrieves the results

        !Important considerations regarding Column Names:
         -> extracted column names from the sql_statement are replaced by: 0, 1, 2, 3 ... etc
         -> the column names of the oracle_table are not used in the `INSERT INTO` statement (because it's easier to write the INSERT INTO statement w/o specifying them)

         => the sql_statement needs to retrieve columns in the same order in which they are in the oracle_table
         => oracle_table needs to have the same nr of columns as returned by the sql_statement
        """
        logger = logging.getLogger(__name__); logger.info('calling {}'.format(inspect.stack()[0][3]))

        insert_this: DataFrame = OnPremise_Connecter2(server=server, database=on_prem_database).fetch_to_pandas(sql_statement=sql_statement)
        nr_cols: int = len(insert_this.columns)
        insert_this = insert_this.to_dict('records')  # makes a list of dicts (one dict per row)

        # Full syntax would be: INSERT INTO <table> (<col1>, <col2>) VALUES (<val1>, <val2>)
        query: str = """INSERT INTO {table} VALUES ({columns})""".format(table=oracle_table, columns=self.make_string(nr_cols))
        # `make_string` fct will create something like: :0, :1, :2 ... -> those are the column names of `insert_this`

        print(f'THE INSERT QUERY for {oracle_table}: ', query)
        self.cursor.executemany(query, insert_this)
        self.connection.commit()

    @classmethod  # https://stackoverflow.com/questions/12179271/meaning-of-classmethod-and-staticmethod-for-beginner
    def instance_other_user(cls, environment, host, user, password) -> 'AWS_Connecter':
        logger = logging.getLogger(__name__); logger.info('calling {}'.format(inspect.stack()[0][3]))
        instance_other_user = cls(environment, host, user, password)  # essentially each instance of AWS_Connecter() will create yet another AWS_Connecter() instance by using this approach
        return instance_other_user

   # http://www.oracle.com/technetwork/articles/prez-stored-proc-084100.html
    def run_oracle_function(self, instance, fct_name, fct_params):
        run_fct = instance.cursor.callfunc(fct_name, cx_Oracle.NUMBER, fct_params)

        #logger = logging.getLogger(__name__)
        #logger.info(f'{fct_name} called with params = {fct_params}')
        return run_fct

    # this could be a normal func (or a @staticmethod)
    def _sql_query_to_get_Oracle_latest_HASH_VALUES(self, oracle_table: str, primary_key: str, col_to_increment: str) -> str:
        sql_script_text: str = """SELECT main.{PRIMARY_KEY}, main.{col_to_increment}, main.HASH_VALUE as LATEST_HASH_VALUE
               FROM
                    (SELECT {PRIMARY_KEY}
                           ,MAX({col_to_increment}) as {col_to_increment}
                           
                    FROM {oracle_table}
                    GROUP BY {PRIMARY_KEY}) grouped
                  
                   INNER JOIN {oracle_table} main
                   ON main.{PRIMARY_KEY} = grouped.{PRIMARY_KEY}
                   AND main.{col_to_increment} = grouped.{col_to_increment}""".format(oracle_table=oracle_table, PRIMARY_KEY=primary_key, col_to_increment=col_to_increment)
        return sql_script_text

    def insert_to_oracle_specify_columns(self, oracle_table: str, hierarchy: int, server: str, on_prem_database: str,
                                         sql_statement: str, col_to_increment: str, primary_key: str,
                                         company: str, delete_last: bool=False) -> None:
        # global i
        # i += 1
        # x = i  # this is very important
        """sql_statement needs to have the columns after the word `AS`, and they need to match the name of the columns in Oracle table!"""
        logger = logging.getLogger(__name__); logger.info('calling {}'.format(inspect.stack()[0][3]))

        # instance_new = self.instance_other_user(host='tvha-aws-semarchy-dev.csaymlyq76p1.eu-west-1.rds.amazonaws.com', user='SEMARCHY_REPOSITORY', password='SEMARCHY_REPOSITORY')
        instance_new: AWS_Connecter = self.instance_other_user(environment=self.environment, host=self.SemarchyFunctions_host, user=self.SemarchyFunctions_user, password=self.SemarchyFunctions_password)
        auto_increment = self.run_oracle_function(instance=instance_new, fct_name="MTA_GET_NEW_LOADID", fct_params=['TVHA_MDM', 0, 0, 'PYTHON', 'upload_data', 'adrian_iordache_'+self.machine_running_script])

        # Previous logic for calculating: auto_increment
        # try:
        #     auto_increment = self.execute_sql('SELECT MAX({col_to_increment}) FROM {oracle_table}'.format(col_to_increment=col_to_increment, oracle_table=oracle_table)).fetchall()[0][0]
        #     #print('auto_increment from db = ', auto_increment)
        #     if auto_increment is None:  # this happens when the table is empty
        #         #auto_increment = 1
        #         # Get the Max(B_LOADID) from the GD table
        #         auto_increment = self.execute_sql('SELECT MAX({col_to_increment}) FROM {GD_table}'.format(col_to_increment='B_BATCHID', GD_table='GD_'+oracle_table.split('_')[1])).fetchall()[0][0]
        # except:
        #     # auto_increment = 1
        #     # Get the Max(B_LOADID) from the GD table
        #     auto_increment = self.execute_sql('SELECT MAX({col_to_increment}) FROM {GD_table}'.format(col_to_increment='B_BATCHID', GD_table='GD_'+oracle_table.split('_')[1])).fetchall()[0][0]

        #auto_increment += x

        # DELETE Rows having the max load
        if delete_last:
            self.execute_sql(sql_statement='DELETE FROM {oracle_table} WHERE {col_to_increment} = {auto_increment}'.format(oracle_table=oracle_table, col_to_increment=col_to_increment, auto_increment=auto_increment))

        # Modify the provided sql_statement (include the value that needs to go in `col_to_increment`)
        modification: str = " {} AS {} ".format(auto_increment if delete_last else auto_increment+0, col_to_increment) #; print('modification =', modification)

        #sql_statement = sql_statement.split()[0] + modification + ' '.join(sql_statement.split()[1:])
        sql_statement: str = modify_script(old_script=sql_statement, modification=modification)
        #logger.info('The SQL statement used to update {} from {} db is \n{}'.format(oracle_table, on_prem_database, sql_statement))

        # Extract the data that you want to insert from the On-Premise database
        insert_this: DataFrame = OnPremise_Connecter2(server=server, database=on_prem_database, company=company).fetch_to_pandas(sql_statement=sql_statement)
        nr_rows_retrieved: int = len(insert_this)
        nr_cols: int = len(insert_this.columns)
        insert_this_original_columns: str = ', '.join(insert_this.columns)  # ; print('insert_this_original_columns = ', insert_this_original_columns)

        # Make a modification for testing purposes! Delete afterwards this code
        # if oracle_table == 'SA_UNITS' and on_prem_database == 'Fizzy':
        #     for x in ['26040', '26041', '26042', '26043']:
        #         insert_this.loc[insert_this.UNITS_ID == x, 'HASH_VALUE'] = 999999999
        #     print('fake change got executed')


        # Get the latest HASH_VALUE in ORACLE for each Primary Key (SCHEMES_ID, BLOCK_ID etc depending on the table)
        get_oracle_latest_HASH_VALUE: str = self._sql_query_to_get_Oracle_latest_HASH_VALUES(oracle_table=oracle_table, primary_key=primary_key, col_to_increment=col_to_increment)  # creates the SQL code
        #print(f'get_oracle_latest_HASH_VALUE = {get_oracle_latest_HASH_VALUE}')

        oracle_latest_HASH_VALUE: DataFrame = self.fetch_to_pandas(get_oracle_latest_HASH_VALUE)  # retrieves the latest HASH_VALUE for each Primary Key
        #print("oracle_latest_HASH_VALUE:")
        #print(oracle_latest_HASH_VALUE)

        oracle_list_of_HASH_VALUES: List = oracle_latest_HASH_VALUE['latest_hash_value'].tolist()  # might be faster if this was a Set, instead of List
        #print('oracle_list_of_HASH_VALUES = ', oracle_list_of_HASH_VALUES)


        # If HASH_VALUE is bytes, convert it to string (this happens when HASHBYTES is used instead of CHECKSUM)
        max_hash = insert_this['HASH_VALUE'].max()
        if isinstance(max_hash, bytes):
            #print('B_LOADID = {} -> HASH_VALUE was BYTES'.format(auto_increment))
            insert_this['HASH_VALUE'] = insert_this['HASH_VALUE'].astype('str')

        # Drop the rows that have the same HASH_VALUE as in the Oracle table (exit if nothing to insert)
        insert_this = insert_this[~insert_this['HASH_VALUE'].isin(oracle_list_of_HASH_VALUES)]
        #print(insert_this)

        if len(insert_this) == 0:  # if all the HASH_VALUES we got from On-Prem matched the latest ones from Oracle, then there's nothing to insert, so return
            logger.info('{}: {} rows inserted ({} retrieved from {}) in {} with {} = {}\n'.format(inspect.stack()[0][3], len(insert_this), nr_rows_retrieved, on_prem_database, oracle_table, col_to_increment, auto_increment if delete_last else auto_increment + 0))
            return

        insert_this = insert_this.where(pd.notnull(insert_this), None)  # fucking magic. Without this, there are null conversion issues
        insert_this.columns: List[str] = [str(_) for _ in range(0, len(insert_this.columns))]  # instead of the actual column names, put: 0, 1, 2 ...
        insert_this = insert_this.to_dict('records')  # makes a list of dicts (one dict per row)

        query: str = """INSERT INTO {table} ({columns}) VALUES ({column_number})"""\
                   .format(table=oracle_table,
                           columns= all_next_words_after_word(sql_statement, after_this_word='AS', split_string_by=',') if '* FROM'.format(oracle_table) not in sql_statement else insert_this_original_columns
                           ,column_number=self.make_string(nr_cols))  # /*+ PARALLEL({table}) */
        # `make_string` fct will create something like: :0, :1, :2 ... -> those are the column names of `insert_this`
        #print('sql_statement = ', sql_statement)
        # print('query = ', query)

        self.cursor.executemany(query, insert_this)
        self.connection.commit()

        # Run the job that moves from SA tables to GD tables
        #self.run_oracle_function(instance=instance_new, fct_name="MTA_SUBMIT_LOAD", fct_params= [auto_increment, 'INTEGRATE_HOUSING', 'adrian_iordache'])
        Singleton(key=auto_increment, value=[hierarchy, auto_increment, "MTA_SUBMIT_LOAD", 'INTEGRATE_HOUSING', 'adrian_iordache_'+self.machine_running_script, oracle_table])  # adding somewhere where they can be accessed later

        # Log what the fuck happened
        logger.info('{}: {} rows inserted ({} retrieved from {}) in {} with {} = {}\n'.format(inspect.stack()[0][3], len(insert_this), nr_rows_retrieved,  on_prem_database, oracle_table, col_to_increment, auto_increment if delete_last else auto_increment+0))


    @staticmethod
    def insert_to_oracle_specify_columns_pickable(oracle_table, server, on_prem_database, sql_statement, col_to_increment, delete_last=True) -> None:
        AWS_Connecter().insert_to_oracle_specify_columns(oracle_table, server, on_prem_database, sql_statement, col_to_increment, delete_last)


if __name__ == "__main__":
    AWS = AWS_Connecter(environment='Dev')
    #AWS.execute_sql(sql_statement='SELECT COUNT(*) FROM SA_RESIDENTS')  # smoke test
    #print('successful connection to AWS Oracle')

    AWS.execute_sql(sql_statement='DELETE FROM SA_RESIDENTS')
    AWS.execute_sql(sql_statement='DELETE FROM GD_RESIDENTS')

    # AWS.execute_sql(sql_statement='DELETE FROM SA_LOOKUP')
    # AWS.execute_sql(sql_statement='DELETE FROM SA_PATCH_LOOKUP')
    # AWS.execute_sql(sql_statement='DELETE FROM SA_TENURE_TYPE')
    #
    # AWS.execute_sql(sql_statement='DELETE FROM SA_LOCAL_AUTHORITY')
    # AWS.execute_sql(sql_statement='DELETE FROM SA_STAFF_INVOLVEMENT')
    # AWS.execute_sql(sql_statement='DELETE FROM SA_SCHEMES')
    #
    # AWS.execute_sql(sql_statement='DELETE FROM SA_BLOCKS')
    # AWS.execute_sql(sql_statement='DELETE FROM SA_SUB_BLOCKS')
    # AWS.execute_sql(sql_statement='DELETE FROM SA_UNITS')  # this deletes only the data
    # AWS.execute_sql(sql_statement='DELETE FROM GD_UNITS')  # careful, this is GD


    # AWS.execute_sql(sql_statement='DELETE FROM SA_RESIDENTS')
    # AWS.execute_sql(sql_statement='DELETE FROM SA_PERSON')
    #
    # AWS.execute_sql(sql_statement='DELETE FROM SA_RENT_GRP_REF')
    # AWS.execute_sql(sql_statement='DELETE FROM SA_PERSON_LOOKUP')
    # AWS.execute_sql(sql_statement='DELETE FROM SA_CONTACT_PREFRENCES')
    # AWS.execute_sql(sql_statement='DELETE FROM SA_COMMUNICATION')
    # AWS.execute_sql(sql_statement='DELETE FROM SA_ECONOMIC_STATUS')
    # AWS.execute_sql(sql_statement='DELETE FROM SA_VULNERABILTY_DETAILS')
    #
    # AWS.execute_sql(sql_statement='DELETE FROM GD_RESIDENTS')
    # AWS.execute_sql(sql_statement='DELETE FROM GD_RENT_GRP_REF')
    # AWS.execute_sql(sql_statement='DELETE FROM GD_PERSON')
    # AWS.execute_sql(sql_statement='DELETE FROM GD_PERSON_LOOKUP')
    # AWS.execute_sql(sql_statement='DELETE FROM GD_CONTACT_PREFRENCES')
    # AWS.execute_sql(sql_statement='DELETE FROM GD_COMMUNICATION')
    # AWS.execute_sql(sql_statement='DELETE FROM GD_ECONOMIC_STATUS')
    # AWS.execute_sql(sql_statement='DELETE FROM GD_VULNERABILTY_DETAILS')
    # #AWS.execute_sql(sql_statement='DROP TABLE Dummy_Table PURGE')  # this deletes the table completely

    # AWS.execute_sql(sql_statement='DELETE FROM SA_ESTATE_INSP_AND_CLEANING')
    # AWS.execute_sql(sql_statement='DELETE FROM SA_PROPERTY_TYPE')
    # AWS.execute_sql(sql_statement='DELETE FROM SA_UNIT_CLUSTER')
    # AWS.execute_sql(sql_statement='DELETE FROM SA_PROPERTY_ATTRIBUTES')
    #
    # AWS.execute_sql(sql_statement='DELETE FROM SA_ATTRIBUTE_KEY_LOOKUP')
    # AWS.execute_sql(sql_statement='DELETE FROM SA_PROP_ATTRIBUTE_TYPE')
    # AWS.execute_sql(sql_statement='DELETE FROM SA_PROP_ATTRIBUTE_KEY_VALUE')
    #
    # AWS.execute_sql(sql_statement='DELETE FROM GD_UNITS')  # this deletes only the data
    # AWS.execute_sql(sql_statement='DELETE FROM GD_SCHEMES')
    # AWS.execute_sql(sql_statement='DELETE FROM GD_BLOCKS')
    #
    # AWS.execute_sql(sql_statement='DELETE FROM GD_ESTATE_INSP_AND_CLEANING')
    # AWS.execute_sql(sql_statement='DELETE FROM GD_PROPERTY_TYPE')
    #
    # AWS.execute_sql(sql_statement='DELETE FROM GD_LOOKUP')
    # AWS.execute_sql(sql_statement='DELETE FROM GD_LOCAL_AUTHORITY')
    # AWS.execute_sql(sql_statement='DELETE FROM GD_TENURE_TYPE')
    #
    # AWS.execute_sql(sql_statement='DELETE FROM GD_STAFF_INVOLVEMENT')
    # AWS.execute_sql(sql_statement='DELETE FROM GD_UNIT_CLUSTER')
    # AWS.execute_sql(sql_statement='DELETE FROM GD_PROPERTY_ATTRIBUTES')
    #
    # AWS.execute_sql(sql_statement='DELETE FROM GD_ATTRIBUTE_KEY_LOOKUP')
    # AWS.execute_sql(sql_statement='DELETE FROM GD_PROP_ATTRIBUTE_TYPE')
    # AWS.execute_sql(sql_statement='DELETE FROM GD_PROP_ATTRIBUTE_KEY_VALUE')

    ###############
    # AWS.execute_sql(sql_statement="""CREATE TABLE INSERT_
    #                                  ( PROPERTY_CODE varchar2(50) NOT NULL,
    #                                    PROPERTY_NAME varchar2(50) NOT NULL
    #                                  )
    #                               """
    #                 )

    #AWS.execute_sql(sql_statement='SELECT * from GD_BLOCKS')  # GX_LOOKUP
    #print(AWS.fetch_to_pandas(sql_statement ='SELECT owner, table_name FROM all_tables'))
    print('Done deleting')
