import sqlalchemy
import cx_Oracle
from pprint import pprint
import logging
import inspect
from UDFs import all_next_words_after_word, modify_script
from time import sleep
import pandas as pd; pd.set_option('display.width', 1000)
import os
from config import config

i = -1

class AWS_Connecter():
    def __init__(self, environment, host=None, user=None, password=None):
        self.environment = environment  # this should be passed as one of 2 values: 'Dev' / 'Prod'

        # Dynamically determine the credentials based on environment
        self.host = host if host != None else config['AWS-Dev']['host'] if self.environment == 'Dev' else config['AWS-Prod']['host'] if self.environment == 'Prod' else 'idiot'
        self.user = user if user != None else config['AWS-Dev']['user'] if self.environment == 'Dev' else config['AWS-Prod']['user'] if self.environment == 'Prod' else 'idiot'
        self.password = password if password != None else config['AWS-Dev']['password'] if self.environment == 'Dev' else config['AWS-Prod']['password'] if self.environment == 'Prod' else 'idiot'

        self.port = config['AWS-Dev']['port'] if self.environment == 'Dev' else config['AWS-Prod']['port'] if self.environment == 'Prod' else 'idiot'
        self.sid = config['AWS-Dev']['sid'] if self.environment == 'Dev' else config['AWS-Prod']['sid'] if self.environment == 'Prod' else 'idiot'

        # the 2 functions MTA_GET_NEW_LOADID (which gives the LOAD_ID that we need to use) & MTA_SUBMIT_LOAD (which pushes SA to GD) are taken from here
        self.SemarchyFunctions_host = config['SemarchyFunctions-Dev']['host'] if self.environment == 'Dev' else config['SemarchyFunctions-Prod']['host'] if self.environment == 'Prod' else 'idiot'
        self.SemarchyFunctions_user = config['SemarchyFunctions-Dev']['user'] if self.environment == 'Dev' else config['SemarchyFunctions-Prod']['user'] if self.environment == 'Prod' else 'idiot'
        self.SemarchyFunctions_password = config['SemarchyFunctions-Dev']['password'] if self.environment == 'Dev' else config['SemarchyFunctions-Prod']['password'] if self.environment == 'Prod' else 'idiot'


        self.SID = cx_Oracle.makedsn(self.host, self.port, sid=self.sid)
        connection_string = 'oracle://{user}:{password}@{sid}'.format(user=self.user, password=self.password, sid=self.SID)
        #execution_options = {"timeout": 10000, "statement_timeout": 10000, "query_timeout": 10000, "execution_timeout": 10000}
        self.engine = sqlalchemy.create_engine(connection_string, convert_unicode=False, pool_recycle=1000, pool_size=1000, echo=False)  # pool_pre_ping=True

        # used for inserting
        self.connection = cx_Oracle.Connection("{}/{}@{}".format(self.user, self.password, self.SID))
        self.cursor = cx_Oracle.Cursor(self.connection)

        logging.info('Instantiated: {}'.format(__class__.__name__))

    def execute_sql(self, sql_statement):
        logger = logging.getLogger(__name__); logger.info('calling {} with statement: {}'.format(inspect.stack()[0][3], sql_statement))
        result = self.engine.execute(sql_statement)
        print(result)
        return result

    def fetch_results(self, sql_statement):
        logger = logging.getLogger(__name__); logger.info('calling {}'.format(inspect.stack()[0][3]))
        results = self.execute_sql(sql_statement)
        fetch_results = results.fetchall()
        df = pd.DataFrame(fetch_results)
        return df

    def fetch_to_pandas(self, sql_statement):
        """ Shorter than `fetch_results()` """
        logger = logging.getLogger(__name__); logger.info('calling {}'.format(inspect.stack()[0][3]))
        df = pd.read_sql(sql_statement, self.engine)
        return df

    def create_table_manual_columns(self):
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

    def create_table_from_columns(self, table_name, columns_description):
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

    def create_table_from_name(self, server, db, table_name):
        logger = logging.getLogger(__name__); logger.info('calling {}'.format(inspect.stack()[0][3]))
        from onPremise_Connecter import OnPremise_Connecter
        columns = OnPremise_Connecter(server=server, database=db)._get_columns(table=table_name)
        self.create_table_from_columns(table_name='_' + table_name, columns_description=columns)

    @staticmethod
    def make_string(x):
        """ :param x: 5
            :return: :0, :1, :2, :3, :4
        """
        L = list(range(0, x))
        L = list(map(str, L))
        L = [':' + x for x in L]
        L = ', '.join(L)
        return L

    def insert_to_oracle(self, oracle_table, server, on_prem_database, sql_statement):
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

        from onPremise_Connecter import OnPremise_Connecter
        insert_this = OnPremise_Connecter(server=server, database=on_prem_database).fetch_to_pandas(sql_statement=sql_statement)
        nr_cols = len(insert_this.columns)
        insert_this = insert_this.to_dict('records')  # makes a list of dicts (one dict per row)

        # Full syntax would be: INSERT INTO <table> (<col1>, <col2>) VALUES (<val1>, <val2>)
        query = """INSERT INTO {table} VALUES ({columns})""".format(table=oracle_table, columns=self.make_string(nr_cols))
        # `make_string` fct will create something like: :0, :1, :2 ... -> those are the column names of `insert_this`

        self.cursor.executemany(query, insert_this)
        self.connection.commit()

    @classmethod  # https://stackoverflow.com/questions/12179271/meaning-of-classmethod-and-staticmethod-for-beginner
    def instance_other_user(cls, environment, host, user, password):
        logger = logging.getLogger(__name__); logger.info('calling {}'.format(inspect.stack()[0][3]))
        instance_other_user = cls(environment, host, user, password)  # essentially each instance of AWS_Connecter() will create yet another AWS_Connecter() instance by using this approach
        return instance_other_user

   # http://www.oracle.com/technetwork/articles/prez-stored-proc-084100.html
    def run_oracle_function(self, instance, fct_name, fct_params):
        run_fct = instance.cursor.callfunc(fct_name, cx_Oracle.NUMBER, fct_params)

        logger = logging.getLogger(__name__)
        logger.info(f'{fct_name} called with params = {fct_params}')
        return run_fct

    def _sql_query_to_get_Oracle_latest_HASH_VALUES(self, oracle_table, primary_key, col_to_increment):
        text = """SELECT main.{PRIMARY_KEY}, main.{col_to_increment}, main.HASH_VALUE as LATEST_HASH_VALUE
               FROM
                    (SELECT {PRIMARY_KEY}
                           ,MAX({col_to_increment}) as {col_to_increment}
                           
                    FROM {oracle_table}
                    GROUP BY {PRIMARY_KEY}) grouped
                  
                   INNER JOIN {oracle_table} main
                   ON main.{PRIMARY_KEY} = grouped.{PRIMARY_KEY}
                   AND main.{col_to_increment} = grouped.{col_to_increment}""".format(oracle_table=oracle_table, PRIMARY_KEY=primary_key, col_to_increment=col_to_increment)
        return text

    def insert_to_oracle_specify_columns(self, oracle_table, server, on_prem_database, sql_statement, col_to_increment, primary_key, delete_last=False):
        # global i
        # i += 1
        # x = i  # this is very important
        """sql_statement needs to have the columns after the word `AS`, and they need to match the name of the columns in Oracle table!"""
        logger = logging.getLogger(__name__); logger.info('calling {}'.format(inspect.stack()[0][3]))

        # instance_new = self.instance_other_user(host='tvha-aws-semarchy-dev.csaymlyq76p1.eu-west-1.rds.amazonaws.com', user='SEMARCHY_REPOSITORY', password='SEMARCHY_REPOSITORY')
        instance_new = self.instance_other_user(environment=self.environment, host=self.SemarchyFunctions_host, user=self.SemarchyFunctions_user, password=self.SemarchyFunctions_password)
        auto_increment = self.run_oracle_function(instance=instance_new, fct_name="MTA_GET_NEW_LOADID", fct_params=['TVHA_MDM', 0, 0, 'Python', 'upload_data', 'adrian_iordache'])

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
        modification = " {} AS {} ".format(auto_increment if delete_last else auto_increment+0, col_to_increment) #; print('modification =', modification)

        #sql_statement = sql_statement.split()[0] + modification + ' '.join(sql_statement.split()[1:])
        sql_statement = modify_script(old_script=sql_statement, modification=modification)
        #logger.info('The SQL statement used to update {} from {} db is \n{}'.format(oracle_table, on_prem_database, sql_statement))

        # Extract the data that you want to insert from the On-Premise database
        from onPremise_Connecter import OnPremise_Connecter
        insert_this = OnPremise_Connecter(server=server, database=on_prem_database).fetch_to_pandas(sql_statement=sql_statement)
        nr_rows_retrieved = len(insert_this)
        nr_cols = len(insert_this.columns)
        insert_this_original_columns = ', '.join(insert_this.columns)  # ; print('insert_this_original_columns = ', insert_this_original_columns)

        # Make a modification for testing purposes! Delete afterwards this code
        # if oracle_table == 'SA_UNITS' and on_prem_database == 'Fizzy':
        #     for x in ['26040', '26041', '26042', '26043']:
        #         insert_this.loc[insert_this.UNITS_ID == x, 'HASH_VALUE'] = 999999999
        #     print('fake change got executed')


        # Get the latest HASH_VALUE in ORACLE for each Primary Key (SCHEMES_ID, BLOCK_ID etc depending on the table)
        get_oracle_latest_HASH_VALUE = self._sql_query_to_get_Oracle_latest_HASH_VALUES(oracle_table=oracle_table, primary_key=primary_key, col_to_increment=col_to_increment)  # creates the SQL code
        #print(f'get_oracle_latest_HASH_VALUE = {get_oracle_latest_HASH_VALUE}')

        oracle_latest_HASH_VALUE = self.fetch_to_pandas(get_oracle_latest_HASH_VALUE)  # retrieves the latest HASH_VALUE for each Primary Key
        #print("oracle_latest_HASH_VALUE:")
        #print(oracle_latest_HASH_VALUE)

        oracle_list_of_HASH_VALUES = oracle_latest_HASH_VALUE['latest_hash_value'].tolist()
        #print('oracle_list_of_HASH_VALUES = ', oracle_list_of_HASH_VALUES)


        # If HASH_VALUE is bytes, convert it to string (this happens when HASHBYTES is used instead of CHECKSUM)
        max_hash = insert_this['HASH_VALUE'].max()
        if isinstance(max_hash, bytes):
            print('B_LOADID = {} -> HASH_VALUE was BYTES'.format(auto_increment))
            insert_this['HASH_VALUE'] = insert_this['HASH_VALUE'].astype('str')

        # Drop the rows that have the same HASH_VALUE as in the Oracle table (exit if nothing to insert)
        insert_this = insert_this[~insert_this['HASH_VALUE'].isin(oracle_list_of_HASH_VALUES)]
        #print(insert_this)

        if len(insert_this) == 0:  # if all the HASH_VALUES we got from On-Prem matched the latest one from Oracle, then there's nothing to insert, so return
            logger.info('{}: {} rows inserted ({} retrieved from {}) in {} with {} = {}\n'.format(inspect.stack()[0][3], len(insert_this), nr_rows_retrieved, on_prem_database, oracle_table, col_to_increment, auto_increment if delete_last else auto_increment + 0))
            return

        insert_this = insert_this.where(pd.notnull(insert_this), None)  # fucking magic. Without this, there are null conversion issues
        insert_this.columns = [str(_) for _ in range(0, len(insert_this.columns))]  # instead of the actual column names, put: 0, 1, 2 ...
        insert_this = insert_this.to_dict('records')  # makes a list of dicts (one dict per row)

        query = """INSERT INTO {table} ({columns}) VALUES ({column_number})"""\
                   .format(table=oracle_table,
                           columns= all_next_words_after_word(sql_statement, after_this_word='AS', split_string_by=',') if '* FROM'.format(oracle_table) not in sql_statement else insert_this_original_columns
                           ,column_number=self.make_string(nr_cols))  # /*+ PARALLEL({table}) */
        # `make_string` fct will create something like: :0, :1, :2 ... -> those are the column names of `insert_this`
        #print('sql_statement = ', sql_statement)
        # print('query = ', query)

        self.cursor.executemany(query, insert_this)
        self.connection.commit()

        # Run the job that moves from SA tables to GD tables
        self.run_oracle_function(instance=instance_new, fct_name="MTA_SUBMIT_LOAD", fct_params= [auto_increment, 'INTEGRATE_HOUSING', 'adrian_iordache'])

        # Log what the fuck happened
        logger.info('{}: {} rows inserted ({} retrieved from {}) in {} with {} = {}\n'.format(inspect.stack()[0][3], len(insert_this), nr_rows_retrieved,  on_prem_database, oracle_table, col_to_increment, auto_increment if delete_last else auto_increment+0))


    @staticmethod
    def insert_to_oracle_specify_columns_pickable(oracle_table, server, on_prem_database, sql_statement, col_to_increment, delete_last=True):
        AWS_Connecter().insert_to_oracle_specify_columns(oracle_table, server, on_prem_database, sql_statement, col_to_increment, delete_last)


if __name__ == "__main__":
    # AWS.execute_sql(sql_statement='SELECT * from GD_BLOCKS')  # GX_LOOKUP
    #print(AWS.fetch_to_pandas(sql_statement ='SELECT owner, table_name FROM all_tables'))

    # AWS = AWS_Connecter(host='tvha-aws-semarchy-dev.csaymlyq76p1.eu-west-1.rds.amazonaws.com', user='TVHA_MDM', password='TVHA_MDM')
    AWS = AWS_Connecter(environment='Dev')
    # AWS.execute_sql(sql_statement='DELETE FROM SA_RESIDENTS')
    # AWS.execute_sql(sql_statement='DELETE FROM SA_RENT_GRP_REF')
    # AWS.execute_sql(sql_statement='DELETE FROM SA_PERSON')
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





    ###############
    # AWS.execute_sql(sql_statement="""CREATE TABLE INSERT_
    #                                  ( PROPERTY_CODE varchar2(50) NOT NULL,
    #                                    PROPERTY_NAME varchar2(50) NOT NULL
    #                                  )
    #                               """
    #                 )

    #AWS.execute_sql(sql_statement='DROP TABLE Dummy_Table PURGE')  # this deletes the table completely
    # AWS.execute_sql(sql_statement='DELETE FROM SA_UNITS')  # this deletes only the data
    # AWS.execute_sql(sql_statement='DELETE FROM SA_SCHEMES')
    # AWS.execute_sql(sql_statement='DELETE FROM SA_BLOCKS')
    #
    # AWS.execute_sql(sql_statement='DELETE FROM SA_ESTATE_INSP_AND_CLEANING')
    # AWS.execute_sql(sql_statement='DELETE FROM SA_PROPERTY_TYPE')
    # AWS.execute_sql(sql_statement='DELETE FROM SA_MANAGING_AGENTS')
    #
    # AWS.execute_sql(sql_statement='DELETE FROM SA_LOOKUP')
    # AWS.execute_sql(sql_statement='DELETE FROM SA_LOCAL_AUTHORITY')
    # AWS.execute_sql(sql_statement='DELETE FROM SA_TENURE_TYPE')
    #
    # AWS.execute_sql(sql_statement='DELETE FROM SA_STAFF_INVOLVEMENT')
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
    # AWS.execute_sql(sql_statement='DELETE FROM GD_MANAGING_AGENTS')
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

    print('Done deleting')
