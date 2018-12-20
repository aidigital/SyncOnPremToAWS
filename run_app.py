import logging
from typing import Dict, List, Any
from time import sleep

from AWS_Connecter import AWS_Connecter  # AWS_Connecter class uses onPremise_Connecter2 via composition
from what_to_update import tables_to_update
from config import config
from UDFs import set_logging

if __name__ == "__main__":
    ENVIRONMENT = 'Prod'  # the only possible values are: 'Dev' / 'Prod'
    RUN_FOR = ['TVH']  # the only possible values are: 'TVH' / 'MTH Test' / 'MTH Live' ; the TVH On-Prem VM can only connect to 'TVH'

    set_logging(environment=ENVIRONMENT, file_name_time=True)  # just provide the path_to_logs if you want to save them somewhere else

    # ----------------- METHOD 1 -----------------
    # good old for loop - no parallelism here, each function starts after the previous one finishes
    # AWS = AWS_Connecter()
    # for d in tables_to_update:
    #     AWS.insert_to_oracle_specify_columns(oracle_table=d['oracle_table'], server=d['server'],
    #                                          on_prem_database=d['on_prem_database'], col_to_increment=d['col_to_increment'],
    #                                          sql_statement=d['sql_statement'], delete_last=d['delete_last'])


    # ----------------- METHOD 2 -----------------
    # when using multiprocessing or concurrent.futures.ProcessPoolExecutor, the function needs to be pickleable
    # logging didn't work as expected with this method
    # AWS = AWS_Connecter()
    # import multiprocessing
    # pool = multiprocessing.Pool(processes=4)
    # multiprocessing.freeze_support()
    #
    # for d in tables_to_update:
    #     pool.starmap(AWS.insert_to_oracle_specify_columns_pickable, zip([d['oracle_table']], [d['server']], [d['on_prem_database']],[d['sql_statement']], [d['col_to_increment']], [d['delete_last']]))
    #
    # pool.close()


    # # ----------------- METHOD 3 -----------------
    import concurrent.futures

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=len(tables_to_update))
    wait_for = {executor.submit(AWS_Connecter(environment=ENVIRONMENT).insert_to_oracle_specify_columns,
                                              d['oracle_table'], d['hierarchy'], config['servers'][d['server']], d['on_prem_database'], d['sql_statement'],
                                              d['col_to_increment'], d['primary_key'], d['company'], d['delete_last']
                                ): str(d['hierarchy'])+'_'+d['oracle_table']+'_'+d['on_prem_database']
                                for d in tables_to_update
                                if d['company'] in RUN_FOR  # 2 choices: TVH / MTH
                                #if d['oracle_table'] not in ['SA_ALERT_INFO_LOOKUP', 'SA_ALERTS_INFO_MASTER']
                }  # these Futures will immediately start getting executed
    # above, I use AWS_Connecter() which creates a new object each time. However, if using the same object, cursor.executemany() fails to insert data (various errors received) when run in parallel (multiple threads)
    # so essentially, with this approach, each dict in `tables_to_update` creates 1 instance of AWS_Connecter() class, and 1 instance of OnPremise_Connecter() class

    print("\nfutures:", wait_for, "\n")  # List of Future Instances, each Instance having `state` = `running`

    import traceback
    nr_errors = 0
    for f in concurrent.futures.as_completed(wait_for):
        table_fail = wait_for[f]
        try:
            print('{} {} returned --> {}'.format(table_fail, f, f.result()))  # .result() blocks until the task Completes (either by returning a value or raising an exception), or is Canceled; but in our case, it's already completed (so it doesn't block)
        except Exception:
            print('--unexpected error with {} future {}: {}'.format(table_fail, f, traceback.format_exc()))
            logging.info('--unexpected error with {} future {}:\n{}'.format(table_fail, f, traceback.format_exc()))
            nr_errors += 1

    print('\n', wait_for)  # List of Future Instances, each Instance having `state` = `finished` (+ the result returned or exception raised)
    print(f'\n>>> Nr of tables with errors while pushing data to SA tables: {nr_errors}/{len(wait_for)}')
    logging.info(f'>>> Nr of tables with errors while pushing data to SA tables: {nr_errors}/{len(wait_for)}')
    executor.shutdown()


    # all the SA tables have been populated; we've also created a dict with all the SA_to_GD jobs that need to be run
    from singleton import Borg, sort_values_of_dict
    SA_to_GD_functions: Dict = Borg().__dict__  # SA_to_GD funcs were written here by insert_to_oracle_specify_columns()
    SA_to_GD_functions_sorted: List[List] = sort_values_of_dict(SA_to_GD_functions)

    print("\nSA_to_GD_functions:", SA_to_GD_functions, '\n')
    print("SA_to_GD_functions_sorted:", SA_to_GD_functions_sorted, '\n')

    # connect to the database that can run the SA_to_GD function, and send all of them, with 1 sec pause in between
    host = config['SemarchyFunctions-Dev']['host'] if ENVIRONMENT == 'Dev' else config['SemarchyFunctions-Prod']['host'] if ENVIRONMENT == 'Prod' else 'idiot'
    user = config['SemarchyFunctions-Dev']['user'] if ENVIRONMENT == 'Dev' else config['SemarchyFunctions-Prod']['user'] if ENVIRONMENT == 'Prod' else 'idiot'
    password = config['SemarchyFunctions-Dev']['password'] if ENVIRONMENT == 'Dev' else config['SemarchyFunctions-Prod']['password'] if ENVIRONMENT == 'Prod' else 'idiot'
    runJobs = AWS_Connecter(environment=ENVIRONMENT, host=host, user=user, password=password)

    for i, func in enumerate(SA_to_GD_functions_sorted, 1):
        runJobs.run_oracle_function(instance=runJobs, fct_name=func[2], fct_params=[func[1], func[3], func[4]])  # ex: fct_params = [auto_increment, 'INTEGRATE_HOUSING', 'adrian_iordache']
        sleep(2)
        print(f'{i}. MTA_SUBMIT_LOAD(id= {func[1]}) for table {func[5]} (priority: {func[0]}) has been sent to Semarchy')
        logging.info(f'{i}. MTA_SUBMIT_LOAD(id= {func[1]}) for table {func[5]} (priority: {func[0]}) has been sent to Semarchy')

    # Print we're done
    logging.info('Done')  # this will be logged in last because of the line above `executor.shutdown()`, which must wait for everything to finish
    print('Done')
