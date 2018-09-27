from AWS_Connecter import AWS_Connecter
from what_to_update import tables_to_update
import logging
import os
from config import config
from typing import Dict, List, Any
from time import sleep

if __name__ == "__main__":
    ENVIRONMENT = 'Dev'  # the only possible values are: 'Dev' / 'Prod'

    def set_logging(environment: str, path_to_logs: str = None, file_name_time: bool = False) -> None:
        import datetime
        import sys

        def path_to_desktop() -> str:  # this func is only called when running on Windows
            desktop: str = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')
            return desktop

        path_to_logs: str = path_to_logs if path_to_logs is not None else path_to_desktop() if "win" in sys.platform else "/opt/semarchy/SemarchyLogs" if "linux" in sys.platform else "foo"

        now_str: str = datetime.datetime.now().strftime("%Y-%m-%d %Hh %Mm %Ss")
        file_name: str = environment if file_name_time is False else environment + " " + now_str

        logging.basicConfig(
            filename=path_to_logs + '\\' + file_name + '.txt',
            level=logging.DEBUG,
            format="%(asctime)s - %(name)s | ThreadID: %(thread)d | %(levelname)s: %(message)s",
            filemode='w'
        )

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
    wait_for = [executor.submit(AWS_Connecter(environment=ENVIRONMENT).insert_to_oracle_specify_columns,
                                              d['oracle_table'], d['hierarchy'], config['servers'][d['server']], d['on_prem_database'], d['sql_statement'], d['col_to_increment'], d['primary_key'], d['delete_last']
                                )
                                for d in tables_to_update
                                # if d['oracle_table'] not in ['SA_RESIDENTS', 'SA_COMMUNICATION', 'SA_VULNERABILTY_DETAILS', 'SA_ECONOMIC_STATUS', 'SA_CONTACT_PREFRENCES', 'SA_RENT_GRP_REF', 'SA_PERSON', 'SA_PERSON_LOOKUP']
                                #if d['oracle_table'] in  ['SA_ECONOMIC_STATUS', 'SA_CONTACT_PREFRENCES', 'SA_COMMUNICATION', 'SA_VULNERABILTY_DETAILS']
               ] # these will immediately start getting executed
    # above, I use AWS_Connecter() which creates a new object each time. However, if using the same object, cursor.executemany() fails to insert data (various errors received) when run in parallel (multiple threads)
    # so essentially, with this approach, each dict in `tables_to_update` creates 1 instance of AWS_Connecter() class, and 1 instance of OnPremise_Connecter() class

    # print(wait_for, '\n')  # List of Future Instances, each Instance having `state` = `running`

    import traceback
    for f in concurrent.futures.as_completed(wait_for):
        try:
            print('{} returned --> {}'.format(f, f.result()))  # .result() blocks until the task Completes (either by returning a value or raising an exception), or is Canceled
        except Exception:
            print('unexpected error: ', traceback.format_exc())
            logging.info('Unexpected Error:\n{}'.format(traceback.format_exc()))

    print('\n', wait_for)  # List of Future Instances, each Instance having `state` = `finished` (+ the result returned or exception raised)

    executor.shutdown()


    # all the SA tables have been populated; we've also created a dict with all the SA_to_GD jobs that need to be run
    from singleton import Borg, sort_values_of_dict
    SA_to_GD_functions: Dict = Borg().__dict__  # SA_to_GD funcs were written here by insert_to_oracle_specify_columns()
    SA_to_GD_functions_sorted: List[List] = sort_values_of_dict(SA_to_GD_functions)

    print("SA_to_GD_functions:", SA_to_GD_functions)
    print("SA_to_GD_functions_sorted:", SA_to_GD_functions_sorted)

    # connect to the database that can run the SA_to_GD function, and send all of them, with 1 sec pause in between
    host = config['SemarchyFunctions-Dev']['host'] if ENVIRONMENT == 'Dev' else config['SemarchyFunctions-Prod']['host'] if ENVIRONMENT == 'Prod' else 'idiot'
    user = config['SemarchyFunctions-Dev']['user'] if ENVIRONMENT == 'Dev' else config['SemarchyFunctions-Prod']['user'] if ENVIRONMENT == 'Prod' else 'idiot'
    password = config['SemarchyFunctions-Dev']['password'] if ENVIRONMENT == 'Dev' else config['SemarchyFunctions-Prod']['password'] if ENVIRONMENT == 'Prod' else 'idiot'
    runJobs = AWS_Connecter(environment=ENVIRONMENT, host=host, user=user, password=password)

    for func in SA_to_GD_functions_sorted:
        #runJobs.run_oracle_function(instance=runJobs, fct_name=func[2], fct_params=[func[1], func[3], func[4]])  # ex: fct_params = [auto_increment, 'INTEGRATE_HOUSING', 'adrian_iordache']
        sleep(2)
        print(f'function MTA_SUBMIT_LOAD with ID {func[1]} has been sent to Semarchy')



    # Print we're done
    logging.info('Done')  # this will be logged in last because of the line above `executor.shutdown()`, which must wait for everything to finish
    print('Done')
