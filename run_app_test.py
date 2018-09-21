from AWS_Connecter import AWS_Connecter
from what_to_update import tables_to_update
import logging
import os
from config import config

if __name__ == "__main__":
    desktop = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')

    ENVIRONMENT = 'Dev'  # the only possible values are: 'Dev' / 'Prod'

    logging.basicConfig(
        filename=desktop + '\\' + ENVIRONMENT + '.txt',
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s | ThreadID: %(thread)d | %(levelname)s: %(message)s",
        filemode='w'
    )

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
                                              d['oracle_table'], d['server'], d['on_prem_database'], d['sql_statement'], d['col_to_increment'], d['primary_key'], d['delete_last']
                                )
                                for d in tables_to_update
                                # if d['oracle_table'] not in ['SA_RESIDENTS', 'SA_COMMUNICATION', 'SA_VULNERABILTY_DETAILS', 'SA_ECONOMIC_STATUS', 'SA_CONTACT_PREFRENCES', 'SA_RENT_GRP_REF', 'SA_PERSON', 'SA_PERSON_LOOKUP']
                                if d['oracle_table'] in  ['SA_RENT_GRP_REF'] #['SA_ECONOMIC_STATUS', 'SA_CONTACT_PREFRENCES', 'SA_COMMUNICATION', 'SA_VULNERABILTY_DETAILS']
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

    # Print we're done
    logging.info('Done')  # this will be logged in last because of the line above `executor.shutdown()`, which must wait for everything to finish
    print('Done')
