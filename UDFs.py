import datetime
import sys
import os
import logging
from typing import List

def set_logging(environment: str, path_to_logs: str = None, file_name_time: bool = False) -> None:
    def path_to_desktop() -> str:  # this func is only called when running on Windows
        desktop: str = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')
        return desktop + '/Logs Semarchy'

    path_to_logs: str = path_to_logs if path_to_logs is not None else path_to_desktop() if "win" in sys.platform else "/opt/semarchy/SemarchyLogs" if "linux" in sys.platform else "foo"

    now_str: str = datetime.datetime.now().strftime("%Y-%m-%d %Hh %Mm %Ss")
    file_name: str = environment if file_name_time is False else environment + " " + now_str
    filename: str = path_to_logs + '/' + file_name + '.txt'

    print('saving logs to file:', filename)
    logging.basicConfig(
        filename=filename,
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s | ThreadID: %(thread)d | %(levelname)s: %(message)s",
        filemode='w'
    )


def all_next_words_after_word(my_string: str, after_this_word: str='AS', split_string_by: str=',') -> str:
    def find_between_helper(s: str=my_string, w: str=after_this_word):
        if s.find(w) < 0:  # the word (w) is not in the string (s)
            return None
        else:
            res = s[s.find(w) + len(w):].split()[0]
            return res

    final = map(find_between_helper, my_string.split(split_string_by))
    final = filter(None, final)  # removes any None elements
    final = ', '.join(final)
    return final

def modify_script(old_script: str, modification: str) -> str:  # a bit overkill (not possible to have more than 1 DISTINCT keywords in reality)
    """  INSERTS the `modification` at the right place inside the `old_script`
         ex: old_script = SELECT DISTINCT COLUMN_1, COLUMN_2 FROM MY_TABLE
             modification = 99 AS BL_LOADID
         => returns: SELECT DISTINCT COLUMN_1, 99 AS BL_LOADID , COLUMN_2 FROM MY_TABLE
    """
    split_by_comma: List[str] = old_script.split(',')
    contains_DISTINCT: List[str] = [x for x in split_by_comma if 'DISTINCT' in x]
    other_ELEMENTS: List[str] = split_by_comma[len(contains_DISTINCT):]

    if not contains_DISTINCT:  # no `DISTINCT` keyword in the old_script
        after_SELECT = other_ELEMENTS[0].split('SELECT')[1]; #print('after_SELECT = ', after_SELECT)
        if len(other_ELEMENTS) == 1:
            new_script = 'SELECT ' + modification.strip() + ',' + after_SELECT
        else:
            new_script = 'SELECT ' + modification.strip() + ',' + after_SELECT + ',' + ','.join(other_ELEMENTS[1:])
        #print("no `DISTINCT` keyword in the old_script")
        return replace_star(new_script)

    if not other_ELEMENTS:  # all columns are prefixed with `DISTINCT`
        before_FROM = contains_DISTINCT[-1].split('FROM')[0].strip()
        after_FROM = contains_DISTINCT[-1].split('FROM')[1]

        new_script = ','.join(contains_DISTINCT[:-1]) + ',' + before_FROM + ',' + modification + 'FROM' + after_FROM
        #print("all columns are prefixed with `DISTINCT`")
        return replace_star(new_script)

    contains_DISTINCT = ['SELECT'] if not contains_DISTINCT else contains_DISTINCT
    modification = (',' if contains_DISTINCT != ['SELECT'] else '') + modification

    new_script: str = ','.join(contains_DISTINCT) + modification + ',' + ','.join(other_ELEMENTS)
    return replace_star(new_script)


def replace_star(script: str) -> str:
    """ modifies this: SELECT 1099 AS dbo.B_LOADID, * FROM my_table     # this works for SQL Server, but NOT for Oracle
        to this: SELECT 1099 AS dbo.B_LOADID, my_table.* FROM my_table  # this works for both

        But if there is something like: SELECT * FROM (...), that is not modified, because 2 words after '*' we have a '('
    """

    if '*' not in script:  # if there is no '*' in the script, just return the script as it is
        return script

    sql_script_as_list: List[str] = script.split()
    star_position_in_sql_script: int = sql_script_as_list.index('*')

    try:
        two_words_after_star: str = sql_script_as_list[star_position_in_sql_script + 2]
    except IndexError:  # this should not happen; but if there is an error, just return the script as it is
        return script

    if two_words_after_star != '(':
        script = script.replace('*', two_words_after_star + '.*')
    return script


if __name__ == "__main__":
    sql = """select 456 AS B_LOAD_ID_FAKE,
                 456 AS B_CLASSNAME_FAKE,

                 a.code_ AS SchemeCode,
                 a.name_ AS SchemeName,
                 'FIZ_EST_001' AS F_ESTATES,
                 '3' AS SourceSystem

                 FROM  Area a"""

    # a = all_next_words_after_word(my_string=sql, after_this_word='AS', split_string_by=',')
    # print(a, type(a))

    # modification = " '1371' AS B_LOADID, "
    # sql = sql.split()[0] + modification + ' '.join(sql.split()[1:])
    # print(sql)



    s = """SELECT DISTINCT 'TP_' + Convert (Varchar,b.ComponentID) AS ATTRIBUTE_KEY_LOOKUP_ID,
                   b.Component AS TYPE_NAME, 
                    CHECKSUM(b.ComponentID,b.Component) AS HASH_VALUE, 
                        'AttributeKeyLookup' AS B_CLASSNAME,
                        '1' AS F_SOURCE_SYSTEM,
                        '41' AS F_DATA_OWNERSHIP, 
                        dateadd(day,-1,getdate()) AS VALID_FROM,
                        dateadd(month,100,getdate()) AS VALID_TO 
            FROM (...)"""

    old_script: str = """SELECT HOUSE_SIZE, 
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
                                              F_TENURE_TYPE F_TENURE_TYPE, 
                                              b_classname AS b_classname, 
                                              to_date(B_CREDATE,'DD-MON-RRRR HH:MI:SS') AS B_CREDATE, 
                                              B_CREATOR AS B_CREATOR, 
                                              F_SOURCE_SYSTEM AS F_SOURCE_SYSTEM, 
                                              F_DATA_OWNERSHIP AS F_DATA_OWNERSHIP, 
                                              hash_value AS hash_value
                                          FROM semarchy_residents"""
    # new_script: str = modify_script(old_script=old_script, modification=' 1099 AS dbo.B_LOADID')
    # print(new_script)
    a = all_next_words_after_word(my_string=old_script, after_this_word='AS', split_string_by=',')
    print(a, type(a))
