import datetime
import sys
import os
import logging

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
    split_by_comma = old_script.split(',')
    contains_DISTINCT = [x for x in split_by_comma if 'DISTINCT' in x]
    other_ELEMENTS = split_by_comma[len(contains_DISTINCT):]; #print('other_ELEMENTS = ', other_ELEMENTS)
    #print('contains_DISTINCT = ', contains_DISTINCT)
    #print('other_ELEMENTS = ', other_ELEMENTS)

    if not contains_DISTINCT:  # no `DISTINCT` keyword in the old_script
        after_SELECT = other_ELEMENTS[0].split('SELECT')[1]; #print('after_SELECT = ', after_SELECT)
        if len(other_ELEMENTS) == 1:
            new_script = 'SELECT ' + modification.strip() + ',' + after_SELECT
        else:
            new_script = 'SELECT ' + modification.strip() + ',' + after_SELECT + ',' + ','.join(other_ELEMENTS[1:])
        #print("no `DISTINCT` keyword in the old_script")
        return new_script

    if not other_ELEMENTS:  # all columns are prefixed with `DISTINCT`
        before_FROM = contains_DISTINCT[-1].split('FROM')[0].strip()
        after_FROM = contains_DISTINCT[-1].split('FROM')[1]

        new_script = ','.join(contains_DISTINCT[:-1]) + ',' + before_FROM + ',' + modification + 'FROM' + after_FROM
        #print("all columns are prefixed with `DISTINCT`")
        return new_script

    contains_DISTINCT = ['SELECT'] if not contains_DISTINCT else contains_DISTINCT
    modification = (',' if contains_DISTINCT != ['SELECT'] else '') + modification

    new_script = ','.join(contains_DISTINCT) + modification + ',' + ','.join(other_ELEMENTS)

    return new_script

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

    # s = 'SELECT * FROM my_table'
    x = modify_script(s, ' 1099 AS dbo.B_LOADID ')
    print(x)