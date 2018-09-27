from typing import Dict, List, Any

class Borg:
    """Borg class making class attribute global"""
    _shared_state: Dict = {}  # Attribute dictionary; Singleton class has access to this w/o needing and __init__

    def __init__(self):  # creating a Borg object runs this __init__ => Borg().__dict__ will run it
        print("Borg's __init__ ran")
        self.__dict__: Dict = self._shared_state  # Make it an attribute dictionary


class Singleton(Borg):  # Inherits from the Borg class
    """This class now shares all its attributes among its various instances"""
    # This essentially makes the singleton object an object-oriented global variable

    def __init__(self, key, value):
        #Borg.__init__(self)
        # Update the attribute dictionary by inserting a new key-value pair
        self._shared_state.update({key: value})

    def __str__(self):
        # Returns the attribute dictionary for printing
        return str(self._shared_state)

def sort_values_of_dict(myDict: Dict[int, List], sortByNthElement: int=0) -> List[List]:
    """assumes the values in the dict are lists; gets all the values and sorts them by the 1st number in each list"""
    list_of_dict_values: List[List] = list(myDict.values())
    list_of_dict_values_sorted: List[List] = sorted(list_of_dict_values, key=lambda v: v[sortByNthElement])

    return list_of_dict_values_sorted


if __name__ == "__main__":
    x = Singleton(321, [98, "dsa"])  # Create the 1st singleton object
    y = Singleton(4332, [2, "da"])  # Create the 2nd singleton object

    # print("x=", x)
    # print("y=", y)
    #
    # print("Borg._shared_state=", Borg._shared_state)
    # print("Borg.__dict__=", Borg.__dict__)
    # print("Borg().__dict__=", Borg().__dict__)  # Borg's __init__ populates this

    # sort by the first number in the list
    z = Singleton(5453, [1, "dsa"])
    myDict = Borg().__dict__  # Singleton._shared_state
    print("myDict:", myDict)


    # alternative: instead of getting a sorted list, getting a sorted dict out of Borg().__dict__
    # import collections
    # orderedDict = collections.OrderedDict(sorted(myDict.values(), key=lambda v: +v[0]) )
    # print(orderedDict, type(orderedDict))


    a = sort_values_of_dict(myDict)
    print("sorted list of values", a)




