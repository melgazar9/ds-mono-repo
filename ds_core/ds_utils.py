from ds_core.ds_imports import *

class MetaclassMethodEnforcer:

    """
    Description
    -----------
    Goal: Design a metaclass enforcer that ensures certain method names exist within a class.
    """

    def __init__(self):
        pass


def find_list_duplicates(input_list):
    return [item for item, count in
            Counter(input_list).items()
            if count > 1]


def merge_dicts(*dict_args):
    """
    Given any number of dictionaries, shallow copy and merge into a new dict,
    precedence goes to key-value pairs in latter dictionaries.
    """
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result