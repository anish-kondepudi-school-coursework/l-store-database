from enum import Enum

# will offer the option to run in a parallel process, which will return an endpoint to request server
# or to run in the same process, and will just return itself. include B+ tree and dictionary implmenetation
class DSAStructure(Enum):
    B_TREE_ARRAY = 1
    B_TREE_SET = 2
    B_TREE_DICT = 3
    DICTIONARY_ARRAY = 4
    DICTIONARY_SET = 5
    DICTIONARY_DICT = 6


class Operation(Enum):
    DELETE_RECORD = 1
    INSERT_RECORD = 2
    SEARCH_RECORD = 3
    SAVE_INDEX = 4
    LOAD_INDEX = 5