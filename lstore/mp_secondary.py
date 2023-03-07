import os
import pickle
from typing import Dict, List, Set
from enum import Enum
from .seeding import SeedSet

# will offer the option to run in a parallel process, which will return an endpoint to request server
# or to run in the same process, and will just return itself. include B+ tree and dictionary implmenetation
class DSAStructure(Enum):
    B_TREE_ARRAY = 1
    B_TREE_SET = 2
    B_TREE_DICT = 3
    DICTIONARY_ARRAY = 4
    DICTIONARY_SET = 5
    DICTIONARY_DICT = 6


class AsyncSecondaryIndex:
    def __init__(
        self,
        name: str,
        attribute: str,
        structure: DSAStructure = DSAStructure.DICTIONARY_SET,
        seed: bool = False,
    ) -> None:
        """
        `name`: name of the parent table
        `attribute`: identifying name of attribute, used when saving index in secondary memory
        #: the search object (dict or btree) and seeding objects are either loaded from secondary memory
        or initialized using the load_query method.
        """
        self.index_name = f"{name}_attr_{attribute}"
        self.structure: DSAStructure = structure
        # initializing seeds if deciding to maintain
        if seed:
            self.seeds: SeedSet = SeedSet([])
        else:
            self.seeds: bool = False
        self.dictionary: Dict[int, List[int]] = {}
        self.load_query(replace=True)

    def initialize_structure(self):
        if self.structure == DSAStructure.B_TREE_ARRAY:
            pass
        elif self.structure == DSAStructure.B_TREE_SET:
            pass
        elif self.structure == DSAStructure.B_TREE_DICT:
            pass
        elif self.structure == DSAStructure.DICTIONARY_ARRAY:
            self.dictionary: Dict[int, List[int]] = {}
        elif self.structure == DSAStructure.DICTIONARY_SET:
            self.dictionary: Dict[int, Set[int]] = {}
        elif self.structure == DSAStructure.DICTIONARY_DICT:
            self.dictionary: Dict[int, Dict[int, int]] = {}

    def load_query(self, replace: bool = False):
        """
        `replace`: used to enforce the overwriting of `dictionary` and `seeds` when loading from secondary memory
        #: checks if the index was saved in secondary memory
        #: note: saved dictionary and seeds variables are saved in the same file, in that order
        """
        if os.path.exists(self.index_name):
            with open(self.index_name, "rb") as f:
                self.dictionary = pickle.load(f)
                self.seeds = pickle.load(f)
        elif replace or (not self.dictionary and not self.seeds):
            self.dictionary, self.seeds = {}, False if self.seeds == False else SeedSet(
                []
            )
        else:
            raise Exception(
                "Indices already exist but a file for them doesn't, and parameter did not specify overwriting existing member varaibles"
            )

    def save_index(self):
        """
        #: Saves the index to secondary memory. Note, the saved file will contain both the dictionary and the seeds
        variables, which are saved in that order.
        """
        with open(self.index_name, "wb") as f:
            pickle.dump(self.dictionary, f)
            pickle.dump(self.seeds, f)

    """ Methods for adding, searching, and deleting records from the index
    Use the Enum class DSAStructure to determine which implementation to use
    """

    def add_record(self, key: int, rid: int):
        """
        `key`: the key to be added to the index
        `rid`: the rid of the record that is being added to the index
        """
        if self.structure == DSAStructure.DICTIONARY_ARRAY:
            self.add_record_dict_array(key, rid)
        elif self.structure == DSAStructure.DICTIONARY_SET:
            self.add_record_dict_set(key, rid)

    def search_record(self, key) -> List[int] | Set[int] | Dict[int, int]:
        """
        `key`: the key to be searched in the index
        #: returns the rid of the record that is being searched for in the index
        """
        if self.structure == DSAStructure.DICTIONARY_ARRAY:
            return self.search_record_dict_array(key)
        elif self.structure == DSAStructure.DICTIONARY_SET:
            return self.search_record_dict_set(key)

    def delete_record(self, key: int, rid: int):
        """
        `key`: the key to be deleted from the index
        `rid`: the rid of the record that is being deleted from the index
        #: note, potential edge case where the same key with the same rid is deleted twice
        """
        if self.structure == DSAStructure.DICTIONARY_ARRAY:
            self.delete_record_dict_array(key, rid)
        elif self.structure == DSAStructure.DICTIONARY_SET:
            self.delete_record_dict_set(key, rid)

    """ DSAStructure.DICTIONARY_ARRAY
    Basic structure of having a dictionary for each secondary index, with the key being the attribute value
    and the element being an array of the RIDs associated with it
    """

    def add_record_dict_array(self, key: int, rid: int):
        """
        `key`: the key to be added to the index
        `rid`: the rid of the record that is being added to the index
        #: note, potential edge case where the same key with the same rid is added twice
        """
        val: List[int] = self.dictionary.setdefault(key, [rid])
        if val != [rid]:
            if self.seeds:
                self.seeds.add(rid)
            val.append(rid)

    def search_record_dict_array(self, key) -> List[int]:
        """
        `key`: the key to be searched in the index
        #: returns the rid of the record that is being searched for in the index
        """
        return self.dictionary.get(key, [])

    def delete_record_dict_array(self, key: int, rid: int):
        """
        `key`: the key to be deleted from the index
        `rid`: the rid of the record that is being deleted from the index
        #: note, potential edge case where the same key with the same rid is deleted twice
        """
        if key in self.dictionary:
            vals: List[int] = self.dictionary[key]
            vals.remove(rid)
            if self.seeds:
                self.seeds.remove(rid)

    """ DSAStructure.DICTIONARY_SET
    Basic structure of having a dictionary for each secondary index, with the key being the attribute value
    and the element being a set of the RIDs associated with it
    """

    def add_record_dict_set(self, key: int, rid: int):
        """
        `key`: the key to be added to the index
        `rid`: the rid of the record that is being added to the index
        #: note, potential edge case where the same key with the same rid is added twice
        """
        val: Set[int] = self.dictionary.setdefault(key, set([rid]))
        val.add(rid)
        if self.seeds:
            self.seeds.add(rid)

    def search_record_dict_set(self, key) -> Set[int]:
        """
        `key`: the key to be searched in the index
        #: returns the rid of the record that is being searched for in the index
        """
        return self.dictionary.get(key, set())

    def delete_record_dict_set(self, key: int, rid: int):
        """
        `key`: the key to be deleted from the index
        `rid`: the rid of the record that is being deleted from the index
        #: note, potential edge case where the same key with the same rid is deleted twice
        """
        if key in self.dictionary:
            vals: Set[int] = self.dictionary[key]
            vals.remove(rid)
            if self.seeds:
                self.seeds.remove(rid)
