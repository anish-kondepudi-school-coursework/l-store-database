import multiprocessing as mp
import os
import pickle
from typing import Dict, List

# will offer the option to run in a parallel process, which will return an endpoint to request server
# or to run in the same process, and will just return itself. include B+ tree and dictionary implmenetation
class SecondaryIndex:
    def __init__(self, name: str, attribute: str, multiprocess: bool = False) -> None:
        """
        `name`: name of the parent table
        `attribute`: identifying name of attribute, used when saving index in secondary memory
        `multiprocess`: checks to see if the secondary index will be maintained on the DBMS process or on a separate process
        #: the search object (dict or btree) and seeding objects are either loaded from secondary memory
        or initialized using the load_query method.
        """
        self.index_name = f"{name}_attr_{attribute}"
        self.multiprocess = multiprocess
        self.seeds = []
        self.dictionary: Dict[int, List[int]] = {}
        self.load_query()

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
            self.dictionary, self.seeds = {}, []
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

    def add_record(self, key: int, rid: int):
        """
        `key`: the key to be added to the index
        `rid`: the rid of the record that is being added to the index
        """
        if self.multiprocess:
            self.add_record_mp(key, rid)
        else:
            self.add_record_sp(key, rid)

    def add_record_sp(self, key: int, rid: int):
        """
        `key`: the key to be added to the index
        `rid`: the rid of the record that is being added to the index
        #: note, potential edge case where the same key with the same rid is added twice
        """
        val = self.dictionary.setdefault(key, [rid])
        if val != [rid]:
            val.append(rid)
        else:
            self.seeds.append(key)

    def search_record(self, key):
        """
        `key`: the key to be searched in the index
        #: returns the rid of the record that is being searched for in the index
        """
        if self.multiprocess:
            return self.search_record_mp(key)
        else:
            return self.search_record_sp(key)

    def search_record_mp(self, key):
        """
        `key`: the key to be searched in the index
        #: returns the rid of the record that is being searched for in the index
        """
        pass

    def search_record_sp(self, key):
        """
        `key`: the key to be searched in the index
        #: returns the rid of the record that is being searched for in the index
        """
        return self.dictionary.get(key, [])

    def delete_record(self, key: int, rid: int):
        """
        `key`: the key to be deleted from the index
        `rid`: the rid of the record that is being deleted from the index
        #: note, potential edge case where the same key with the same rid is deleted twice
        """
        if self.multiprocess:
            self.delete_record_mp(key, rid)
        else:
            self.delete_record_sp(key, rid)

    def delete_record_mp(self, key: int, rid: int):
        """
        `key`: the key to be deleted from the index
        `rid`: the rid of the record that is being deleted from the index
        #: note, potential edge case where the same key with the same rid is deleted twice
        """
        pass

    def delete_record_sp(self, key: int, rid: int):
        """
        `key`: the key to be deleted from the index
        `rid`: the rid of the record that is being deleted from the index
        #: note, potential edge case where the same key with the same rid is deleted twice
        """
        if key in self.dictionary:
            vals = self.dictionary[key]
            vals.remove(rid)
            # if not self.dictionary[key]:
            #     self.dictionary.pop(key)
            #     self.seeds.remove(key)
