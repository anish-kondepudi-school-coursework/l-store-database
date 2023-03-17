import os
import pickle
from typing import Dict, List, Set, Tuple
from enum import Enum
import multiprocessing as mp
import time
from multiprocessing.synchronize import Event
from .seeding import SeedSet
from .enums import DSAStructure, Operation

class AsyncSecondaryIndex(mp.Process):
    """
    #: An asynchronous secondary index that is iniitalized and managed
    """

    def __init__(
        self,
        name: str,
        attribute: str,
        request_queue: mp.Queue,
        response_queue: mp.Queue,
        stop_event: Event,
        structure: DSAStructure = DSAStructure.DICTIONARY_SET,
        seed: bool = False,
    ) -> None:
        """
        `name`: name of the parent table
        `attribute`: identifying name of attribute, used when saving index in secondary memory and for identifying the worker process
        `structure`: the structure of the secondary index, used to determine which implementation to use
        `request_queue`: queue for receiving requests from the server
        `response_queue`: queue for sending responses to the server
        `stop_event`: event for stopping the worker process
        #: the search object (dict or btree) and seeding objects are either loaded from secondary memory
        or initialized using the load_query method.
        """
        # initializing multiprocessing components of the object
        super().__init__()
        self.task_queue = request_queue
        self.result_queue = response_queue
        self.stop_event: Event = stop_event
        # setting the descriptors of the secondary index structure and name
        self.index_name = f"{name}_attr_{attribute}"
        self.structure: DSAStructure = structure
        # initializing the internal data structure of the secondary index itself
        if seed:
            self.seeds: SeedSet = SeedSet([])
        else:
            self.seeds: bool = False
        self.initialize_structure()
        self.load_query(replace=True)

    def run(self):
        """
        #: Continously runs the worker process, waiting for requests from the server
        #: Terminates when the Table object uses this object's respective process' stop_event to terminate
        #: Reads from the task_queue, which contains arrays of operations, and then performs them
        #: Writes to the result_queue, which contains the results of the operations
        """
        while not self.stop_event.is_set():
            if not self.task_queue.empty():
                # note that requests are batched on the level of the table
                while not self.task_queue.empty() and not self.stop_event.is_set():
                    # batches contain the operation to be performed, the key, the rid, and the request ID
                    batch: List[Tuple[Operation, int, int, int]] = self.task_queue.get()
                    for request in batch:
                        if self.stop_event.is_set():
                            break
                        response = self.perform_operation(request)
                        self.result_queue.put(response)

    def perform_operation(self, request: Tuple[Operation, int, int, int]) -> Tuple[int, bool | List[int] | Exception]:
        """
        `operation`: the operation to be performed
        `key`: the key to be added to the index
        `rid`: the rid of the record that is being added to the index
        `request_id`: the ID of the request, used to identify the request in the result queue
        #: Performs the operation on the secondary index, and returns the result of the operation and
        the associated request ID
        #: Upon failure, returns the request ID and the error message
        """
        operation, key, rid, request_id = request
        try:
            if operation == Operation.INSERT_RECORD:
                self.add_record(key, rid)
                return (request_id, True)
            elif operation == Operation.DELETE_RECORD:
                self.delete_record(key, rid)
                return (request_id, True)
            elif operation == Operation.SEARCH_RECORD:
                return (request_id, self.search_record(key))
            elif operation == Operation.SAVE_INDEX:
                self.save_index()
                return (request_id, True)
            elif operation == Operation.LOAD_INDEX:
                self.load_query()
                return (request_id, True)
        except Exception as error:
            return (request_id, error)

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
            if rid in vals:
                vals.remove(rid)
            if self.seeds:
                self.seeds.remove(rid)