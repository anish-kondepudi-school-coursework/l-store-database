import unittest
import os
from unittest import mock
from lstore import (
    SecondaryIndex,
    Table,
    DSAStructure,
    Bufferpool,
    DiskInterface,
    PageRange,
    AsyncSecondaryIndex,
    Operation,
)
import copy
import time
import multiprocessing as mp


# constants
TABLE_NAME = "table1_test"
ATTRIBUTE_NAME = "attribute_4"
RECORD_VALUE: int = 5


class TestSecondary(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        bufferpool = Bufferpool(1000, "")
        bufferpool.disk: DiskInterface = mock.Mock()
        bufferpool.disk.page_exists.return_value = False
        self.table: Table = Table("table1", 5, 0, bufferpool)

    @classmethod
    def tearDownClass(self):
        self.table: Table = None
        try:
            self.delete_all_saved_indices(self)
        except OSError:
            pass

    def create_bufferpool(self) -> Bufferpool:
        bufferpool = Bufferpool(1000, "")
        bufferpool.disk: DiskInterface = mock.Mock()
        bufferpool.disk.page_exists.return_value = False
        return bufferpool

    def test_insert_multiprocessing(self) -> None:
        self.delete_all_saved_indices()
        bufferpool = self.create_bufferpool()
        table: Table = Table(
            TABLE_NAME,
            5,
            0,
            bufferpool,
            mp=True,
            secondary_structure=DSAStructure.DICTIONARY_SET,
        )
        table.insert_record([1, 2, 3, 4, 5])
        table.stop_all_secondary_indices()

    def test_select_multiprocessing(self) -> None:
        self.delete_all_saved_indices()
        bufferpool = self.create_bufferpool()
        table: Table = Table(
            TABLE_NAME,
            5,
            0,
            bufferpool,
            mp=True,
            secondary_structure=DSAStructure.DICTIONARY_SET,
        )
        records: list[list[int]] = [
            [1, 2, 3, 4, RECORD_VALUE],
            [2, 6, 5, 1, RECORD_VALUE],
            [3, 2, 8, 3, RECORD_VALUE],
        ]
        for record in records:
            table.insert_record(record)
        values = table.wait_for_async_responses()
        # retrieving the rids of the records
        expected_rids = [table.index.get_rid(record[0]) for record in records]
        request, response = table.search_secondary_multiprocessing(RECORD_VALUE, 4)
        try:
            self.assertEqual(list(response), expected_rids)
        except AssertionError:
            print(response)
            print(expected_rids)
            table.stop_all_secondary_indices()
            raise
        table.stop_all_secondary_indices()

    def test_save_load_multiprocessing_single(self) -> None:
        self.delete_all_saved_indices()
        bufferpool = self.create_bufferpool()
        table: Table = Table(
            TABLE_NAME,
            2,
            0,
            bufferpool,
            mp=True,
            secondary_structure=DSAStructure.DICTIONARY_SET,
        )
        records: list[list[int]] = [
            [1, RECORD_VALUE],
            [2, RECORD_VALUE],
            [3, RECORD_VALUE],
        ]
        for record in records:
            table.insert_record(record)
        values = table.wait_for_async_responses()
        expected_rids = [table.index.get_rid(record[0]) for record in records]
        # save the secondary indices
        table.prepare_to_be_pickled()
        # load the secondary indices
        request_queue = mp.Queue()
        response_queue = mp.Queue()
        stop_event = mp.Event()
        index = AsyncSecondaryIndex(
            TABLE_NAME,
            "attribute_1",
            request_queue,
            response_queue,
            stop_event,
            structure=DSAStructure.DICTIONARY_SET,
        )
        index.start()
        request = (Operation.SEARCH_RECORD, RECORD_VALUE, 1, 0)
        request_queue.put([request])
        while True:
            if not response_queue.empty():
                _, response = response_queue.get()
                break
        self.assertEqual(list(response), expected_rids)
        stop_event.set()
        index.join()

    def test_save_load_multiprocessing(self) -> None:
        self.delete_all_saved_indices()
        bufferpool = self.create_bufferpool()
        table: Table = Table(
            TABLE_NAME,
            5,
            0,
            bufferpool,
            mp=True,
            secondary_structure=DSAStructure.DICTIONARY_SET,
        )
        records: list[list[int]] = [
            [1, 2, 3, 4, RECORD_VALUE],
            [2, 6, 5, 1, RECORD_VALUE],
            [3, 2, 8, 3, RECORD_VALUE],
        ]
        for record in records:
            table.insert_record(record)
        table.wait_for_async_responses()
        expected_rids = [table.index.get_rid(record[0]) for record in records]
        # save the secondary indices
        table.prepare_to_be_pickled()
        # checking to see if they have been saved
        for attribute in range(table.num_columns):
            if attribute == table.primary_key_col:
                continue
            self.assertTrue(os.path.exists(f"{TABLE_NAME}_attr_attribute_{attribute}"))
        # load the secondary indices
        request_queue = mp.Queue()
        response_queue = mp.Queue()
        stop_event = mp.Event()
        index = AsyncSecondaryIndex(
            TABLE_NAME,
            f"{ATTRIBUTE_NAME}",
            request_queue,
            response_queue,
            stop_event,
            structure=DSAStructure.DICTIONARY_SET,
        )
        index.start()
        request = (Operation.SEARCH_RECORD, RECORD_VALUE, 1, 0)
        request_queue.put([request])
        while True:
            if not response_queue.empty():
                _, response = response_queue.get()
                break
        self.assertEqual(list(response), expected_rids)
        stop_event.set()
        index.join()


    def test_delete_functionality(self) -> None:
        self.delete_all_saved_indices()
        bufferpool = self.create_bufferpool()
        table: Table = Table(
            TABLE_NAME,
            5,
            0,
            bufferpool,
            mp=True,
            secondary_structure=DSAStructure.DICTIONARY_SET,
        )
        records: list[list[int]] = [
            [1, 2, 3, 4, RECORD_VALUE],
            [2, 6, 5, 1, RECORD_VALUE],
            [3, 2, 8, 1, RECORD_VALUE],
            [4, 1, 7, 0, RECORD_VALUE],
            [5, 9, 4, 2, RECORD_VALUE],
        ]
        for record in records:
            table.insert_record(record)
        table.wait_for_async_responses()
        expected_rids = [table.index.get_rid(record[0]) for record in records]
        # remove the last record in the records array
        self.assertEqual(len(expected_rids), len(records))
        table.delete_record(records[-1][0])
        table.wait_for_async_responses()
        _, response = table.search_secondary_multiprocessing(RECORD_VALUE, 4)
        self.assertEqual(len(response), len(records) - 1)
        self.assertEqual(list(response), expected_rids[:-1])
        table.stop_all_secondary_indices()

    """
    Helper function
    """

    def delete_all_saved_indices(self) -> None:
        prefixed = [
            filename
            for filename in os.listdir(".")
            if filename.startswith(f"{TABLE_NAME}_attr_")
        ]
        for filename in prefixed:
            os.remove(filename)

    def check_if_saved_exists(self) -> None:
        self.assertEqual(os.path.isfile(f"{TABLE_NAME}_attr_{ATTRIBUTE_NAME}"), False)


if __name__ == "__main__":
    unittest.main()