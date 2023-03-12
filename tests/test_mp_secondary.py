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


# constants
TABLE_NAME = "table1_test"
ATTRIBUTE_NAME = "attribute_4"


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
            os.remove(f"{TABLE_NAME}_attr_{ATTRIBUTE_NAME}")
        except OSError:
            pass

    def create_bufferpool(self) -> Bufferpool:
        bufferpool = Bufferpool(1000, "")
        bufferpool.disk: DiskInterface = mock.Mock()
        bufferpool.disk.page_exists.return_value = False
        return bufferpool

    def test_insert_multiprocessing(self) -> None:
        self.check_if_saved_exists()
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
        self.check_if_saved_exists()
        bufferpool = self.create_bufferpool()
        table: Table = Table(
            TABLE_NAME,
            5,
            0,
            bufferpool,
            mp=True,
            secondary_structure=DSAStructure.DICTIONARY_SET,
        )
        RECORD_VALUE: int = 5
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

    """
    Helper function
    """

    def check_if_saved_exists(self) -> None:
        self.assertEqual(os.path.isfile(f"{TABLE_NAME}_attr_{ATTRIBUTE_NAME}"), False)


if __name__ == "__main__":
    unittest.main()
