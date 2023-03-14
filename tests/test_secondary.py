import unittest
import os
from unittest import mock
from lstore import SecondaryIndex, Table, DSAStructure, Bufferpool, DiskInterface, PageRange
import copy


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

    def test_adding_secondary_array(self) -> None:
        self.check_if_saved_exists()
        bufferpool = self.create_bufferpool()
        table: Table = Table(TABLE_NAME, 5, 0, bufferpool, secondary_structure=DSAStructure.DICTIONARY_ARRAY)
        RECORD_VALUE: int = 5
        records: list[list[int]] = [
            [1, 2, 3, 4, RECORD_VALUE],
            [2, 6, 5, 1, RECORD_VALUE],
            [3, 2, 8, 3, RECORD_VALUE],
        ]
        for record in records:
            table.insert_record(record)
        # retrieving the rids of the records
        expected_rids = [table.index.get_rid(record[0]) for record in records]
        # checking the secondary index
        real_rids = table.secondary_indices[4].search_record(RECORD_VALUE)
        self.assertEqual(expected_rids, real_rids)

    def test_loading_secondary_proper_array(self) -> None:
        self.check_if_saved_exists()
        bufferpool = self.create_bufferpool()
        table: Table = Table(TABLE_NAME, 5, 0, bufferpool, secondary_structure=DSAStructure.DICTIONARY_ARRAY)
        RECORD_VALUE: int = 5
        records: list[list[int]] = [
            [1, 2, 3, 4, RECORD_VALUE],
            [2, 6, 5, 1, RECORD_VALUE],
            [3, 2, 8, 3, RECORD_VALUE],
        ]
        for record in records:
            table.insert_record(record)
        # retrieving the rids of the records and save secondary index
        expected_rids = [table.index.get_rid(record[0]) for record in records]
        table.secondary_indices[4].save_index()
        # creating new secondary index
        secondary = SecondaryIndex(table.name, ATTRIBUTE_NAME, structure=DSAStructure.DICTIONARY_ARRAY)
        # loading values in secondary index
        secondary.load_query(replace=True)
        real_rids = secondary.search_record(RECORD_VALUE)
        try:
            os.remove(f"{TABLE_NAME}_attr_{ATTRIBUTE_NAME}")
        except OSError:
            pass
        # checking the secondary index
        self.assertEqual(expected_rids, real_rids)

    def test_loading_secondary_proper_and_insert_array(self) -> None:
        self.check_if_saved_exists()
        RECORD_VALUE: int = 5
        bufferpool = self.create_bufferpool()
        table: Table = Table(TABLE_NAME, 5, 0, bufferpool, secondary_structure=DSAStructure.DICTIONARY_ARRAY)
        records: list[list[int]] = [
            [1, 2, 3, 4, RECORD_VALUE],
            [2, 6, 5, 1, RECORD_VALUE],
            [3, 2, 8, 3, RECORD_VALUE],
        ]
        for record in records:
            table.insert_record(record)
        # getting expected rids before save
        expected_rids = [table.index.get_rid(record[0]) for record in records]
        table.secondary_indices[4].save_index()
        # creating secondary index and loading it
        secondary = SecondaryIndex(table.name, ATTRIBUTE_NAME)
        secondary.load_query(replace=True)
        # cleanup, deleting the pickled file
        try:
            os.remove(f"{TABLE_NAME}_attr_{ATTRIBUTE_NAME}")
        except OSError:
            pass
        # checking the secondary index
        real_rids = secondary.search_record(RECORD_VALUE)
        self.assertEqual(expected_rids, real_rids)
        # adding a new record to table
        added_record = [4, 1, 5, 5, RECORD_VALUE]
        table.insert_record(added_record)
        # getting RID of inserted record, and adding it to the expected_rids
        rid = table.index.get_rid(added_record[0])
        expected_rids.append(rid)
        real_rids = table.secondary_indices[4].search_record(RECORD_VALUE)
        self.assertEqual(expected_rids, real_rids)

    def test_inserting_primary_key_again_array(self) -> None:
        self.check_if_saved_exists()
        RECORD_VALUE: int = 5
        bufferpool = self.create_bufferpool()
        table: Table = Table(TABLE_NAME, 5, 0, bufferpool, secondary_structure=DSAStructure.DICTIONARY_ARRAY)
        records: list[list[int]] = [
            [1, 2, 3, 4, RECORD_VALUE],
            [2, 6, 5, 1, RECORD_VALUE],
            [3, 2, 8, 3, RECORD_VALUE],
        ]
        for record in records:
            table.insert_record(record)
        # getting expected rids before save
        expected_rids = [table.index.get_rid(record[0]) for record in records]
        # inserting RID with repeated primary key on purpose
        added_record = [3, 1, 5, 5, RECORD_VALUE]
        try:
            table.insert_record(added_record)
        except AssertionError:
            pass
        current_rids = [table.index.get_rid(record[0]) for record in records]
        secondary_rids = table.secondary_indices[4].search_record(RECORD_VALUE)
        self.assertEqual(expected_rids, current_rids)
        self.assertEqual(expected_rids, secondary_rids)

    def test_for_update_changes_array(self) -> None:
        self.check_if_saved_exists()
        bufferpool = self.create_bufferpool()
        table: Table = Table("table1", 5, 0, bufferpool, secondary_structure=DSAStructure.DICTIONARY_ARRAY)
        RECORD_VALUE: int = 5
        records: list[list[int]] = [
            [1, 2, 3, 4, RECORD_VALUE],
            [2, 6, RECORD_VALUE, 1, RECORD_VALUE],
            [3, 2, RECORD_VALUE, 3, RECORD_VALUE],
            [9, 1, 1, 2, RECORD_VALUE],
            [4, 1, 4, 5, RECORD_VALUE],
        ]
        for record in records:
            table.insert_record(record)
        # retrieving the rids of inserted records
        expected_rids_1 = [table.index.get_rid(record[0]) for record in records]
        expected_rids_2 = [expected_rids_1[1], expected_rids_1[2]]
        values_before_update_1 = copy.deepcopy(
            table.secondary_indices[4].search_record(RECORD_VALUE)
        )
        values_before_update_2 = copy.deepcopy(
            table.secondary_indices[2].search_record(RECORD_VALUE)
        )
        # updating a value expected to be in index 4, ie 5th attribute
        update_value = [4, 1, RECORD_VALUE, 4, RECORD_VALUE - 1]
        table.update_record(update_value[0], update_value)
        # extracting the new rid list, which should no longer contain expected_rid[-1]
        values_after_update_1 = table.secondary_indices[4].search_record(RECORD_VALUE)
        values_after_update_2 = table.secondary_indices[2].search_record(RECORD_VALUE)
        self.assertEqual(expected_rids_1, values_before_update_1)
        self.assertEqual(expected_rids_2, values_before_update_2)
        rid_add = expected_rids_1.pop(-1)
        expected_rids_2.append(rid_add)
        self.assertEqual(expected_rids_1, values_after_update_1)
        self.assertEqual(expected_rids_2, values_after_update_2)


    """ Testing for dictionary to set structure """
    def test_adding_secondary_set(self) -> None:
        self.check_if_saved_exists()
        bufferpool = self.create_bufferpool()
        table: Table = Table(TABLE_NAME, 5, 0, bufferpool, secondary_structure=DSAStructure.DICTIONARY_SET)
        RECORD_VALUE: int = 5
        records: list[list[int]] = [
            [1, 2, 3, 4, RECORD_VALUE],
            [2, 6, 5, 1, RECORD_VALUE],
            [3, 2, 8, 3, RECORD_VALUE],
        ]
        for record in records:
            table.insert_record(record)
        # retrieving the rids of the records
        expected_rids = [table.index.get_rid(record[0]) for record in records]
        # checking the secondary index
        real_rids = table.secondary_indices[4].search_record(RECORD_VALUE)
        self.assertEqual(set(expected_rids), real_rids)

    def test_loading_secondary_proper_set(self) -> None:
        self.check_if_saved_exists()
        bufferpool = self.create_bufferpool()
        table: Table = Table(TABLE_NAME, 5, 0, bufferpool, secondary_structure=DSAStructure.DICTIONARY_SET)
        RECORD_VALUE: int = 5
        records: list[list[int]] = [
            [1, 2, 3, 4, RECORD_VALUE],
            [2, 6, 5, 1, RECORD_VALUE],
            [3, 2, 8, 3, RECORD_VALUE],
        ]
        for record in records:
            table.insert_record(record)
        # retrieving the rids of the records and save secondary index
        expected_rids = [table.index.get_rid(record[0]) for record in records]
        table.secondary_indices[4].save_index()
        # creating new secondary index
        secondary = SecondaryIndex(table.name, ATTRIBUTE_NAME, DSAStructure.DICTIONARY_SET)
        # loading values in secondary index
        secondary.load_query(replace=True)
        # cleanup, deleting the pickled file
        try:
            os.remove(f"{TABLE_NAME}_attr_{ATTRIBUTE_NAME}")
        except OSError:
            pass
        # checking the secondary index
        real_rids = secondary.search_record(RECORD_VALUE)
        self.assertEqual(expected_rids.sort(), list(real_rids).sort())

    def test_loading_secondary_proper_and_insert_set(self) -> None:
        self.check_if_saved_exists()
        RECORD_VALUE: int = 5
        bufferpool = self.create_bufferpool()
        table: Table = Table(TABLE_NAME, 5, 0, bufferpool, secondary_structure=DSAStructure.DICTIONARY_SET)
        records: list[list[int]] = [
            [1, 2, 3, 4, RECORD_VALUE],
            [2, 6, 5, 1, RECORD_VALUE],
            [3, 2, 8, 3, RECORD_VALUE],
        ]
        for record in records:
            table.insert_record(record)
        # getting expected rids before save
        expected_rids = [table.index.get_rid(record[0]) for record in records]
        table.secondary_indices[4].save_index()
        # creating secondary index and loading it
        secondary = SecondaryIndex(table.name, ATTRIBUTE_NAME)
        secondary.load_query(replace=True)
        # cleanup, deleting the pickled file
        try:
            os.remove(f"{TABLE_NAME}_attr_{ATTRIBUTE_NAME}")
        except OSError:
            pass
        # checking the secondary index
        real_rids = secondary.search_record(RECORD_VALUE)
        self.assertEqual(expected_rids.sort(), list(real_rids).sort())
        # adding a new record to table
        added_record = [4, 1, 5, 5, RECORD_VALUE]
        table.insert_record(added_record)
        # getting RID of inserted record, and adding it to the expected_rids
        rid = table.index.get_rid(added_record[0])
        expected_rids.append(rid)
        real_rids = table.secondary_indices[4].search_record(RECORD_VALUE)
        self.assertEqual(expected_rids.sort(), list(real_rids).sort())

    def test_inserting_primary_key_again_set(self) -> None:
        self.check_if_saved_exists()
        RECORD_VALUE: int = 5
        bufferpool = self.create_bufferpool()
        table: Table = Table(TABLE_NAME, 5, 0, bufferpool, secondary_structure=DSAStructure.DICTIONARY_SET)
        records: list[list[int]] = [
            [1, 2, 3, 4, RECORD_VALUE],
            [2, 6, 5, 1, RECORD_VALUE],
            [3, 2, 8, 3, RECORD_VALUE],
        ]
        for record in records:
            table.insert_record(record)
        # getting expected rids before save
        expected_rids = [table.index.get_rid(record[0]) for record in records]
        # inserting RID with repeated primary key on purpose
        added_record = [3, 1, 5, 5, RECORD_VALUE]
        try:
            table.insert_record(added_record)
        except AssertionError:
            pass
        current_rids = [table.index.get_rid(record[0]) for record in records]
        secondary_rids = table.secondary_indices[4].search_record(RECORD_VALUE)
        self.assertEqual(expected_rids.sort(), current_rids.sort())
        self.assertEqual(expected_rids.sort(), list(secondary_rids).sort())

    def test_for_update_changes_set(self) -> None:
        self.check_if_saved_exists()
        bufferpool = self.create_bufferpool()
        table: Table = Table("table1", 5, 0, bufferpool, secondary_structure=DSAStructure.DICTIONARY_SET)
        RECORD_VALUE: int = 5
        records: list[list[int]] = [
            [1, 2, 3, 4, RECORD_VALUE],
            [2, 6, RECORD_VALUE, 1, RECORD_VALUE],
            [3, 2, RECORD_VALUE, 3, RECORD_VALUE],
            [9, 1, 1, 2, RECORD_VALUE],
            [4, 1, 4, 5, RECORD_VALUE],
        ]
        for record in records:
            table.insert_record(record)
        # retrieving the rids of inserted records
        expected_rids_1 = [table.index.get_rid(record[0]) for record in records]
        expected_rids_2 = [expected_rids_1[1], expected_rids_1[2]]
        values_before_update_1 = copy.deepcopy(
            table.secondary_indices[4].search_record(RECORD_VALUE)
        )
        values_before_update_2 = copy.deepcopy(
            table.secondary_indices[2].search_record(RECORD_VALUE)
        )
        # updating a value expected to be in index 4, ie 5th attribute
        update_value = [4, 1, RECORD_VALUE, 4, RECORD_VALUE - 1]
        table.update_record(update_value[0], update_value)
        # extracting the new rid list, which should no longer contain expected_rid[-1]
        values_after_update_1 = table.secondary_indices[4].search_record(RECORD_VALUE)
        values_after_update_2 = table.secondary_indices[2].search_record(RECORD_VALUE)
        self.assertEqual(expected_rids_1.sort(), list(values_before_update_1).sort())
        self.assertEqual(expected_rids_2.sort(), list(values_before_update_2).sort())
        rid_add = expected_rids_1.pop(-1)
        expected_rids_2.append(rid_add)
        self.assertEqual(expected_rids_1.sort(), list(values_after_update_1).sort())
        self.assertEqual(expected_rids_2.sort(), list(values_after_update_2).sort())

    """
    Helper function
    """
    def check_if_saved_exists(self) -> None:
        self.assertEqual(os.path.isfile(f"{TABLE_NAME}_attr_{ATTRIBUTE_NAME}"), False)

if __name__ == "__main__":
    unittest.main()
