import unittest
import os
from lstore import SecondaryIndex, Table, Index
import copy


class TestSecondary(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.table: Table = Table("table1", 5, 0)

    @classmethod
    def tearDownClass(self):
        self.table: Table = None

    def test_adding_secondary(self) -> None:
        RECORD_VALUE: int = 5
        records: list[list[int]] = [
            [1, 2, 3, 4, RECORD_VALUE],
            [2, 6, 5, 1, RECORD_VALUE],
            [3, 2, 8, 3, RECORD_VALUE],
        ]
        for record in records:
            self.table.insert_record(record)
        # retrieving the rids of the records
        expected_rids = [self.table.index.get_rid(record[0]) for record in records]
        # checking the secondary index
        real_rids = self.table.secondary_indices[4].search_record(RECORD_VALUE)
        self.assertEqual(expected_rids, real_rids)

    def test_loading_secondary_proper(self) -> None:
        TABLE_NAME = "table1_test"
        table: Table = Table(TABLE_NAME, 5, 0)
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
        secondary = SecondaryIndex(table.name, f"attribute_4", multiprocess=False)
        # loading values in secondary index
        secondary.load_query(replace=True)
        # checking the secondary index
        real_rids = secondary.search_record(RECORD_VALUE)
        self.assertEqual(expected_rids, real_rids)
        try:
            os.remove("table1_test_attr_attribute_4")
        except OSError:
            pass

    def test_loading_secondary_proper_and_insert(self) -> None:
        RECORD_VALUE: int = 5
        TABLE_NAME = "table1_test"
        table: Table = Table(TABLE_NAME, 5, 0)
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
        secondary = SecondaryIndex(table.name, f"attribute_4", multiprocess=False)
        secondary.load_query(replace=True)
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
        # cleanup, deleting the pickled file
        try:
            os.remove("table1_test_attr_attribute_4")
        except OSError:
            pass

    def test_inserting_primary_key_again(self) -> None:
        RECORD_VALUE: int = 5
        TABLE_NAME = "table1_test"
        table: Table = Table(TABLE_NAME, 5, 0)
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

    def test_for_update_changes(self):
        table: Table = Table("table1", 5, 0)
        RECORD_VALUE: int = 5
        records: list[list[int]] = [
            [1, 2, 3, 4, RECORD_VALUE],
            [2, 6, 5, 1, RECORD_VALUE],
            [3, 2, 8, 3, RECORD_VALUE],
            [9, 1, 5, 2, RECORD_VALUE],
            [4, 1, 5, 5, RECORD_VALUE],
        ]
        for record in records:
            table.insert_record(record)
        # retrieving the rids of inserted records
        expected_rids = [table.index.get_rid(record[0]) for record in records]
        values_before_update = copy.deepcopy(
            table.secondary_indices[4].search_record(RECORD_VALUE)
        )
        # updating a value expected to be in index 4, ie 5th attribute
        update_value = [4, 1, 4, 4, RECORD_VALUE - 1]
        table.update_record(update_value[0], update_value)
        # extracting the new rid list, which should no longer contain expected_rid[-1]
        values_after_update = table.secondary_indices[4].search_record(RECORD_VALUE)
        self.assertEqual(expected_rids, values_before_update)
        expected_rids.pop(-1)
        self.assertEqual(expected_rids, values_after_update)


if __name__ == "__main__":
    unittest.main()
