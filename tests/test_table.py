import unittest
from unittest import mock
from unittest.mock import patch
from lstore import (
    Table
)

class TestTable(unittest.TestCase):
    
    @classmethod
    def setUpClass(self):
        self.primary_key_col = 0

    @classmethod
    def tearDownClass(self):
        self.primary_key_col = None

    def test_insert_record(self) -> None:
        table = Table('table1', 2, self.primary_key_col)
        page_range = mock.Mock()
        page_range.is_full = mock.Mock(return_value=False)
        table.page_ranges = [page_range]
        record = [1, 2]
        assert table.insert_record(record)
        assert len(table.page_ranges) == 1

    def test_insert_record_latest_page_range_full(self) -> None:
        table = Table('table1', 2, self.primary_key_col)
        page_range = mock.Mock()
        page_range.is_full = mock.Mock(return_value=True)
        table.page_ranges = [page_range]
        record = [1, 2]
        assert table.insert_record(record)
        assert len(table.page_ranges) == 2

    def test_update_existent_record(self):
        table = Table('table1', 3, self.primary_key_col)
        record = [10, 20, 30]
        new_record = [11, 21, 31]
        table.insert_record(record)
        old_rid = table.index.get_rid(record[self.primary_key_col])
        assert table.update_record(record[self.primary_key_col], new_record)
        # make sure index updated with new primary key
        assert table.index.get_rid(new_record[self.primary_key_col]) == old_rid
        with self.assertRaises(AssertionError):
            table.index.get_rid(record[self.primary_key_col])

    def test_update_non_existing_record(self):
        table = Table('table1', 2, self.primary_key_col)
        with self.assertRaises(AssertionError):
            table.update_record(1, [90, 14])

    def test_get_latest_column_values_after_insert(self):
        table = Table('table1', 2, self.primary_key_col)
        prim_key = 10
        record = [prim_key, 20]
        table.insert_record(record)
        assert table.get_latest_column_values(prim_key, [0, 0]) == []
        assert table.get_latest_column_values(prim_key, [0, 1]) == [record[1]]
        assert table.get_latest_column_values(prim_key, [1, 0]) == [record[0]]
        assert table.get_latest_column_values(prim_key, [1, 1]) == record

    def test_get_latest_column_values_after_update(self):
        table = Table('table1', 2, self.primary_key_col)
        rid = table.insert_record([1, 2])
        new_prim_key = 90
        new_record = [new_prim_key, 14]
        table.update_record(rid, new_record)
        assert table.get_latest_column_values(new_prim_key, [0, 0]) == []
        assert table.get_latest_column_values(new_prim_key, [0, 1]) == [new_record[1]]
        assert table.get_latest_column_values(new_prim_key, [1, 0]) == [new_record[0]]
        assert table.get_latest_column_values(new_prim_key, [1, 1]) == new_record

    def test_get_latest_column_values_nonexisting_record(self):
        table = Table('table1', 2, self.primary_key_col)
        with self.assertRaises(AssertionError):
            table.get_latest_column_values(1, [1, 1]) 

    def test_delete_record(self):
        table = Table('table1', 2, self.primary_key_col)
        record = [1, 2]
        table.insert_record(record)
        table.delete_record(record[self.primary_key_col])
        with self.assertRaises(AssertionError):
            table.index.get_rid(record[self.primary_key_col])
        with self.assertRaises(AssertionError):
            table.update_record(record[self.primary_key_col], record)
        with self.assertRaises(AssertionError):
            table.get_latest_column_values(record[self.primary_key_col], [1])
    
    def test_delete_non_existing_record(self):
        table = Table('table1', 2, self.primary_key_col)
        with self.assertRaises(AssertionError):
            table.delete_record(1)

if __name__ == '__main__':
    unittest.main()