import unittest
from unittest import mock
from lstore import (
    Table
)

class TestTable(unittest.TestCase):
    
    @classmethod
    def setUpClass(self):
        self.primary_key_col: int = 0

    @classmethod
    def tearDownClass(self):
        self.primary_key_col: int = None

    def test_insert_record(self) -> None:
        table: Table = Table('table1', 2, self.primary_key_col)
        page_range: mock.MagicMock = mock.Mock()
        page_range.is_full = mock.Mock(return_value=False)
        table.page_ranges: list = [page_range]
        record: list = [1, 2]
        
        insert_ok: bool = table.insert_record(record)
        self.assertTrue(insert_ok)
        self.assertEqual(len(table.page_ranges), 1)

    def test_insert_record_latest_page_range_full(self) -> None:
        table: Table = Table('table1', 2, self.primary_key_col)
        page_range: mock.MagicMock = mock.Mock()
        page_range.is_full = mock.Mock(return_value=True)
        table.page_ranges: list = [page_range]
        record: list = [1, 2]
        
        insert_ok: bool = table.insert_record(record)
        self.assertTrue(insert_ok)
        self.assertEqual(len(table.page_ranges), 2)

    def test_update_existent_record(self):
        table: Table = Table('table1', 3, self.primary_key_col)
        record: list = [10, 20, 30]
        new_record: list = [11, 21, 31]
        
        table.insert_record(record)
        old_rid: int = table.index.get_rid(record[self.primary_key_col])
        update_ok: bool = table.update_record(record[self.primary_key_col], new_record)
        self.assertTrue(update_ok)
        
        given_rid: int = table.index.get_rid(new_record[self.primary_key_col])
        self.assertEqual(given_rid, old_rid)
        
        with self.assertRaises(AssertionError):
            table.index.get_rid(record[self.primary_key_col])

    def test_update_non_existing_record(self):
        table: Table = Table('table1', 2, self.primary_key_col)
        with self.assertRaises(AssertionError):
            table.update_record(1, [90, 14])

    def test_get_latest_column_values_after_insert(self):
        table: Table = Table('table1', 2, self.primary_key_col)
        prim_key: int = 10
        record: list = [prim_key, 20]
        table.insert_record(record)
        self._test_all_get_column_possibilities(prim_key, table, record)

    def test_get_latest_column_values_after_update(self):
        table: Table = Table('table1', 2, self.primary_key_col)
        rid: int = table.insert_record([1, 2])
        new_prim_key: int = 90
        new_record: list = [new_prim_key, 14]
        table.update_record(rid, new_record)
        self._test_all_get_column_possibilities(new_prim_key, table, new_record)

    def _test_all_get_column_possibilities(self, prim_key, table, record):
        rid: int = table.index.get_rid(prim_key)
        self.assertEqual(table.get_latest_column_values(rid, [0, 0]), [])
        self.assertEqual(table.get_latest_column_values(rid, [0, 1]), [record[1]])
        self.assertEqual(table.get_latest_column_values(rid, [1, 0]), [record[0]])
        self.assertEqual(table.get_latest_column_values(rid, [1, 1]), record)

    def test_get_latest_column_values_nonexisting_record(self):
        table: Table = Table('table1', 2, self.primary_key_col)
        with self.assertRaises(AssertionError):
            table.get_latest_column_values(1, [1, 1]) 

    def test_get_latest_column_values_invalid_projected_cols(self):
        table: Table = Table('table1', 2, self.primary_key_col)
        with self.assertRaises(AssertionError):
            table.get_latest_column_values(1, [1, 1, 1]) 

    def test_delete_record(self):
        table: Table = Table('table1', 2, self.primary_key_col)
        record: list = [1, 2]
        
        table.insert_record(record)
        table.delete_record(record[self.primary_key_col])
        
        with self.assertRaises(AssertionError):
            table.index.get_rid(record[self.primary_key_col])
        with self.assertRaises(AssertionError):
            table.update_record(record[self.primary_key_col], record)
        with self.assertRaises(AssertionError):
            table.get_latest_column_values(table.index.get_rid(record[self.primary_key_col]), [1])
    
    def test_delete_non_existing_record(self):
        table: Table = Table('table1', 2, self.primary_key_col)
        with self.assertRaises(AssertionError):
            table.delete_record(1)

if __name__ == '__main__':
    unittest.main()

    

