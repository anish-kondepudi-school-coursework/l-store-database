import unittest
from unittest import mock
from lstore import PageRange, Table


class TestTable(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.primary_key_col: int = 0

    @classmethod
    def tearDownClass(self):
        self.primary_key_col: int = None

    def test_insert_record(self) -> None:
        table: Table = Table("table1", 2, self.primary_key_col)
        page_range: mock.MagicMock = mock.Mock()
        page_range.is_full: mock.MagicMock = mock.Mock(return_value=False)
        table.page_ranges: list[PageRange] = [page_range]
        record: list[int] = [1, 2]

        insert_ok: bool = table.insert_record(record)
        self.assertTrue(insert_ok)
        self.assertEqual(len(table.page_ranges), 1)

    def test_insert_record_latest_page_range_full(self) -> None:
        table: Table = Table("table1", 2, self.primary_key_col)
        page_range: mock.MagicMock = mock.Mock()
        page_range.is_full: mock.MagicMock = mock.Mock(return_value=True)
        table.page_ranges: list[PageRange] = [page_range]
        record: list[int] = [1, 2]

        insert_ok: bool = table.insert_record(record)
        self.assertTrue(insert_ok)
        self.assertEqual(len(table.page_ranges), 2)

    def test_insert_record_with_duplicate_key(self) -> None:
        table: Table = Table("table1", 2, self.primary_key_col)
        table.index.key_exists: mock.MagicMock = mock.Mock(return_value=True)
        table.index.add_key_rid: mock.MagicMock = mock.Mock()
        page_range: mock.MagicMock = mock.Mock()
        page_range.is_full: mock.MagicMock = mock.Mock(return_value=False)
        table.page_ranges: list[mock.MagicMock] = [page_range]
        with self.assertRaises(AssertionError):
            table.insert_record([1, 2])
        # ensure atomicity - make sure no changes to table resulted from invalid transaction
        self.assertEqual(len(table.page_ranges), 1)
        self.assertFalse(page_range.insert_record.called)
        self.assertFalse(table.index.add_key_rid.called)

    def test_update_existent_record(self) -> None:
        table: Table = Table("table1", 3, self.primary_key_col)
        record: list[int] = [10, 20, 30]
        new_record: list[int] = [11, 21, 31]

        table.insert_record(record)
        old_rid: int = table.index.get_rid(record[self.primary_key_col])
        update_ok: bool = table.update_record(record[self.primary_key_col], new_record)
        self.assertTrue(update_ok)

        given_rid: int = table.index.get_rid(new_record[self.primary_key_col])
        self.assertEqual(given_rid, old_rid)

        with self.assertRaises(AssertionError):
            table.index.get_rid(record[self.primary_key_col])

    def test_update_non_existing_record(self) -> None:
        table: Table = Table("table1", 2, self.primary_key_col)
        with self.assertRaises(AssertionError):
            table.update_record(1, [90, 14])

    def test_get_latest_column_values_after_insert(self) -> None:
        table: Table = Table("table1", 2, self.primary_key_col)
        prim_key: int = 10
        record: list[int] = [prim_key, 20]
        table.insert_record(record)
        self._test_all_get_column_possibilities(prim_key, table, record)

    def test_get_latest_column_values_after_update(self) -> None:
        table: Table = Table("table1", 2, self.primary_key_col)
        rid: int = table.insert_record([1, 2])
        new_prim_key: int = 90
        new_record: list[int] = [new_prim_key, 14]
        table.update_record(rid, new_record)
        self._test_all_get_column_possibilities(new_prim_key, table, new_record)

    def test_delete_record(self) -> None:
        table: Table = Table("table1", 2, self.primary_key_col)
        prim_key = 1
        table.insert_record([prim_key, 2])
        table.delete_record(prim_key)
        with self.assertRaises(AssertionError):
            table.get_latest_column_values(prim_key, [1, 1])

    def _test_all_get_column_possibilities(self, prim_key, table, record) -> None:
        rid: int = table.index.get_rid(prim_key)
        self.assertEqual(table.get_latest_column_values(rid, [0, 0]), [])
        self.assertEqual(table.get_latest_column_values(rid, [0, 1]), [record[1]])
        self.assertEqual(table.get_latest_column_values(rid, [1, 0]), [record[0]])
        self.assertEqual(table.get_latest_column_values(rid, [1, 1]), record)

    def test_get_latest_column_values_nonexisting_record(self) -> None:
        table: Table = Table("table1", 2, self.primary_key_col)
        with self.assertRaises(AssertionError):
            table.get_latest_column_values(1, [1, 1])

    def test_get_latest_column_values_invalid_projected_cols(self) -> None:
        table: Table = Table("table1", 2, self.primary_key_col)
        with self.assertRaises(AssertionError):
            table.get_latest_column_values(1, [1, 1, 1])

    def test_delete_non_existing_record(self) -> None:
        table: Table = Table("table1", 2, self.primary_key_col)
        with self.assertRaises(AssertionError):
            table.delete_record(1)


if __name__ == "__main__":
    unittest.main()
