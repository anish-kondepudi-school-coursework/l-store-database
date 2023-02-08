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
        self.assertTrue(table.insert_record(record))
        self.assertEqual(len(table.page_ranges), 1)

    def test_insert_record_latest_page_range_full(self) -> None:
        table = Table('table1', 2, self.primary_key_col)
        page_range = mock.Mock()
        page_range.is_full = mock.Mock(return_value=True)
        table.page_ranges = [page_range]
        record = [1, 2]
        self.assertTrue(table.insert_record(record))
        self.assertEqual(len(table.page_ranges), 2)

    def test_update_existent_record(self):
        table = Table('table1', 3, self.primary_key_col)
        record = [10, 20, 30]
        new_record = [11, 21, 31]
        table.insert_record(record)
        old_rid = table.index.get_rid(record[self.primary_key_col])
        self.assertTrue(table.update_record(record[self.primary_key_col], new_record))
        # make sure index updated with new primary key
        self.assertEqual(table.index.get_rid(new_record[self.primary_key_col]), old_rid)
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
        self.assertEqual(table.get_latest_column_values(prim_key, [0, 0]), [])
        self.assertEqual(table.get_latest_column_values(prim_key, [0, 1]), [record[1]])
        self.assertEqual(table.get_latest_column_values(prim_key, [1, 0]), [record[0]])
        self.assertEqual(table.get_latest_column_values(prim_key, [1, 1]), record)

    def test_get_latest_column_values_after_update(self):
        table = Table('table1', 2, self.primary_key_col)
        rid = table.insert_record([1, 2])
        new_prim_key = 90
        new_record = [new_prim_key, 14]
        table.update_record(rid, new_record)
        self.assertEqual(table.get_latest_column_values(new_prim_key, [0, 0]), [])
        self.assertEqual(table.get_latest_column_values(new_prim_key, [0, 1]), [new_record[1]])
        self.assertEqual(table.get_latest_column_values(new_prim_key, [1, 0]), [new_record[0]])
        self.assertEqual(table.get_latest_column_values(new_prim_key, [1, 1]), new_record)

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

# if __name__ == '__main__':
#     unittest.main()
    
# Previous tests:

if __name__ == '__main__':
    primary_key_col = 0
    table = Table('asdfdf', 3, primary_key_col)

    for i in range(100000):
        record = [i, i+1, i+2]
        table.insert_record(record)

    for i in range(5):
        import random
        key = random.randrange(0, 100000) 
        print(f"For {key}, record values before: {table.get_latest_column_values(key, [1, 1, 1])}")
        new_record = [-key, -1-key, -2-key]
        print(f"Updating: {table.update_record(key, new_record)}")
        print(f"Record values after: {table.get_latest_column_values(-key, [1, 1, 1])}")
        print()


    for i in range(10):
        import random
        key = random.randrange(0, 100000) 
        print(f"Deleting {key}...")
        table.delete_record(key)
    
    for i in range(5):
        import random
        key = random.randrange(0, 100000) 
        print(f"For {key}, records values: {table.get_latest_column_values(key, [1, 1, 1])}")
 

    unittest.main()

#     # print("testing delete record with page ranges")
#     # page_range = PageRange(3, RID_Generator())
#     # rid = page_range.insert_columns([1, 4, 5])
#     # rid2 = page_range.insert_columns([2, 8, 4])
#     # print(page_range.get_latest_column_value(rid, 1))
#     # print(page_range.get_latest_column_value(rid2, 1))
#     # page_range.delete_record(rid)
#     # print(page_range.get_latest_column_value(rid2, 1))

#     print("Messy testing of table:")
#     primary_key_col = 0
#     table = Table('asdfdf', 3, primary_key_col)
#     record = [11, 12, 23]
#     record2 = [90, 21, 4]
#     record3 = [3, 6, 2]
#     record4 = [8, 2, 19]

#     rid = table.insert_record(record)
#     rid2 = table.insert_record(record2)

#     print(table.get_latest_column_values(record[0], [0, 1, 1]))


#     table.update_record(record[primary_key_col], record3)

#     print(table.get_latest_column_values(record3[0], [1, 1, 1]))
#     print(table.index.get_rid(record3[0]))
#     # should be assertion error 
#     #print(table.index.get_rid(record[0]))

# #     for i in range(5):
# #         print(table.page_ranges[-1].get_latest_column_value(1, i))

#     print("Before deletion:")
#     print(table.get_latest_column_values(record3[primary_key_col], [1, 1, 1]))
#     print(table.get_latest_column_values(record2[primary_key_col], [1,1,1]))
#     table.delete_record(record3[primary_key_col])

#     print("After deletion:")
#     print(table.get_latest_column_values(record2[primary_key_col], [1,1,1]))
#     print(table.get_latest_column_values(record3[primary_key_col], [1, 1, 1]))

#     table.delete_record(2)

#     print(table.get_latest_column_values(2, [1, 1, 1]))

#     table.insert_record(record3)
#     table.insert_record(record4)
#     print(table.get_latest_column_values(4, [1, 1, 1]))
#     print(table.get_latest_column_values(5, [1, 1, 1]))

#     table.delete_record(5)
#     print(table.get_latest_column_values(1, [1, 1, 1]))
#     print(table.get_latest_column_values(2, [1, 1, 1]))
#     print(table.get_latest_column_values(4, [1, 1, 1]))
#     print(table.get_latest_column_values(5, [1, 1, 1]))



    


