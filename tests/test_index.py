import unittest
# todo: order imports correctly
from lstore import (
    Table,
    Index
)

class TestIndex(unittest.TestCase):
    
    @classmethod
    def setUpClass(self):
        pass

    @classmethod
    def tearDownClass(self):
        pass

    def test_get_existent_key(self) -> None:
        table = Table('asfdsf', 3, 0)
        index = Index(table)
        key = 1
        rid = 2
        index.add_key_rid(key, rid)
        self.assertEqual(index.get_rid(key), rid)
    
    def test_get_nonexistent_key(self) -> None:
        table = Table('asfdsf', 3, 0)
        index = Index(table)
        with self.assertRaises(AssertionError):
            index.get_rid(1)

    def test_delete_existent_key(self):
        table = Table('asfdsf', 3, 0)
        index = Index(table)
        key = 1
        rid = 2
        index.add_key_rid(key, rid)
        index.delete_key(key)
        with self.assertRaises(AssertionError):
            index.get_rid(key)

    def test_delete_nonexistent_key(self):
        table = Table('asfdsf', 3, 0)
        index = Index(table)
        with self.assertRaises(AssertionError):
            index.delete_key(1)

if __name__ == '__main__':
    unittest.main()
