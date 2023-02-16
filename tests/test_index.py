import unittest
from lstore import Index, Table


class TestIndex(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.table: Table = Table("table1", 3, 0)

    @classmethod
    def tearDownClass(self):
        self.table: Table = None

    def test_add_duplicate_key(self) -> None:
        index: Index = Index(self.table)
        repeated_key: int = 1
        index.add_key_rid(repeated_key, 2)
        with self.assertRaises(AssertionError):
            index.add_key_rid(repeated_key, 3)

    def test_get_existent_key(self) -> None:
        index: Index = Index(self.table)
        key: int = 1
        rid: int = 2
        index.add_key_rid(key, rid)
        given_rid: int = index.get_rid(key)
        self.assertEqual(given_rid, rid)

    def test_get_nonexistent_key(self) -> None:
        index: Index = Index(self.table)
        with self.assertRaises(AssertionError):
            index.get_rid(1)

    def test_key_exists(self) -> None:
        index: Index = Index(self.table)
        key: int = 1
        self.assertFalse(index.key_exists(key))
        index.add_key_rid(key, 2)
        self.assertTrue(index.key_exists(key))

    def test_delete_existent_key(self) -> None:
        index: Index = Index(self.table)
        key: int = 1
        rid: int = 2
        index.add_key_rid(key, rid)
        index.delete_key(key)
        with self.assertRaises(AssertionError):
            index.get_rid(key)

    def test_delete_nonexistent_key(self) -> None:
        index: Index = Index(self.table)
        with self.assertRaises(AssertionError):
            index.delete_key(1)


if __name__ == "__main__":
    unittest.main()
