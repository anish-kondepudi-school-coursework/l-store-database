import unittest
from lstore import RID_Generator, PhysicalPage

class TestRidGenerator(unittest.TestCase):
    def test_get_base_rids(self) -> None:
        PhysicalPage.max_number_of_records = 5
        rid_generator = RID_Generator()
        self.assertListEqual(rid_generator.get_base_rids(), [5, 4, 3, 2, 1])
        self.assertListEqual(rid_generator.get_base_rids(), [10, 9, 8, 7, 6])
        self.assertListEqual(rid_generator.get_base_rids(), [15, 14, 13, 12, 11])

    def test_get_tail_rids(self) -> None:
        PhysicalPage.max_number_of_records = 5
        rid_generator = RID_Generator()
        self.assertListEqual(rid_generator.get_tail_rids(), [-5, -4, -3, -2, -1])
        self.assertListEqual(rid_generator.get_tail_rids(), [-10, -9, -8, -7, -6])
        self.assertListEqual(rid_generator.get_tail_rids(), [-15, -14, -13, -12, -11])

    def test_get_base_rid_to_starting_rid(self) -> None:
        PhysicalPage.max_number_of_records = 5
        rid_generator = RID_Generator()
        for rid in range(1, 6):
            self.assertEqual(rid_generator.base_rid_to_starting_rid(rid), 1)
        for rid in range(6, 11):
            self.assertEqual(rid_generator.base_rid_to_starting_rid(rid), 6)
        for rid in range(11, 16):
            self.assertEqual(rid_generator.base_rid_to_starting_rid(rid), 11)
    
    def test_get_tail_rid_to_starting_rid(self) -> None:
        PhysicalPage.max_number_of_records = 5
        rid_generator = RID_Generator()
        for rid in range(-5, 0):
            self.assertEqual(rid_generator.tail_rid_to_starting_rid(rid), -1)
        for rid in range(-10, -5):
            self.assertEqual(rid_generator.tail_rid_to_starting_rid(rid), -6)
        for rid in range(-15, -10):
            self.assertEqual(rid_generator.tail_rid_to_starting_rid(rid), -11)

if __name__ == "__main__":
    unittest.main()