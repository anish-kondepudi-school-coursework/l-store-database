import unittest
import os
from lstore import Table, SeedSet
import copy
import numpy as np
import random


class TestSeed(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.table: Table = Table("table1", 5, 0)

    @classmethod
    def tearDownClass(self):
        self.table: Table = None

    def test_adding_record_secondary(self) -> None:
        # creating random array of integers and finding random low and high values
        randnums = np.random.randint(0, 5001, 5000)
        low = random.randint(10, 250)
        high = random.randint(800, 950)
        # removing duplicates from randnums
        randnums = np.unique(randnums)
        # finding all of the numbers between the range manually
        expected_range = []
        for randnum in randnums:
            if low <= randnum <= high:
                expected_range.append(randnum)
        # creating the seed object
        seed = SeedSet(randnums)
        real_range = seed.search_range(low, high)
        # sorting both arrays
        expected_range.sort()
        real_range.sort()
        self.assertEqual(expected_range, real_range)

    def test_remove_record_secondary(self) -> None:
        # creating random array of integers and finding random low and high values
        randnums = np.random.randint(0, 5001, 5000)
        low = random.randint(10, 250)
        high = random.randint(4750, 5000)
        # removing duplicates from randnums, and then removing random number
        randnums = np.unique(randnums)
        # creating the seed object
        seed = SeedSet(randnums)
        # removing random number from seed
        seed.remove(randnums[0])
        # removing random number from randnums
        randnums = np.delete(randnums, 0)
        # finding all of the numbers between the range manually
        expected_range = []
        for randnum in randnums:
            if low <= randnum <= high:
                expected_range.append(randnum)
        real_range = seed.search_range(low, high)
        # sorting both arrays
        expected_range.sort()
        real_range.sort()
        self.assertEqual(expected_range, real_range)

    def test_remove_duplicate(self) -> None:
        # creating random array of integers and finding random low and high values
        randnums = np.random.randint(0, 5001, 5000)
        # removing duplicates from randnums, and then removing random number
        randnums = np.unique(randnums)
        # creating the seed object
        seed = SeedSet(randnums)
        # removing random number from seed
        seed.remove(randnums[0])
        value = seed.remove(randnums[0])
        self.assertEqual(value, None)

    def test_remove_duplicate_fixed_largest(self) -> None:
        # creating random array of integers and finding random low and high values
        numbers = [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]
        seed = SeedSet(numbers)
        # removing random number from seed
        seed.remove(20)
        value = seed.remove(20)
        self.assertEqual(value, None)

    def test_remove_duplicate_fixed_smallest(self) -> None:
        # creating random array of integers and finding random low and high values
        numbers = [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]
        seed = SeedSet(numbers)
        # removing random number from seed
        seed.remove(2)
        value = seed.remove(2)
        self.assertEqual(value, None)

    def test_remove_duplicate_fixed_mid(self) -> None:
        # creating array of integers and finding random low and high values
        numbers = [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]
        seed = SeedSet(numbers)
        # removing random number from seed
        seed.remove(10)
        value = seed.remove(10)
        self.assertEqual(value, None)

    def test_remove_duplicate_fixed_mid(self) -> None:
        # creating array of integers and finding random low and high values
        numbers = [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]
        seed = SeedSet(numbers)
        # removing random number from seed
        seed.remove(10)
        value = seed.remove(10)
        self.assertEqual(value, None)

    def test_remove_non_existant(self) -> None:
        # creating array of integers and finding random low and high values
        numbers = [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]
        seed = SeedSet(numbers)
        # removing random number from seed
        seed.remove(21)
        value = seed.remove(10)
        self.assertEqual(value, None)

    def test_add_remove_value(self) -> None:
        # creating array of integers and finding random low and high values
        numbers = [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]
        seed = SeedSet(numbers)
        # removing random number from seed
        seed.add(21)
        seed.remove(21)

if __name__ == "__main__":
    unittest.main()
