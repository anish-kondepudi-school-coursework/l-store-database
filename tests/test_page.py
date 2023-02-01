import unittest
from lstore import PhysicalPage, PHYSICAL_PAGE_SIZE, ATTRIBUTE_SIZE

class TestPageMethods(unittest.TestCase):

    def test_insert_value_for_valid_offsets(self) -> None:
        page: PhysicalPage = PhysicalPage()
        for offset in range(PhysicalPage.max_number_of_records):
            self.assertTrue(page.insert_value(123, offset))

    def test_insert_value_for_invalid_offsets(self) -> None:
        page: PhysicalPage = PhysicalPage()
        self.assertFalse(page.insert_value(123, PhysicalPage.max_number_of_records))
        self.assertFalse(page.insert_value(123, -1))

    def test_insert_value_for_invalid_integers(self) -> None:
        page: PhysicalPage = PhysicalPage()

        offset: int = ATTRIBUTE_SIZE
        values_to_insert: list[int] = [-1 * 2**63 -1, 2**63]

        for value_to_insert in values_to_insert:
            with self.assertRaises(OverflowError):
                page.insert_value(value_to_insert, offset)

    def test_get_column_value_for_valid_integers(self) -> None:
        page: PhysicalPage = PhysicalPage()

        offset: int = ATTRIBUTE_SIZE
        values_to_insert: list[int] = [-1 * 2**63, -10, 0, 10, 2**63 - 1]

        for value_to_insert in values_to_insert:
            self.assertTrue(page.insert_value(value_to_insert, offset))
            column_value: int = page.get_column_value(offset)
            self.assertEquals(
                first=column_value,
                second=value_to_insert,
                msg=f"Expected: {value_to_insert} Received: {column_value}")


if __name__ == '__main__':
    unittest.main()