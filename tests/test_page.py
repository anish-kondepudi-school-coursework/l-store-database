import unittest
from lstore import PhysicalPage, LogicalPage, BasePage, TailPage, ATTRIBUTE_SIZE, INDIRECTION_COLUMN, INVALID_OFFSET


class TestPhysPage(unittest.TestCase):

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
            self.assertEqual(
                first=column_value,
                second=value_to_insert,
                msg=f"Expected: {value_to_insert} Received: {column_value}")

class TestLogicalPage(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.num_cols: int = 3
        self.values_to_insert: list[int] = [9, 4, 10]

    @classmethod
    def tearDownClass(self):
        self.num_cols: int = None
        self.values_to_insert: list[int] = None

    def test_insert_record(self) -> None:
        page: LogicalPage = LogicalPage(self.num_cols)
        offset = page.insert_record(self.values_to_insert)
        assert offset != INVALID_OFFSET
        for ind in range(self.num_cols):
            assert page.phys_pages[ind].get_column_value(offset) == self.values_to_insert[ind]

    def test_insert_record_into_full_page(self) -> None:
        page: LogicalPage = LogicalPage(self.num_cols)
        for _ in range(PhysicalPage.max_number_of_records):
            page.insert_record(self.values_to_insert)
        offset = page.insert_record(self.values_to_insert)
        assert offset == INVALID_OFFSET

    def test_is_full(self) -> None:
        page: LogicalPage = LogicalPage(self.num_cols)
        for _ in range(PhysicalPage.max_number_of_records):
            assert not page.is_full()
            page.insert_record(self.values_to_insert)
        assert page.is_full()

class TestBasePage(TestLogicalPage):
    
    def test_update_indir(self) -> None:
        page: BasePage = BasePage(self.num_cols)
        offset = page.insert_record(self.values_to_insert)
        new_indir_val = 5
        assert page.update_indir_of_record(new_indir_val, offset)
        assert page.phys_pages[INDIRECTION_COLUMN].get_column_value(offset) == new_indir_val
    
class TestTailPage(TestLogicalPage):

    def test_get_column(self):
        page: TailPage = TailPage(self.num_cols)
        offset = page.insert_record(self.values_to_insert)
        for ind in range(self.num_cols):
            assert page.get_column_of_record(ind, offset) == self.values_to_insert[ind]
        
    def test_get_column_invalid_index(self):
        page: TailPage = TailPage(self.num_cols)
        offset = page.insert_record(self.values_to_insert)
        with self.assertRaises(AssertionError):
            page.get_column_of_record(self.num_cols+1, offset)
        with self.assertRaises(AssertionError):
            page.get_column_of_record(-1, offset)
        

if __name__ == '__main__':
    unittest.main()