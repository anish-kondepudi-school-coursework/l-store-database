import unittest
from unittest import mock
from lstore import (
    Bufferpool,
    DiskInterface,
    PhysicalPage,
    LogicalPage,
    BasePage,
    get_copy_of_base_page,
    TailPage,
    ATTRIBUTE_SIZE,
    INDIRECTION_COLUMN,
    INVALID_SLOT_NUM,
    INVALID_RID,
    SCHEMA_ENCODING_COLUMN,
    RID_Generator,
    NUM_METADATA_COLS,
)
from abc import ABC

class TestPhysPage(unittest.TestCase):
    def test_insert_value_for_valid_offsets(self) -> None:
        page: PhysicalPage = PhysicalPage()
        orig_time_stamp = page.get_timestamp()
        for offset in range(PhysicalPage.max_number_of_records):
            success = page.insert_value(123, offset)
            self.assertTrue(success)
            self.assertTrue(page.is_dirty())
            self.assertGreater(page.timestamp, orig_time_stamp)

    def test_insert_value_for_INVALID_SLOT_NUMs(self) -> None:
        page: PhysicalPage = PhysicalPage()
        orig_time_stamp = page.get_timestamp()
        
        for INVALID_SLOT_NUM in [-1, PhysicalPage.max_number_of_records]:
            success = page.insert_value(123, INVALID_SLOT_NUM)
            self.assertFalse(success)
            self.assertFalse(page.is_dirty())
            self.assertEqual(orig_time_stamp, page.timestamp)

    def test_insert_value_for_invalid_integers(self) -> None:
        page: PhysicalPage = PhysicalPage()
        orig_time_stamp = page.get_timestamp()

        offset: int = ATTRIBUTE_SIZE
        values_to_insert: list[int] = [-1 * 2**63 - 1, 2**63]

        for value_to_insert in values_to_insert:
            with self.assertRaises(OverflowError):
                page.insert_value(value_to_insert, offset)
            self.assertFalse(page.is_dirty())
            self.assertEqual(orig_time_stamp, page.get_timestamp())
        
    def test_get_column_value_for_valid_integers(self) -> None:
        page: PhysicalPage = PhysicalPage()
        orig_time_stamp = page.timestamp

        offset: int = ATTRIBUTE_SIZE
        values_to_insert: list[int] = [-1 * 2**63, -10, 0, 10, 2**63 - 1]

        for value_to_insert in values_to_insert:
            self.assertTrue(page.insert_value(value_to_insert, offset))
            column_value: int = page.get_column_value(offset)
            self.assertEqual(
                first=column_value,
                second=value_to_insert,
                msg=f"Expected: {value_to_insert} Received: {column_value}",
            )
            self.assertGreater(page.get_timestamp(), orig_time_stamp)

    def test_pin_unpin(self) -> None:
        page: PhysicalPage = PhysicalPage()
        self.assertTrue(page.can_evict())
        page.pin_page()
        self.assertFalse(page.can_evict())
        page.unpin_page()
        self.assertTrue(page.can_evict())

class LogicalPageTests(ABC):
    @classmethod
    def setUpClass(self):
        self.num_cols: int = 3
        self.values_to_insert: list[int] = [0, 4, 10]
        self.rid_generator = RID_Generator()
        self.max_bufferpool_pages: int = 200
        self.path = ""
        self.bufferpool: Bufferpool = Bufferpool(self.max_bufferpool_pages, self.path)
        disk_interface: mock.MagicMock = mock.Mock()
        self.bufferpool.disk: DiskInterface = disk_interface
        disk_interface.page_exists.return_value = False

    @classmethod
    def tearDownClass(self):
        self.num_cols: int = None
        self.values_to_insert: list[int] = None
        self.rid_generator = None

    def init_page(self) -> LogicalPage:
        return LogicalPage(self.num_cols, self.rid_generator)

    def test_insert_record(self) -> None:
        page: LogicalPage = self.init_page()
        rid, offset = page.insert_record(self.values_to_insert)
        self.assertNotEqual(rid, INVALID_RID)
        self.assertNotEqual(offset, INVALID_SLOT_NUM)
        for ind in range(self.num_cols):
            given_col = page.bufferpool.get_page(page.page_ids[ind]).get_column_value(offset)
            exp_col = self.values_to_insert[ind]
            self.assertEqual(given_col, exp_col)

    def test_insert_record_valid_rids(self) -> None:
        max_num_inserts = 100
        page: LogicalPage = self.init_page()
        for _ in range(max_num_inserts):
            rid, _ = page.insert_record(self.values_to_insert)
            self.check_rid_is_valid(rid)

    def check_rid_is_valid(self, rid) -> None:
        self.assertNotEqual(rid, INVALID_RID)
    
    def test_insert_record_into_full_page(self) -> None:
        page: LogicalPage = self.init_page()
        for _ in range(PhysicalPage.max_number_of_records):
            page.insert_record(self.values_to_insert)
        rid, offset = page.insert_record(self.values_to_insert)
        self.assertEqual(rid, INVALID_RID)
        self.assertEqual(offset, INVALID_SLOT_NUM)
    
    def test_is_full(self) -> None:
        page: LogicalPage = self.init_page()
        for _ in range(PhysicalPage.max_number_of_records):
            self.assertFalse(page.is_full())
            page.insert_record(self.values_to_insert)
        self.assertTrue(page.is_full())
    
    def test_get_column(self):
        page: LogicalPage = self.init_page()
        _, offset = page.insert_record(self.values_to_insert)
        for ind in range(self.num_cols):
            given_col = page.get_column_of_record(ind, offset)
            exp_col = self.values_to_insert[ind]
            self.assertEqual(given_col, exp_col)
        for ind in (INDIRECTION_COLUMN, SCHEMA_ENCODING_COLUMN):
            given_col = page.get_column_of_record(ind, offset)
            exp_col = self.values_to_insert[ind]
            self.assertEqual(given_col, exp_col)
    
    def test_get_column_invalid_index(self):
        page: LogicalPage = self.init_page()
        _, offset = page.insert_record(self.values_to_insert)
        with self.assertRaises(AssertionError):
            page.get_column_of_record(self.num_cols + 1, offset)
        with self.assertRaises(AssertionError):
            page.get_column_of_record(-3, offset)

    def test_update_indir(self) -> None:
        page: LogicalPage = self.init_page()
        _, slot_num = page.insert_record(self.values_to_insert)
        new_indir_val = 5
        res = page.update_indir_of_record(new_indir_val, slot_num)
        self.assertTrue(res)
        given_indir_val = page.bufferpool.get_page(page.page_ids[INDIRECTION_COLUMN]).get_column_value(slot_num)
        self.assertEqual(given_indir_val, new_indir_val)
    
class TestBasePage(LogicalPageTests, unittest.TestCase):
    def init_page(self) -> BasePage:
        return BasePage("", self.num_cols, self.bufferpool, self.rid_generator)
    
    def test_update_record(self) -> None:
        page: BasePage = self.init_page()
        new_cols = [i+1 for i in range(self.num_cols)]
        _, slot_num = page.insert_record(self.values_to_insert)
        success = page.update_record(new_cols, slot_num)
        self.assertTrue(success)
        for ind in range(self.num_cols):
            given_col = page.bufferpool.get_page(page.page_ids[ind]).get_column_value(slot_num)
            exp_col = new_cols[ind]
            self.assertEqual(given_col, exp_col)

    def test_get_copy_page_ids(self) -> None:
        page: BasePage = self.init_page()
        page.insert_record(self.values_to_insert)
        copied_page: BasePage = get_copy_of_base_page(page)
        for i in range(self.num_cols - NUM_METADATA_COLS):
            self.assertNotEqual(page.page_ids[i], copied_page.page_ids[i])
        for i in range(self.num_cols - NUM_METADATA_COLS, self.num_cols):
            self.assertEqual(page.page_ids[i], copied_page.page_ids[i])

    def test_get_copy_after_insert(self) -> None:
        page: BasePage = self.init_page()
        _, slot_num = page.insert_record(self.values_to_insert)
        copied_page: BasePage = get_copy_of_base_page(page)
        for ind in range(self.num_cols):
            given_col = copied_page.bufferpool.get_page(copied_page.page_ids[ind]).get_column_value(slot_num)
            exp_col = self.values_to_insert[ind]
            self.assertEqual(given_col, exp_col)

    def test_get_copy_update_record(self) -> None:
        page: BasePage = self.init_page()
        _, slot_num = page.insert_record(self.values_to_insert)
        copied_page: BasePage = get_copy_of_base_page(page)
        new_cols = [i+1 for i in range(self.num_cols)]
        copied_page.update_record(new_cols, slot_num)
        ''' The original records data columns should not be changed '''
        for ind in range(self.num_cols - NUM_METADATA_COLS):
            orig_page_col = page.bufferpool.get_page(page.page_ids[ind]).get_column_value(slot_num)
            copied_page_col = copied_page.bufferpool.get_page(copied_page.page_ids[ind]).get_column_value(slot_num)
            self.assertNotEqual(orig_page_col, copied_page_col)
        ''' The original records metadata columns should be changed s'''
        for ind in range(self.num_cols - NUM_METADATA_COLS, self.num_cols):
            orig_page_col = page.bufferpool.get_page(page.page_ids[ind]).get_column_value(slot_num)
            copied_page_col = copied_page.bufferpool.get_page(copied_page.page_ids[ind]).get_column_value(slot_num)
            self.assertEqual(orig_page_col, copied_page_col)
    
    def check_rid_is_valid(self, rid) -> None:
        self.assertGreaterEqual(rid, 1)


class TestTailPage(LogicalPageTests, unittest.TestCase):
    def init_page(self) -> TailPage:
        return TailPage("", self.num_cols, self.bufferpool, self.rid_generator)
    
    def check_rid_is_valid(self, rid) -> None:
        self.assertLessEqual(rid, -1)

if __name__ == "__main__":
    unittest.main()
    # buff = Bufferpool(16, "")
    # bp = BasePage("asf", 5, buff, RID_Generator())
    # bp.insert_record([1, 2, 3, 0, 0])
    # buff.evict_all_pages()
    # print("Col: ", bp.get_column_of_record(0, 0))
    # print("Col: ", bp.get_column_of_record(1, 0))
    # print("Col: ", bp.get_column_of_record(2, 0))
    # print("Col: ", bp.get_column_of_record(3, 0))
    # print("Col: ", bp.get_column_of_record(4, 0))
    
    # bp.update_indir_of_record(12, 0)
    # buff.evict_all_pages()
    # print("Col: ", bp.get_column_of_record(0, 0))
    # print("Col: ", bp.get_column_of_record(1, 0))
    # print("Col: ", bp.get_column_of_record(2, 0))
    # print("Col: ", bp.get_column_of_record(3, 0))
    # print("Col: ", bp.get_column_of_record(4, 0))

    # buff.evict_all_pages()
    # bp.update_record([5, 6, 3, 5, 4], 0)
    # buff.evict_all_pages()
    # print("Col: ", bp.get_column_of_record(0, 0))
    # print("Col: ", bp.get_column_of_record(1, 0))
    # print("Col: ", bp.get_column_of_record(2, 0))
    # print("Col: ", bp.get_column_of_record(3, 0))
    # print("Col: ", bp.get_column_of_record(4, 0))

    # from lstore.page import get_copy_of_base_page
    # bp2 = get_copy_of_base_page(bp)
    # print(bp2.merge_iteration, bp.merge_iteration)
    # print(bp2.page_ids, bp.page_ids)
    # print("Col: ", bp2.get_column_of_record(0, 511))
    # print("Col: ", bp2.get_column_of_record(1, 511))
    # print("Col: ", bp2.get_column_of_record(2, 511))
    # print("Col: ", bp2.get_column_of_record(3, 511))
    # print("Col: ", bp2.get_column_of_record(4, 511))

    # bp2.update_record([1, 2, 2, 3, 4], 511)
    # print("Col: ", bp2.get_column_of_record(0, 511))
    # print("Col: ", bp2.get_column_of_record(1, 511))
    # print("Col: ", bp2.get_column_of_record(2, 511))
    # print("Col: ", bp2.get_column_of_record(3, 511))
    # print("Col: ", bp2.get_column_of_record(4, 511))

    # #buff.evict_all_pages()
    # print("Previous:")
    # print("Col: ", bp.get_column_of_record(0, 511))
    # print("Col: ", bp.get_column_of_record(1, 511))
    # print("Col: ", bp.get_column_of_record(2, 511))
    # print("Col: ", bp.get_column_of_record(3, 511))
    # print("Col: ", bp.get_column_of_record(4, 511))

    '''
        for tests of get copy:
            make sure that if the copied base page updates the indirection, then the original base page
            can see that update, but that if the copied base page updates a data column in the table,
            the original base page DOESN't see that change 
    '''

    #print(buff.get_page("asf_1_0").get_column_value(0))
    #unittest.main()
