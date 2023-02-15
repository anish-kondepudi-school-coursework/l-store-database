import unittest
from lstore import BasePage, RID_Generator, PageDirectory, INVALID_RID, INVALID_OFFSET


class TestPageDirectory(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.rid_generator = RID_Generator()
        self.base_rid: int = 24
        self.base_page: BasePage = BasePage(5, self.rid_generator)
        self.base_record_offset: int = 16

    @classmethod
    def tearDownClass(self):
        self.rid_generator = None
        self.base_rid: int = None
        self.base_page: BasePage = None
        self.base_record_offset: int = None

    def test_insert_page_when_page_not_previously_inserted(self) -> None:
        page_directory: PageDirectory = PageDirectory()
        try:
            page_directory.insert_page(
                self.base_rid, self.base_page, self.base_record_offset
            )
        except:
            self.fail("Exception raises on insert_page unexpectedly.")

    def test_insert_page_when_page_previously_inserted(self) -> None:
        page_directory: PageDirectory = PageDirectory()
        page_directory.insert_page(
            self.base_rid, self.base_page, self.base_record_offset
        )
        with self.assertRaises(AssertionError):
            page_directory.insert_page(
                self.base_rid, self.base_page, self.base_record_offset
            )

    def test_get_page_when_no_pages_inserted(self) -> None:
        page_directory: PageDirectory = PageDirectory()
        page_details: tuple[BasePage, int] = page_directory.get_page(7)
        self.assertTupleEqual(
            tuple1=page_details,
            tuple2=(None, INVALID_OFFSET),
            msg=f"Received valid page details. Expected: {(INVALID_RID, INVALID_OFFSET)} Received: {page_details}",
        )

    def test_get_page_when_page_inserted(self) -> None:
        page_directory: PageDirectory = PageDirectory()
        page_directory.insert_page(
            self.base_rid, self.base_page, self.base_record_offset
        )
        page_details: tuple[BasePage, int] = page_directory.get_page(self.base_rid)
        self.assertTupleEqual(
            tuple1=page_details,
            tuple2=(self.base_page, self.base_record_offset),
            msg=f"Received valid page details. Expected: {(INVALID_RID, INVALID_OFFSET)} Received: {page_details}",
        )

    def test_delete_page_when_page_does_not_exist(self) -> None:
        page_directory: PageDirectory = PageDirectory()
        with self.assertRaises(AssertionError):
            page_directory.delete_page(self.base_rid)

    def test_delete_page_when_page_exists(self) -> None:
        page_directory: PageDirectory = PageDirectory()
        page_directory.insert_page(
            self.base_rid, self.base_page, self.base_record_offset
        )
        try:
            page_directory.delete_page(self.base_rid)
        except:
            self.fail("Exception raises on insert_page unexpectedly.")
        page_details: tuple[BasePage, int] = page_directory.get_page(self.base_rid)
        self.assertTupleEqual(
            tuple1=page_details,
            tuple2=(None, INVALID_OFFSET),
            msg=f"Received valid page details. Expected: {(INVALID_RID, INVALID_OFFSET)} Received: {page_details}",
        )


if __name__ == "__main__":
    unittest.main()
