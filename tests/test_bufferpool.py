import unittest
from unittest.mock import MagicMock, mock_open
from unittest.mock import patch
from io import BytesIO
from lstore import (
    PhysicalPage,
    DiskInterface,
    Bufferpool,
)
import unittest
from unittest import mock
from lstore import PageRange, Table

class TestBufferpool(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.max_bufferpool_pages: int = 2
        self.slot_num: int = PhysicalPage.max_number_of_records // 2
        self.page_id = "page_id_1"

    @classmethod
    def tearDownClass(self):
        self.max_bufferpool_pages = None
        self.slot_num = None
        self.page_id = None

    def test_insert_page(self) -> None:
        bufferpool: Bufferpool = Bufferpool(self.max_bufferpool_pages)
        disk_interface: mock.MagicMock = mock.Mock()
        bufferpool.disk: DiskInterface = disk_interface

        slot_num: int = PhysicalPage.max_number_of_records // 2
        bufferpool.insert_page(self.page_id, slot_num, 832)

        self.assertEqual(len(bufferpool.physical_pages), 1)

        page_id: PhysicalPage = tuple(bufferpool.physical_pages.keys())[0]
        physical_page: PhysicalPage = tuple(bufferpool.physical_pages.values())[0]

        self.assertEqual(page_id, self.page_id)
        self.assertEqual(physical_page.get_column_value(slot_num), 832)
        self.assertEqual(physical_page.is_dirty(), True)
        self.assertEqual(physical_page.can_evict(), True)


    def test_get_page(self) -> None:
        bufferpool: Bufferpool = Bufferpool(self.max_bufferpool_pages)
        disk_interface: mock.MagicMock = mock.Mock()
        bufferpool.disk: DiskInterface = disk_interface

        bufferpool.insert_page(self.page_id, self.slot_num, 832)

        self.assertEqual(len(bufferpool.physical_pages), 1)

        physical_page: PhysicalPage = bufferpool.get_page(self.page_id)

        self.assertEqual(physical_page.get_column_value(self.slot_num), 832)
        self.assertEqual(physical_page.is_dirty(), True)
        self.assertEqual(physical_page.can_evict(), True)

    def test_evict_all_pages(self) -> None:

        bufferpool: Bufferpool = Bufferpool(self.max_bufferpool_pages)
        disk_interface: mock.MagicMock = mock.Mock()
        bufferpool.disk: DiskInterface = disk_interface

        bufferpool.insert_page(self.page_id, self.slot_num, 832)
        physical_page: PhysicalPage = bufferpool.get_page(self.page_id)
        bufferpool.evict_all_pages()
        disk_interface.write_page.assert_called_with(self.page_id, physical_page)

if __name__ == "__main__":
    unittest.main()