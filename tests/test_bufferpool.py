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

    def test_insert_page_when_bufferpool_empty(self) -> None:
        bufferpool: Bufferpool = Bufferpool(self.max_bufferpool_pages)
        disk_interface: mock.MagicMock = mock.Mock()
        bufferpool.disk: DiskInterface = disk_interface

        self.assertTrue(bufferpool.insert_page(self.page_id, self.slot_num, 832))
        self.assertEqual(len(bufferpool.physical_pages), 1)

        page_id: PhysicalPage = tuple(bufferpool.physical_pages.keys())[0]
        physical_page: PhysicalPage = tuple(bufferpool.physical_pages.values())[0]

        self.assertEqual(page_id, self.page_id)
        self.assertEqual(physical_page.get_column_value(self.slot_num), 832)
        self.assertEqual(physical_page.is_dirty(), True)
        self.assertEqual(physical_page.can_evict(), True)

    def test_insert_page_when_inserting_duplicate_page_in_memory(self) -> None:
        bufferpool: Bufferpool = Bufferpool(self.max_bufferpool_pages)
        disk_interface: mock.MagicMock = mock.Mock()
        bufferpool.disk: DiskInterface = disk_interface

        self.assertTrue(bufferpool.insert_page(self.page_id, self.slot_num, 832))
        self.assertFalse(bufferpool.insert_page(self.page_id, self.slot_num, 832))

    def test_insert_page_when_bufferpool_full(self) -> None:

        bufferpool: Bufferpool = Bufferpool(self.max_bufferpool_pages)
        disk_interface: mock.MagicMock = mock.Mock()
        bufferpool.disk: DiskInterface = disk_interface

        self.assertTrue(bufferpool.insert_page("page_id_1", self.slot_num, 111))
        physical_page1: PhysicalPage = bufferpool.get_page("page_id_1")

        self.assertTrue(bufferpool.insert_page("page_id_2", self.slot_num, 222))
        physical_page2: PhysicalPage = bufferpool.get_page("page_id_2")

        self.assertTrue(bufferpool.insert_page("page_id_3", self.slot_num, 333))
        physical_page3: PhysicalPage = bufferpool.get_page("page_id_3")

        disk_interface.write_page.assert_called_with("page_id_1", physical_page1)

        physical_pages: dict[str,PhysicalPage] = bufferpool.physical_pages
        self.assertEqual(len(physical_pages), 2)

        self.assertTrue("page_id_1" not in physical_pages)
        self.assertTrue("page_id_2" in physical_pages)
        self.assertTrue("page_id_3" in physical_pages)
        self.assertTrue("page_id_3" in physical_pages)

        self.__verify_physical_page_equality(physical_pages["page_id_2"], physical_page2)
        self.__verify_physical_page_equality(physical_pages["page_id_3"], physical_page3)

    def test_get_page_when_page_in_bufferpool(self) -> None:
        bufferpool: Bufferpool = Bufferpool(self.max_bufferpool_pages)
        disk_interface: mock.MagicMock = mock.Mock()
        bufferpool.disk: DiskInterface = disk_interface

        self.assertTrue(bufferpool.insert_page(self.page_id, self.slot_num, 832))
        self.assertEqual(len(bufferpool.physical_pages), 1)

        physical_page: PhysicalPage = bufferpool.get_page(self.page_id)
        self.assertEqual(physical_page.get_column_value(self.slot_num), 832)
        self.assertEqual(physical_page.is_dirty(), True)
        self.assertEqual(physical_page.can_evict(), True)


    def test_get_page_when_page_not_in_bufferpool(self) -> None:
        bufferpool: Bufferpool = Bufferpool(self.max_bufferpool_pages)
        disk_interface: mock.MagicMock = mock.Mock()
        bufferpool.disk: DiskInterface = disk_interface

        self.assertTrue(bufferpool.insert_page("page_id_1", self.slot_num, 111))
        physical_page_in_memory: PhysicalPage = bufferpool.get_page("page_id_1")

        self.assertTrue(bufferpool.insert_page("page_id_2", self.slot_num, 222))
        self.assertTrue(bufferpool.insert_page("page_id_3", self.slot_num, 333))

        disk_interface.get_page.return_value = physical_page_in_memory
        physical_page_from_disk: PhysicalPage = bufferpool.get_page("page_id_1")

        self.assertEqual(physical_page_in_memory.data, physical_page_from_disk.data)

    def test_get_page_when_page_does_not_exist(self) -> None:
        bufferpool: Bufferpool = Bufferpool(self.max_bufferpool_pages)
        disk_interface: mock.MagicMock = mock.Mock()
        bufferpool.disk: DiskInterface = disk_interface

        disk_interface.get_page.side_effect = FileNotFoundError
        page: PhysicalPage = bufferpool.get_page("non_existent_page_id")
        disk_interface.get_page.assert_called_with("non_existent_page_id")
        self.assertEqual(page, None)

    def test_evict_all_pages(self) -> None:
        bufferpool: Bufferpool = Bufferpool(self.max_bufferpool_pages)
        disk_interface: mock.MagicMock = mock.Mock()
        bufferpool.disk: DiskInterface = disk_interface

        self.assertTrue(bufferpool.insert_page(self.page_id, self.slot_num, 832))
        physical_page: PhysicalPage = bufferpool.get_page(self.page_id)
        bufferpool.evict_all_pages()
        disk_interface.write_page.assert_called_with(self.page_id, physical_page)

    def __verify_physical_page_equality(self, physical_page1: PhysicalPage, physical_page2: PhysicalPage) -> None:
        self.assertEqual(physical_page1.get_data(), physical_page2.get_data())
        self.assertEqual(physical_page1.is_dirty(), physical_page2.is_dirty())
        self.assertEqual(physical_page1.can_evict(), physical_page2.can_evict())

if __name__ == "__main__":
    unittest.main()