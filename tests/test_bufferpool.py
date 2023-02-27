import unittest
from unittest import mock
from lstore import (
    PhysicalPage,
    DiskInterface,
    Bufferpool,
)

class TestBufferpool(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.max_bufferpool_pages: int = 2
        self.slot_num: int = PhysicalPage.max_number_of_records // 2
        self.page_id = "page_id_1"
        self.path = "test_path"

    @classmethod
    def tearDownClass(self):
        self.max_bufferpool_pages = None
        self.slot_num = None
        self.page_id = None
        self.path = None

    def test_insert_page_when_bufferpool_empty(self) -> None:
        bufferpool: Bufferpool = Bufferpool(self.max_bufferpool_pages, self.path)
        disk_interface: mock.MagicMock = mock.Mock()
        bufferpool.disk: DiskInterface = disk_interface
        disk_interface.page_exists.return_value = False

        self.assertTrue(bufferpool.insert_page(self.page_id, self.slot_num, 832))
        self.assertEqual(len(bufferpool.physical_pages), 1)

        page_id: PhysicalPage = tuple(bufferpool.physical_pages.keys())[0]
        physical_page: PhysicalPage = tuple(bufferpool.physical_pages.values())[0]

        self.assertEqual(page_id, self.page_id)
        self.assertEqual(physical_page.get_column_value(self.slot_num), 832)
        self.assertEqual(physical_page.is_dirty(), True)
        self.assertEqual(physical_page.can_evict(), True)
    
    def test_insert_page_when_inserting_duplicate_page_in_memory(self) -> None:
        bufferpool: Bufferpool = Bufferpool(self.max_bufferpool_pages, self.path)
        disk_interface: mock.MagicMock = mock.Mock()
        bufferpool.disk: DiskInterface = disk_interface
        disk_interface.page_exists.return_value = False

        self.assertTrue(bufferpool.insert_page(self.page_id, self.slot_num, 832))
        self.assertFalse(bufferpool.insert_page(self.page_id, self.slot_num, 832))

    def test_insert_page_when_inserting_duplicate_page_on_disk(self) -> None:
        bufferpool: Bufferpool = Bufferpool(self.max_bufferpool_pages, self.path)
        disk_interface: mock.MagicMock = mock.Mock()
        bufferpool.disk: DiskInterface = disk_interface
        disk_interface.page_exists.return_value = False

        self.assertTrue(bufferpool.insert_page(self.page_id, self.slot_num, 832))
        physical_page: PhysicalPage = bufferpool.get_page(self.page_id)
        bufferpool._evict_page()
        disk_interface.write_page.assert_called_with(self.page_id, physical_page)

        disk_interface.page_exists.return_value = True
        self.assertFalse(bufferpool.insert_page(self.page_id, self.slot_num, 832))
        disk_interface.page_exists.assert_called_with(self.page_id)

    def test_insert_page_when_bufferpool_full(self) -> None:
        bufferpool: Bufferpool = Bufferpool(self.max_bufferpool_pages, self.path)
        disk_interface: mock.MagicMock = mock.Mock()
        bufferpool.disk: DiskInterface = disk_interface
        disk_interface.page_exists.return_value = False

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
    
    def test_copy_page_when_bufferpool_not_full(self) -> None:
        bufferpool: Bufferpool = Bufferpool(self.max_bufferpool_pages, self.path)
        disk_interface: mock.MagicMock = mock.Mock()
        bufferpool.disk: DiskInterface = disk_interface
        disk_interface.page_exists.return_value = False

        source_phys_page = PhysicalPage()
        source_page_id = "1"
        for i in range(PhysicalPage.max_number_of_records):
            source_phys_page.insert_value(i+1, i)
        bufferpool.physical_pages[source_page_id] = source_phys_page

        dest_page_id = "2"
        bufferpool.copy_page(source_page_id, dest_page_id)
        self.assertTrue(dest_page_id in bufferpool.physical_pages)
        dest_phys_page = bufferpool.physical_pages[dest_page_id]
        for i in range(PhysicalPage.max_number_of_records):
            source_page_val = source_phys_page.get_column_value(i)
            dest_page_val = dest_phys_page.get_column_value(i)
            self.assertEqual(source_page_val, dest_page_val)
    
    def test_copy_page_when_bufferpool_full(self) -> None:
        bufferpool: Bufferpool = Bufferpool(self.max_bufferpool_pages, self.path)
        disk_interface: mock.MagicMock = mock.Mock()
        bufferpool.disk: DiskInterface = disk_interface
        disk_interface.page_exists.return_value = False

        phys_page_id1 = "1"
        phys_page_id2 = "2"
        bufferpool.insert_page(phys_page_id1, self.slot_num, 832)
        bufferpool.insert_page(phys_page_id2, self.slot_num, 833)

        dest_page_id = "3"
        bufferpool.copy_page(phys_page_id2, dest_page_id)
        self.assertTrue(dest_page_id in bufferpool.physical_pages)
        dest_phys_page = bufferpool.physical_pages[dest_page_id]
        col_val = dest_phys_page.get_column_value(self.slot_num)
        self.assertEqual(col_val, 833)

    def test_copy_page_with_existent_destination_page(self) -> None:
        bufferpool: Bufferpool = Bufferpool(self.max_bufferpool_pages, self.path)
        disk_interface: mock.MagicMock = mock.Mock()
        bufferpool.disk: DiskInterface = disk_interface
        disk_interface.page_exists.return_value = False

        source_page_id = "1"
        bufferpool.insert_page(source_page_id, 0, 0)
        self.assertFalse(bufferpool.copy_page(source_page_id, source_page_id))

    def test_get_page_when_page_in_bufferpool(self) -> None:
        bufferpool: Bufferpool = Bufferpool(self.max_bufferpool_pages, self.path)
        disk_interface: mock.MagicMock = mock.Mock()
        bufferpool.disk: DiskInterface = disk_interface
        disk_interface.page_exists.return_value = False

        self.assertTrue(bufferpool.insert_page(self.page_id, self.slot_num, 832))
        self.assertEqual(len(bufferpool.physical_pages), 1)

        physical_page: PhysicalPage = bufferpool.get_page(self.page_id)
        self.assertEqual(physical_page.get_column_value(self.slot_num), 832)
        self.assertEqual(physical_page.is_dirty(), True)
        self.assertEqual(physical_page.can_evict(), True)

    def test_get_page_when_page_not_in_bufferpool(self) -> None:
        bufferpool: Bufferpool = Bufferpool(self.max_bufferpool_pages, self.path)
        disk_interface: mock.MagicMock = mock.Mock()
        bufferpool.disk: DiskInterface = disk_interface
        disk_interface.page_exists.return_value = False

        self.assertTrue(bufferpool.insert_page("page_id_1", self.slot_num, 111))
        physical_page_in_memory: PhysicalPage = bufferpool.get_page("page_id_1")

        self.assertTrue(bufferpool.insert_page("page_id_2", self.slot_num, 222))
        self.assertTrue(bufferpool.insert_page("page_id_3", self.slot_num, 333))

        disk_interface.page_exists.return_value = True
        disk_interface.get_page.return_value = physical_page_in_memory
        physical_page_from_disk: PhysicalPage = bufferpool.get_page("page_id_1")
        disk_interface.page_exists.assert_called_with("page_id_1")

        self.assertEqual(physical_page_in_memory.data, physical_page_from_disk.data)

    def test_get_page_when_page_does_not_exist(self) -> None:
        bufferpool: Bufferpool = Bufferpool(self.max_bufferpool_pages, self.path)
        disk_interface: mock.MagicMock = mock.Mock()
        bufferpool.disk: DiskInterface = disk_interface
        disk_interface.page_exists.return_value = False

        disk_interface.page_exists.return_value = False
        page: PhysicalPage = bufferpool.get_page("non_existent_page_id")
        disk_interface.page_exists.assert_called_with("non_existent_page_id")
        self.assertEqual(page, None)

    def test_evict_all_pages(self) -> None:
        bufferpool: Bufferpool = Bufferpool(self.max_bufferpool_pages, self.path)
        disk_interface: mock.MagicMock = mock.Mock()
        bufferpool.disk: DiskInterface = disk_interface
        disk_interface.page_exists.return_value = False

        self.assertTrue(bufferpool.insert_page(self.page_id, self.slot_num, 832))
        physical_page: PhysicalPage = bufferpool.get_page(self.page_id)
        bufferpool.evict_all_pages()
        disk_interface.write_page.assert_called_with(self.page_id, physical_page)

    def test_evict_page(self) -> None:
        bufferpool: Bufferpool = Bufferpool(self.max_bufferpool_pages, self.path)
        disk_interface: mock.MagicMock = mock.Mock()
        bufferpool.disk: DiskInterface = disk_interface
        disk_interface.page_exists.return_value = False

        self.assertTrue(bufferpool.insert_page("page_id_1", self.slot_num, 111))
        physical_page1: PhysicalPage = bufferpool.get_page("page_id_1")

        self.assertTrue(bufferpool.insert_page("page_id_2", self.slot_num, 222))
        physical_page2: PhysicalPage = bufferpool.get_page("page_id_2")

        bufferpool._evict_page()
        disk_interface.write_page.assert_called_with("page_id_1", physical_page1)

        physical_pages: dict[str,PhysicalPage] = bufferpool.physical_pages
        self.assertEqual(len(physical_pages), 1)
        self.__verify_physical_page_equality(physical_pages["page_id_2"], physical_page2)

    def __verify_physical_page_equality(self, physical_page1: PhysicalPage, physical_page2: PhysicalPage) -> None:
        self.assertEqual(physical_page1.get_data(), physical_page2.get_data())
        self.assertEqual(physical_page1.is_dirty(), physical_page2.is_dirty())
        self.assertEqual(physical_page1.can_evict(), physical_page2.can_evict())

if __name__ == "__main__":
    unittest.main()