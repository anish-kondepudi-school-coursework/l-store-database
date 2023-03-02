import unittest
from unittest.mock import MagicMock, mock_open
from unittest.mock import patch
from io import BytesIO
from lstore import (
    PhysicalPage,
    DiskInterface,
    PHYSICAL_PAGE_SIZE,
)
import zlib

class TestDiskInterface(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.path_of_file = "/something/somethingelse"
        self.file_name = "random_file"

    @classmethod
    def tearDownClass(self):
        self.path_of_file = None
        self.file_name = None

    @patch("os.path.isfile")
    def test_page_exists(self, isfile) -> None:
        disk_interface = DiskInterface(self.path_of_file)
        val = disk_interface.page_exists(self.file_name)
        isfile.assert_called_once_with(f"{self.path_of_file}/{self.file_name}")

    @patch("builtins.open")
    def test_write_page(self, mock_open) -> None:
        disk_interface = DiskInterface(self.path_of_file)
        p = PhysicalPage()
        p.insert_value(100, 1)
        mock_file = BytesIO()
        mock_open().__enter__ = MagicMock(return_value=mock_file)
        disk_interface.write_page("file", p)

        mock_file.seek(0)
        mock_open.assert_called_with(f"{self.path_of_file}/file", "wb")
        self.assertEqual(zlib.decompress(mock_file.read()), p.get_data())

    @patch("builtins.open")
    def test_get_page(self, mock_open) -> None:
        disk_interface = DiskInterface(self.path_of_file)
        data = bytearray(PHYSICAL_PAGE_SIZE)
        data[0:5] = b'12345'
        mock_file = BytesIO(zlib.compress(data))
        mock_open().__enter__ = MagicMock(return_value=mock_file)
        page : PhysicalPage = disk_interface.get_page("file")

        mock_file.seek(0)
        mock_open.assert_called_with(f"{self.path_of_file}/file", "rb")
        self.assertEqual(zlib.decompress(mock_file.read()), page.get_data())

if __name__ == "__main__":
    unittest.main()