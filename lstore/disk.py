from .phys_page import PhysicalPage
import zlib
import os

class DiskInterface:
    def __init__(self, path: str) -> None:
        if len(path) > 0 and path[-1] == "/":
            self.path : str = path[:-1]
        else:
            self.path : str = path

    def page_exists(self, page_id) -> bool:
        file_name : str = self.__make_file_name(page_id)
        return os.path.isfile(file_name)

    def get_page(self, page_id : str) -> PhysicalPage:
        file_name : str = self.__make_file_name(page_id)
        while True:
            with open(file_name, "rb") as file:
                compressed_data = file.read()
            if len(compressed_data) != 0:
                break
        uncompressed_data : bytearray = zlib.decompress(compressed_data)
        return PhysicalPage(bytearray(uncompressed_data))

    def write_page(self, page_id : str, page_to_write : PhysicalPage) -> None:
        file_name : str = self.__make_file_name(page_id)
        data: bytearray = page_to_write.get_data()
        compressed_data: bytearray = zlib.compress(data)
        with open(file_name, "wb") as file:
            file.write(compressed_data)

    def __make_file_name(self, page_id : str) -> str:
        return f"{self.path}/{page_id}"
