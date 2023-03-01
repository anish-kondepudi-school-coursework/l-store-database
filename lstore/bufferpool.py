from .phys_page import PhysicalPage
from .disk import DiskInterface
from copy import deepcopy
import os

class Bufferpool:

    def __init__(self, max_buffer_pool_size: int, path: str) -> None:
        self.max_buffer_pool_size: int = max_buffer_pool_size
        self.physical_pages: dict[str,PhysicalPage] = dict()
        self.disk: DiskInterface = DiskInterface(path)
        if path != "":
            os.makedirs(path, exist_ok=True)

    def insert_page(self, page_id: str, slot_num: int, value: int) -> bool:
        if (page_id in self.physical_pages):
            physical_page = self.physical_pages[page_id]
        elif self.disk.page_exists(page_id):
            self.__evict_page_if_bufferpool_full()
            physical_page = self.disk.get_page(page_id)
        else:
            self.__evict_page_if_bufferpool_full()
            physical_page: PhysicalPage = PhysicalPage()

        #print(f"bufferpool insert page: Inserting {value} in {slot_num}")
        physical_page.insert_value(value, slot_num)
        physical_page.set_dirty()

        self.physical_pages[page_id] = physical_page
        return True

    def copy_page(self, source_page_id: str, dest_page_id: str) -> bool:
        if (dest_page_id in self.physical_pages or
            self.disk.page_exists(dest_page_id)):
            return False
        self.__evict_page_if_bufferpool_full()
        source_page = self.get_page(source_page_id)
        source_page_copy = deepcopy(source_page)
        self.__evict_page_if_bufferpool_full()
        source_page_copy.set_dirty()
        self.physical_pages[dest_page_id] = source_page_copy
        return True

    def get_page(self, page_id: str) -> PhysicalPage:
        if page_id in self.physical_pages:
            #print("Bufferpool get page - found page ", page_id)
            return self.physical_pages[page_id]

        if not self.disk.page_exists(page_id):
            return None

        self.__evict_page_if_bufferpool_full()

        physical_page: PhysicalPage = self.disk.get_page(page_id)
        self.physical_pages[page_id] = physical_page
        return physical_page

    def evict_all_pages(self) -> None:
        for _ in range(len(self.physical_pages)):
            self._evict_page()

    def __evict_page_if_bufferpool_full(self) -> None:
        num_free_pages: int = self.max_buffer_pool_size - len(self.physical_pages)
        if num_free_pages == 0:
            self._evict_page()

    def _evict_page(self) -> None:
        if len(self.physical_pages) == 0:
            return

        physical_pages_and_page_ids: list[tuple[PhysicalPage,str]] = \
            sorted([(phys_page, page_id) for page_id, phys_page in list(self.physical_pages.items())],
                   key=lambda x: x[0].get_timestamp())

        bufferpool_page_index: int = 0
        while True:
            physical_page, page_id = physical_pages_and_page_ids[bufferpool_page_index]
            bufferpool_page_index = (bufferpool_page_index + 1) % len(physical_pages_and_page_ids)

            if not physical_page.can_evict():
                continue

            if physical_page.is_dirty():
                self.disk.write_page(page_id, physical_page)

            del self.physical_pages[page_id]
            break