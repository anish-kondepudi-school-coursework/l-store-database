from .page import PhysicalPage
from .disk import DiskInterface

class Bufferpool:

    def __init__(self, max_buffer_pool_size: int) -> None:
        self.max_buffer_pool_size: int = max_buffer_pool_size
        self.physical_pages: dict[PhysicalPage] = dict()
        self.disk = DiskInterface("test") # ToDo: change from "test" to real name

    def insert_page(self, page_id: str, slot_num: int, value: int) -> bool:
        if page_id in self.physical_pages:
            return False

        num_free_pages: int = self.max_buffer_pool_size - len(self.physical_pages)
        if num_free_pages == 0:
            self.__evict_page()

        physical_page: PhysicalPage = PhysicalPage()
        physical_page.insert_value(value, slot_num)
        physical_page.set_dirty()

        assert page_id not in self.physical_pages
        self.physical_pages[page_id] = physical_page
        return True

    def get_page(self, page_id: str) -> PhysicalPage:
        if page_id in self.physical_pages:
            return self.physical_pages[page_id]

        num_free_pages: int = self.max_buffer_pool_size - len(self.physical_pages)
        if num_free_pages == 0:
            self.__evict_page()

        physical_page: PhysicalPage = self.disk.get_page(page_id)
        self.physical_pages[page_id] = physical_page
        return physical_page

    def evict_all_pages(self) -> None:
        for _ in range(self.physical_pages):
            self.__evict_page()

    def __evict_page(self) -> None:
        if len(self.physical_pages) == 0:
            return

        physical_pages_and_page_ids: list[tuple[PhysicalPage,str]] = [(physical_page, page_id) for page_id, physical_page in self.physical_pages.items()]
        physical_pages_and_page_ids.sort(key=lambda x: x[0].get_timestamp())

        idx: int = 0
        while True:
            physical_page, page_id = physical_pages_and_page_ids[idx]
            idx = (idx + 1) % len(physical_pages_and_page_ids)

            if not physical_page.can_evict():
                physical_pages_and_page_ids.append((physical_page, page_id))
                continue

            if physical_page.is_dirty():
                self.disk.write_page(page_id, physical_page)

            del self.physical_pages[page_id]
