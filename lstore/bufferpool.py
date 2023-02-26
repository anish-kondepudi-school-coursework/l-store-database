from .page import PhysicalPage
from .disk import DiskInterface

class Bufferpool:

    def __init__(self, max_buffer_pool_size: int, path: str) -> None:
        self.max_buffer_pool_size: int = max_buffer_pool_size
        self.physical_pages: dict[str,PhysicalPage] = dict()
        self.disk = DiskInterface(path)

    def insert_page(self, page_id: str, slot_num: int, value: int) -> bool:
        if page_id in self.physical_pages:
            return False

        # Can we expose a new endpoint in DiskInterface for "file_exists", so we
        # don't have to assert on exceptions to check if a page exists on disk

        # try:
        #     self.disk.get_page(page_id)
        #     return False
        # except FileNotFoundError:
        #     pass

        self.__evict_page_if_bufferpool_full()

        physical_page: PhysicalPage = PhysicalPage()
        physical_page.insert_value(value, slot_num)
        physical_page.set_dirty()

        self.physical_pages[page_id] = physical_page
        return True

    def get_page(self, page_id: str) -> PhysicalPage:
        if page_id in self.physical_pages:
            return self.physical_pages[page_id]

        self.__evict_page_if_bufferpool_full()

        try:
            physical_page: PhysicalPage = self.disk.get_page(page_id)
            self.physical_pages[page_id] = physical_page
            return physical_page
        except FileNotFoundError:
            return None

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
            sorted([(phys_page, page_id) for page_id, phys_page in self.physical_pages.items()],
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