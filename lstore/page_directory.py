from .config import INVALID_OFFSET
from .page import BasePage, TailPage

class PageDirectory():

    def __init__(self) -> None:
        self.page_directory: dict[int,tuple[BasePage,int]] | dict[int,tuple[TailPage,int]] = dict()

    def get_page(self, rid: int):
        return self.page_directory.get(rid, (None, INVALID_OFFSET))

    def insert_page(self, rid: int, base_page: BasePage, base_record_offset: int) -> None:
        assert rid not in self.page_directory
        self.page_directory[rid] = base_page, base_record_offset

    def delete_page(self, rid: int) -> None:
        assert rid in self.page_directory
        del self.page_directory[rid]