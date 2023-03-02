from .config import INVALID_SLOT_NUM
from .page import BasePage, TailPage, LogicalPage
from typing import Tuple


class PageDirectory:
    def __init__(self) -> None:
        self.page_directory = dict()

    def get_page(self, rid: int) -> Tuple[LogicalPage | None, int]:
        return self.page_directory.get(rid)

    def insert_page(self, rid: int, base_page: BasePage) -> None:
        self.page_directory[rid] = base_page

    def delete_page(self, rid: int) -> None:
        assert rid in self.page_directory
        del self.page_directory[rid]

    def update_page(self, rid_list: dict()) -> None:
        for rid in rid_list:
            self.page_directory[rid] = rid_list[rid]
