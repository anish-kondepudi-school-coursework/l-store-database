from .config import (
    INDIRECTION_COLUMN,
    SCHEMA_ENCODING_COLUMN,
    INVALID_SLOT_NUM,
    INVALID_RID,
    BASE_RID,
)
from copy import copy, deepcopy
from .rid import RID_Generator
from abc import ABC
from .bufferpool import Bufferpool
from .phys_page import PhysicalPage


class LogicalPage(ABC):
    def __init__(self, table_name: str, num_cols: int, bufferpool: Bufferpool) -> None:
        self.starting_rid = self.rids[-1]
        self.num_cols = num_cols
        self.table_name = table_name
        self.page_ids = [self.get_page_id_of_col(col) for col in range(num_cols)]
        self.available_chunks = [index for index in range(PhysicalPage.max_number_of_records - 1, -1, -1)]
        self.bufferpool = bufferpool

    def insert_record(self, columns: list):
        if self.is_full():
            return INVALID_RID, INVALID_SLOT_NUM
        slot_num = self.available_chunks.pop()
        for ind in range(self.num_cols):
            if columns[ind] != None:
                self.bufferpool.insert_page(self.page_ids[ind], slot_num, columns[ind])
        new_rid = self.rids.pop()
        return new_rid, slot_num

    def get_column_of_record(self, column_index: int, slot_num: int) -> int:
        assert self.__is_valid_column_index(column_index)
        page_id = self.page_ids[column_index]
        phys_page = self.bufferpool.get_page(page_id)
        return phys_page.get_column_value(slot_num)

    def update_indir_of_record(self, new_value: int, slot_num: int) -> bool:
        page_id = self.page_ids[INDIRECTION_COLUMN]
        phys_page = self.bufferpool.get_page(page_id)
        return phys_page.insert_value(new_value, slot_num)

    def is_full(self) -> bool:
        return len(self.available_chunks) == 0

    def __is_valid_column_index(self, column_index: int) -> bool:
        return (0 <= column_index < self.num_cols) or (column_index in (INDIRECTION_COLUMN, BASE_RID, SCHEMA_ENCODING_COLUMN))

    def get_starting_rid(self) -> int:
        return self.starting_rid


class BasePage(LogicalPage):
    def __init__(self, table_name: str, num_cols: int, bufferpool: Bufferpool, rid_generator: RID_Generator):
        self.rids = rid_generator.get_base_rids()
        self.merge_iteration = 0
        self.tps = 0
        super().__init__(table_name, num_cols, bufferpool)

    def copy_table_data_cols(self):
        self.merge_iteration += 1
        for col in range(self.num_cols - 2):
            old_page_id = self.page_ids[col]
            new_page_id = self.get_page_id_of_col(col)
            self.bufferpool.copy_page(old_page_id, new_page_id)
            self.page_ids[col] = new_page_id

    def get_page_id_of_col(self, col):
        return f"{self.table_name}_{self.starting_rid}_{col}_{self.merge_iteration}"

    """ Must only be called by merge """

    def update_record(self, columns: list, slot_num: int) -> None:
        for ind in range(self.num_cols - 2):
            success = self.bufferpool.insert_page(self.page_ids[ind], slot_num, columns[ind])
            if not success:
                # todo: make atomic by reverting all previous inserts?
                return False
        return True


def get_copy_of_base_page(base_page: BasePage) -> BasePage:
    copy_base_page = copy(base_page)
    copy_base_page.available_chunks = deepcopy(base_page.available_chunks)
    copy_base_page.page_ids = deepcopy(base_page.page_ids)
    copy_base_page.copy_table_data_cols()
    return copy_base_page


class TailPage(LogicalPage):
    def __init__(self, table_name: str, num_cols: int, bufferpool: Bufferpool, rid_generator: RID_Generator):
        self.rids = rid_generator.get_tail_rids()
        super().__init__(table_name, num_cols, bufferpool)

    def get_page_id_of_col(self, col):
        return f"{self.table_name}_{self.starting_rid}_{col}"
