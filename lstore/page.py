from .config import (
    PHYSICAL_PAGE_SIZE,
    ATTRIBUTE_SIZE,
    INDIRECTION_COLUMN,
    SCHEMA_ENCODING_COLUMN,
    INVALID_SLOT_NUM,
    INVALID_RID,
)
import time
from .rid import RID_Generator
from abc import ABC, abstractmethod
from .bufferpool import Bufferpool
from .phys_page import PhysicalPage
import random 

class LogicalPage(ABC):
    def __init__(self, table_name: str, num_cols: int, bufferpool: Bufferpool) -> None:
        self.starting_rid = self.rids[0]
        self.num_cols = num_cols
        self.id = random.randint(0, 100000000)
        self.table_name = table_name
        self.page_ids = [f"{table_name}_{self.starting_rid}_{col}_{self.id}" for col in range(num_cols)]
        self.available_chunks = [
            index for index in range(PhysicalPage.max_number_of_records)
        ]
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
        #print("Getting page id: ", page_id)
        phys_page = self.bufferpool.get_page(page_id)
        return phys_page.get_column_value(slot_num)

    def update_indir_of_record(self, new_value: int, slot_num: int) -> bool:
        page_id = self.page_ids[INDIRECTION_COLUMN]
        phys_page = self.bufferpool.get_page(page_id)
        return phys_page.insert_value(new_value, slot_num)

    def is_full(self) -> bool:
        return len(self.available_chunks) == 0

    def __is_valid_column_index(self, column_index: int) -> bool:
        return (0 <= column_index < self.num_cols) or (
            column_index in (INDIRECTION_COLUMN, SCHEMA_ENCODING_COLUMN)
        )
    
    ''' Functions needed by merge '''
    def get_starting_rid(self) -> int:
        return self.starting_rid

from copy import deepcopy
NUM_METADATA_COLS = 2
class BasePage(LogicalPage):
    def __init__(self, table_name: str, num_cols: int, bufferpool: Bufferpool, rid_generator: RID_Generator):
        self.rids = rid_generator.get_base_rids()
        super().__init__(table_name, num_cols, bufferpool)
    
    ''' Must only be called by merge '''
    def update_record(self, columns: list, slot_num: int) -> None:
        for ind in range(self.num_cols):
            phys_page = self.bufferpool.get_page(self.page_ids[ind])
            success = phys_page.insert_value(columns[ind], slot_num)
            if not success:
                # todo: make atomic by reverting all previous inserts?
                return False 
        return True

def get_copy_of_base_page(base_page: BasePage) -> BasePage:
    copy_base_page = deepcopy(base_page)
    copy_base_page.bufferpool = base_page.bufferpool
    copy_base_page.id = random.randint(0, 100000000)
    copy_base_page.page_ids[:-NUM_METADATA_COLS] = [f"{base_page.table_name}_{base_page.starting_rid}_{col}_{copy_base_page.id}" for col in range(base_page.num_cols-NUM_METADATA_COLS)]
    for ind in range(0, base_page.num_cols - NUM_METADATA_COLS):
        base_page.bufferpool.copy_page(base_page.page_ids[ind], copy_base_page.page_ids[ind])
    return copy_base_page

class TailPage(LogicalPage):
    def __init__(self, table_name: str, num_cols: int, bufferpool: Bufferpool, rid_generator: RID_Generator):
        self.rids = rid_generator.get_tail_rids()
        super().__init__(table_name, num_cols, bufferpool)