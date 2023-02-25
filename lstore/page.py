from .config import (
    PHYSICAL_PAGE_SIZE,
    ATTRIBUTE_SIZE,
    INDIRECTION_COLUMN,
    SCHEMA_ENCODING_COLUMN,
    INVALID_SLOT_NUM,
    INVALID_RID,
)
from .rid import RID_Generator
from abc import ABC, abstractmethod
import time

class LogicalPage(ABC):
    def __init__(self, num_cols: int, rid_generator: RID_Generator):
        self.num_cols = num_cols
        self.phys_pages = [PhysicalPage() for _ in range(self.num_cols)]
        self.available_chunks = [
            index for index in range(PhysicalPage.max_number_of_records)
        ]
        self.rid_generator = rid_generator

    def insert_record(self, columns: list):
        if self.is_full():
            return INVALID_RID, INVALID_SLOT_NUM
        slot_num = self.available_chunks.pop()
        for ind in range(self.num_cols):
            if columns[ind] != None:
                self.phys_pages[ind].insert_value(columns[ind], slot_num)
        new_rid = self._generate_new_rid()
        return new_rid, slot_num

    def is_full(self) -> bool:
        return len(self.available_chunks) == 0

    def get_column_of_record(self, column_index: int, slot_num: int) -> int:
        assert self.__is_valid_column_index(column_index)
        return self.phys_pages[column_index].get_column_value(slot_num)

    def update_indir_of_record(self, new_value: int, slot_num: int) -> bool:
        phys_page_of_indir = self.phys_pages[INDIRECTION_COLUMN]
        return phys_page_of_indir.insert_value(new_value, slot_num)

    @abstractmethod
    def _generate_new_rid(self):
        pass

    def __is_valid_column_index(self, column_index: int) -> bool:
        return (0 <= column_index < self.num_cols) or (
            column_index in (INDIRECTION_COLUMN, SCHEMA_ENCODING_COLUMN)
        )


class BasePage(LogicalPage):
    def _generate_new_rid(self) -> int:
        return self.rid_generator.new_base_rid()


class TailPage(LogicalPage):
    def _generate_new_rid(self) -> int:
        return self.rid_generator.new_tail_rid()

class PhysicalPage:
    max_number_of_records: int = PHYSICAL_PAGE_SIZE // ATTRIBUTE_SIZE

    def __init__(self, data : bytearray | None = None):
        if data is None:
            self.data = bytearray(PHYSICAL_PAGE_SIZE)
        else:
            assert len(data) == PHYSICAL_PAGE_SIZE
            self.data = data
        self.pinned : int = 0
        self.dirty : bool = False
        self.timestamp : float = time.time()

    def get_data(self) -> bytearray:
        return self.data

    def is_dirty(self) -> bool:
        return self.dirty
    
    def set_dirty(self) -> None:
        self.dirty = True

    def get_timestamp(self) -> float:
        return self.timestamp

    def can_evict(self) -> bool:
        return self.pinned == 0

    def pin_page(self) -> None:
        self.pinned += 1
    
    def unpin_page(self) -> None:
        self.pinned -= 1

    def get_column_value(self, slot_num: int) -> int:
        assert self.__is_slot_num_valid(slot_num)
        column_bytes = self.data[
            slot_num * ATTRIBUTE_SIZE : slot_num * ATTRIBUTE_SIZE + ATTRIBUTE_SIZE
        ]
        column_value = int.from_bytes(column_bytes, byteorder="big", signed=True)
        self.timestamp = time.time()
        return column_value

    def insert_value(self, value: int, slot_num: int) -> bool:
        if not self.__is_slot_num_valid(slot_num):
            return False
        value_bytes = value.to_bytes(ATTRIBUTE_SIZE, byteorder="big", signed=True)
        self.data[
            slot_num * ATTRIBUTE_SIZE : slot_num * ATTRIBUTE_SIZE + ATTRIBUTE_SIZE
        ] = value_bytes
        self.dirty = True
        self.timestamp = time.time()
        return True

    def __is_slot_num_valid(self, slot_num: int) -> bool:
        return 0 <= slot_num < PhysicalPage.max_number_of_records