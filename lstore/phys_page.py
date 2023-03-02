from .config import (
    PHYSICAL_PAGE_SIZE,
    ATTRIBUTE_SIZE,
    INDIRECTION_COLUMN,
    SCHEMA_ENCODING_COLUMN,
    INVALID_SLOT_NUM,
    INVALID_RID,
)
import time


class PhysicalPage:
    max_number_of_records: int = PHYSICAL_PAGE_SIZE // ATTRIBUTE_SIZE

    def __init__(self, data: bytearray | None = None):
        if data is None:
            self.data = bytearray(PHYSICAL_PAGE_SIZE)
        else:
            assert len(data) == PHYSICAL_PAGE_SIZE
            self.data = data
        self.pinned: int = 0
        self.dirty: bool = False
        self.timestamp: float = time.time()

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
        column_bytes = self.data[slot_num * ATTRIBUTE_SIZE : slot_num * ATTRIBUTE_SIZE + ATTRIBUTE_SIZE]
        column_value = int.from_bytes(column_bytes, byteorder="big", signed=True)
        self.timestamp = time.time()
        return column_value

    def insert_value(self, value: int, slot_num: int) -> bool:
        if not self.__is_slot_num_valid(slot_num):
            return False
        value_bytes = value.to_bytes(ATTRIBUTE_SIZE, byteorder="big", signed=True)
        self.data[slot_num * ATTRIBUTE_SIZE : slot_num * ATTRIBUTE_SIZE + ATTRIBUTE_SIZE] = value_bytes
        self.dirty = True
        self.timestamp = time.time()
        return True

    def __is_slot_num_valid(self, slot_num: int) -> bool:
        return 0 <= slot_num < PhysicalPage.max_number_of_records
