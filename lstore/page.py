from .config import PHYSICAL_PAGE_SIZE, ATTRIBUTE_SIZE, INDIRECTION_COLUMN, INVALID_OFFSET

class LogicalPage:

    def __init__(self, num_cols: int):
        self.num_cols = num_cols
        self.phys_pages = [PhysicalPage() for _ in range(self.num_cols)]
        self.available_chunks = [index for index in range(PhysicalPage.max_number_of_records)]
    
    def insert_record(self, columns: list) -> int:
        if self.is_full():
            return INVALID_OFFSET
        offset = self.available_chunks.pop()
        for ind in range(self.num_cols):
            if columns[ind]:
                self.phys_pages[ind].insert_value(columns[ind], offset)
        return offset

    def is_full(self) -> bool:
        return len(self.available_chunks) == 0

class BasePage(LogicalPage):

    def update_indir_of_record(self, new_value: int, offset: int) -> bool:
        phys_page_of_indir = self.phys_pages[INDIRECTION_COLUMN]
        return phys_page_of_indir.insert_value(new_value, offset)

class TailPage(LogicalPage):

    def get_column_of_record(self, column_index: int, offset: int) -> int:
        assert self.__is_valid_column_index(column_index)
        return self.phys_pages[column_index].get_column_value(offset)
    
    def __is_valid_column_index(self, column_index: int) -> bool:
        return 0 <= column_index < self.num_cols

class PhysicalPage:

    max_number_of_records: int = PHYSICAL_PAGE_SIZE // ATTRIBUTE_SIZE

    def __init__(self):
        self.data = bytearray(PHYSICAL_PAGE_SIZE)

    def get_column_value(self, offset: int) -> int:
        assert self.__is_offset_valid(offset)
        column_bytes = self.data[offset * ATTRIBUTE_SIZE : offset * ATTRIBUTE_SIZE + ATTRIBUTE_SIZE]
        column_value = int.from_bytes(column_bytes, byteorder='big', signed=True)
        return column_value

    def insert_value(self, value: int, offset: int) -> bool:
        if not self.__is_offset_valid(offset):
            return False
        value_bytes = value.to_bytes(ATTRIBUTE_SIZE, byteorder='big', signed=True)
        self.data[offset * ATTRIBUTE_SIZE : offset * ATTRIBUTE_SIZE + ATTRIBUTE_SIZE] = value_bytes
        return True

    def __is_offset_valid(self, offset: int) -> bool:
        return 0 <= offset < PhysicalPage.max_number_of_records