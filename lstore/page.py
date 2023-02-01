from .config import PHYSICAL_PAGE_SIZE, ATTRIBUTE_SIZE

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