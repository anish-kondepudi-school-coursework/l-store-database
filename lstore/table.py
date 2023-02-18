from .config import (
    INVALID_RID,
    MAX_BASE_PAGES_IN_PAGE_RANGE,
    PHYSICAL_PAGE_SIZE,
    ATTRIBUTE_SIZE,
    INDIRECTION_COLUMN
)
from .index import Index
from .rid import RID_Generator
from .page_range import PageRange
from .page_directory import PageDirectory


class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns


class Table:
    num_records_in_page_range = MAX_BASE_PAGES_IN_PAGE_RANGE * PHYSICAL_PAGE_SIZE // ATTRIBUTE_SIZE

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name: str, num_columns: int, primary_key_col: int, cumulative=True):
        self.name: str = name
        self.primary_key_col: int = primary_key_col
        self.num_columns: int = num_columns
        self.index: Index = Index(self)
        self.page_directory: PageDirectory = PageDirectory()
        self.rid_generator: RID_Generator = RID_Generator()
        self.cumulative = cumulative
        self.page_ranges: list[PageRange] = [PageRange(self.num_columns, self.page_directory, self.rid_generator, cumulative)]

    def delete_record(self, primary_key: int) -> None:
        rid: int = self.index.get_rid(primary_key)
        page_range: PageRange = self.__find_page_range_with_rid(rid)
        page_range.invalidate_record(rid)
        self.index.delete_key(primary_key)
        self.page_directory.delete_page(rid)

    def insert_record(self, columns: list) -> bool:
        """abort operation if index already contains primary key -- keeps operations atomic"""
        assert not self.index.key_exists(columns[self.primary_key_col])
        last_page_range: PageRange = self.page_ranges[-1]
        if last_page_range.is_full():
            new_page_range: PageRange = PageRange(
                self.num_columns,
                self.page_directory,
                self.rid_generator,
                self.cumulative,
            )
            rid_from_insertion: int = new_page_range.insert_record(columns)
            self.page_ranges.append(new_page_range)
        else:
            rid_from_insertion: int = last_page_range.insert_record(columns)
        if rid_from_insertion != INVALID_RID:
            self.index.add_key_rid(columns[self.primary_key_col], rid_from_insertion)
            return True
        return False

    def update_record(self, primary_key: int, columns: list) -> bool:
        """index.get_rid() will throw assertion error and stop transaction if
        primary key does not exist in index -- keeps operations atomic"""
        rid: int = self.index.get_rid(primary_key)
        page_range_with_record: PageRange = self.__find_page_range_with_rid(rid)
        self.index.delete_key(primary_key)
        newPrimaryKey: int = primary_key
        if columns[self.primary_key_col] != None:
            newPrimaryKey = columns[self.primary_key_col]
        self.index.add_key_rid(newPrimaryKey, rid)
        return page_range_with_record.update_record(rid, columns) != INVALID_RID

    def get_latest_column_values(self, rid: int, projected_columns_index: list):
        assert len(projected_columns_index) == self.num_columns
        """ index.get_rid() will throw assertion error and stop transaction if
            primary key does not exist in index -- keeps operations atomic """
        page_range: PageRange = self.__find_page_range_with_rid(rid)
        col_vals: list[int] = []
        if self.cumulative:
            indices = [i for i in range(self.num_columns) if projected_columns_index[i] == 1]
            col_vals = page_range.cumulative_get_multiple_latest_column_value(rid, indices)
        else:
            for col_ind in range(self.num_columns):
                if projected_columns_index[col_ind] == 1:
                    col_val: int = page_range.get_latest_column_value(rid, col_ind)
                    col_vals.append(col_val)
        return col_vals

    def __find_page_range_with_rid(self, rid: int):
        page_range_index: int = rid // self.num_records_in_page_range
        assert 0 <= page_range_index < len(self.page_ranges)
        return self.page_ranges[page_range_index]

    def get_indirection_value(self, rid: int):
        page_range: PageRange = self.__find_page_range_with_rid(rid)
        return page_range.get_latest_column_value(rid, INDIRECTION_COLUMN)

    def get_versioned_rid(self, rid: int, relative_version: int):
        page_range: PageRange = self.__find_page_range_with_rid(rid)
        for _ in range(0, relative_version):
            rid = page_range.get_latest_column_value(rid, -1)
        return rid

    def __merge(self):
        print("merge is happening")
        pass
