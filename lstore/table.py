from .config import (
    INVALID_RID,
    NUM_RECORDS_IN_PAGE_RANGE
)
from .index import Index
from time import time
from .rid import RID_Generator
from .page_range import PageRange
from .page_directory import PageDirectory

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, primary_key_col):
        self.name = name
        self.primary_key_col = primary_key_col
        self.num_columns = num_columns
        self.index = Index(self)
        self.page_directory = PageDirectory()
        self.rid_gen = RID_Generator()
        self.page_ranges = [PageRange(self.num_columns, self.page_directory, self.rid_gen)]

    # todo: change name to primary_key
    def delete_record(self, key) -> bool:
        rid = self.index.get_rid(key)
        self.index.delete_key(key)
        self.page_directory.delete_page(rid)
        return True

    # should we check / assert that the primary key does not already exist in the index?
    def insert_record(self, columns) -> bool:
        last_page_range = self.page_ranges[-1]
        if last_page_range.is_full():
            new_page_range = PageRange(self.num_columns, self.page_directory, self.rid_gen)
            rid_from_insertion = new_page_range.insert_record(columns)
            self.page_ranges.append(new_page_range)
        else:
            rid_from_insertion = last_page_range.insert_record(columns)
        if rid_from_insertion != INVALID_RID:
            self.index.add_key_rid(columns[self.primary_key_col], rid_from_insertion)
            return True
        # Todo: change to return if insert cols of page range is successful or not
        return False

    def update_record(self, key: int, columns) -> bool:
        rid = self.index.get_rid(key)
        page_range_with_record = self._find_page_range_with_rid(rid)

        # update the index with new primary key
        self.index.delete_key(key)
        self.index.add_key_rid(columns[self.primary_key_col], rid)

        return page_range_with_record.update_record(rid, columns) != INVALID_RID

    # # todo: handle error where page range get latest col value returns error 
    # # also maybe handle error where projected_columns_index length is not num_col
    def get_latest_column_values(self, key, projected_columns_index):
        rid = self.index.get_rid(key)
        page_range = self._find_page_range_with_rid(rid)
        col_vals = []
        for col_ind in range(self.num_columns):
            if projected_columns_index[col_ind]:
                col_val = page_range.get_latest_column_value(rid, col_ind)
                col_vals.append(col_val)
        return col_vals

    def _find_page_range_with_rid(self, rid):
        page_range_index = int(rid / NUM_RECORDS_IN_PAGE_RANGE)
        assert 0 <= page_range_index < len(self.page_ranges)
        return self.page_ranges[page_range_index]

    def __merge(self):
        print("merge is happening")
        pass
 