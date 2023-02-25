from .config import (
    MAX_BASE_PAGES_IN_PAGE_RANGE,
    INVALID_RID,
    INVALID_OFFSET,
    INDIRECTION_COLUMN,
    SCHEMA_ENCODING_COLUMN,
    NUMBER_OF_METADATA_COLUMNS,
    LOGICAL_DELETE,
)
from .page import LogicalPage, BasePage, TailPage
from .page_directory import PageDirectory
from .rid import RID_Generator
from typing import Tuple


class PageRange:
    __slots__ = "num_attr_cols", "num_total_cols", "base_pages", "tail_pages", "page_directory", "rid_generator", "cumulative"

    def __init__(self, num_cols: int, page_directory: PageDirectory, rid_generator: RID_Generator, cumulative: bool):
        self.num_attr_cols: int = num_cols
        self.num_total_cols: int = num_cols + NUMBER_OF_METADATA_COLUMNS - (1 if cumulative else 0)
        self.base_pages: list[BasePage] = [BasePage(self.num_total_cols, rid_generator)]
        self.tail_pages: list[TailPage] = [TailPage(self.num_total_cols, rid_generator)]
        self.page_directory: PageDirectory = page_directory
        self.rid_generator: RID_Generator = rid_generator
        self.cumulative = cumulative

    def is_full(self) -> bool:
        return len(self.base_pages) == MAX_BASE_PAGES_IN_PAGE_RANGE and self.base_pages[-1].is_full()

    # note that for milestone the record will never actually be deleted, only invalidated
    # in the future record will be removed during merge cycles
    def invalidate_record(self, base_rid: int):
        curr_rid: int = base_rid
        tail_chain = []
        while True:
            page, offset = self.page_directory.get_page(curr_rid)
            curr_rid = page.get_column_of_record(INDIRECTION_COLUMN, offset)
            page.update_indir_of_record(LOGICAL_DELETE, offset)
            if curr_rid == base_rid:
                break
        return tail_chain

    def insert_record(self, columns: list) -> int:
        if self.is_full():
            return INVALID_RID

        latest_base_page: BasePage = self.base_pages[-1]
        if latest_base_page.is_full():
            new_base_page: BasePage = BasePage(self.num_total_cols, self.rid_generator)
            self.base_pages.append(new_base_page)
            latest_base_page = new_base_page

        # schema encoding and indirection set to 0
        rid, offset = latest_base_page.insert_record(columns + [0, INVALID_RID])
        latest_base_page.update_indir_of_record(rid, offset)
        self.page_directory.insert_page(rid, latest_base_page, offset)
        return rid

    def update_record(self, base_rid: int, columns_to_update: list) -> int:
        assert len(columns_to_update) == self.num_attr_cols

        # Find latest version of record (may be in Base or Tail page)
        (
            latest_page,
            latest_page_offset,
            latest_record_rid,
        ) = self.__get_latest_record_details(base_rid)
        latest_record_columns: list[int] = [latest_page.get_column_of_record(ind, latest_page_offset) for ind in range(self.num_attr_cols)]

        new_tail_record_columns = []
        if self.cumulative:
            for ind, (old_col, new_col) in enumerate(zip(latest_record_columns, columns_to_update)):
                new_tail_record_columns.append(old_col if new_col == None else new_col)
        else:
            new_tail_record_columns = columns_to_update.copy()

        if not self.cumulative:
            # Construct schema encoding integer for new record
            schema_encoding_integer = 0
            for ind, col in enumerate(columns_to_update):
                schema_encoding_integer |= 1 << (self.num_attr_cols - ind - 1) if col != None else 0
            new_tail_record_columns.append(schema_encoding_integer)

        new_tail_record_columns.append(latest_record_rid)

        # Find latest tail page to insert next version of record
        latest_tail_page: TailPage = self.tail_pages[-1]
        if latest_tail_page.is_full():
            new_tail_page = TailPage(self.num_total_cols, self.rid_generator)
            self.tail_pages.append(new_tail_page)
            latest_tail_page = new_tail_page

        # Insert new record and update page directory
        new_tail_page_rid, new_tail_page_offset = latest_tail_page.insert_record(new_tail_record_columns)
        self.page_directory.insert_page(new_tail_page_rid, latest_tail_page, new_tail_page_offset)

        # Update indirection of base record to point to latest tail record
        base_page, base_page_offset = self.page_directory.get_page(base_rid)
        base_page.update_indir_of_record(new_tail_page_rid, base_page_offset)

        return new_tail_page_rid

    def get_latest_column_values(self, rid: int, projected_columns_index: list) -> list[int]:
        if self.cumulative:
            indices = [i for i in range(len(projected_columns_index)) if projected_columns_index[i] == 1]
            return self.cumulative_get_latest_column_values(rid, indices)
        else:
            return self.non_cumulative_get_latest_column_values(rid, projected_columns_index)        

    def cumulative_get_latest_column_values(self, base_rid, column_indices):
        """
        `base_rid`: the RID of the record that is being fetched
        `column_indices`: binary array of attribute indices to be fetched
        `Description` this method pulls all of the column attributes specified by `column_indices`
        from a given page. Is preferable over `cumulative_get_latest_column_value` for when fetching
        multiple attributes from a given record when a record is fully in cache.
        """
        page, offset, _ = self.__get_latest_record_details(base_rid)
        return [page.get_column_of_record(column_index, offset) for column_index in column_indices]

    def non_cumulative_get_latest_column_values(self, rid: int, projected_columns_index: list) -> list[int]:
        col_vals: list[int] = []
        for col_ind in range(len(projected_columns_index)):
            if projected_columns_index[col_ind] == 1:
                col_val: int = self.non_cumulative_get_single_column(rid, col_ind)
                col_vals.append(col_val)
        return col_vals

    def non_cumulative_get_single_column(self, base_rid, column_index):
        page, offset = self.__get_base_page_of_record(base_rid)
        while self.record_has_most_recent_col_value(page, offset, column_index):
            next_page_rid = page.get_column_of_record(INDIRECTION_COLUMN, offset)
            if next_page_rid == INVALID_RID:
                break
            page, offset = self.page_directory.get_page(next_page_rid)
            if next_page_rid == base_rid:
                break
        column_value = page.get_column_of_record(column_index, offset)
        return column_value

    def record_has_most_recent_col_value(self, page: LogicalPage, offset: int, column_index: int):
        schema_encoding_value = page.get_column_of_record(SCHEMA_ENCODING_COLUMN, offset)
        column_index_in_schema_encoding = self.num_attr_cols - column_index - 1
        return (schema_encoding_value >> column_index_in_schema_encoding) % 2 == 0

    def _get_tail_chain(self, base_rid: int) -> list:
        curr_rid: int = base_rid
        tail_chain = []
        while True:
            page, offset = self.page_directory.get_page(curr_rid)
            record = [page.get_column_of_record(ind, offset) for ind in range(self.num_total_cols)]
            tail_chain.append((curr_rid, record))
            curr_rid = page.get_column_of_record(INDIRECTION_COLUMN, offset)
            if curr_rid == base_rid:
                break
        return tail_chain

    def __get_latest_record_details(self, base_rid: int) -> Tuple[LogicalPage, int, int]:
        base_page, base_page_offset = self.__get_base_page_of_record(base_rid)
        base_record_indir_rid: int = base_page.get_column_of_record(INDIRECTION_COLUMN, base_page_offset)
        if base_rid == base_record_indir_rid:
            return base_page, base_page_offset, base_rid

        tail_page, tail_page_offset = self.page_directory.get_page(base_record_indir_rid)
        return tail_page, tail_page_offset, base_record_indir_rid

    # note, we should consider making sure that the page retrieved is a BasePage object
    # rather than a parent LogicalPage
    def __get_base_page_of_record(self, base_rid) -> Tuple[BasePage, int]:
        base_page, base_page_offset = self.page_directory.get_page(base_rid)
        assert base_page != None and base_page_offset != INVALID_OFFSET
        return base_page, base_page_offset
