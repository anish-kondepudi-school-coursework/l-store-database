from .config import (
    INVALID_RID,
    MAX_BASE_PAGES_IN_PAGE_RANGE,
    PHYSICAL_PAGE_SIZE,
    ATTRIBUTE_SIZE,
    BASE_RID,
)
from .index import Index
from .rid import RID_Generator
from .page import get_copy_of_base_page
from .page_range import PageRange
from .page_directory import PageDirectory
from .secondary import SecondaryIndex, DSAStructure
from typing import List
import queue
import threading
from .bufferpool import Bufferpool


class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns


class Table:
    num_records_in_page_range = MAX_BASE_PAGES_IN_PAGE_RANGE * PHYSICAL_PAGE_SIZE // ATTRIBUTE_SIZE

    def __init__(
        self,
        name: str,
        num_columns: int,
        primary_key_col: int,
        bufferpool: Bufferpool,
        cumulative=True,
        mp=False,
        secondary_structure: DSAStructure = DSAStructure.DICTIONARY_SET
    ):
        """
        `name`: string         #Table name
        `num_columns`: int     #Number of Columns: all columns are integer
        `key`: int             #Index of table key in columns
        `mp`: bool             #Whether to use multiprocessing
        #: note, this will initialize the table with a single page range and all attributes
        with a secondary indices initially
        """
        self.name: str = name
        self.primary_key_col: int = primary_key_col
        self.num_columns: int = num_columns
        self.bufferpool = bufferpool
        self.index: Index = Index(self)
        self.page_directory: PageDirectory = PageDirectory()
        self.rid_generator: RID_Generator = RID_Generator()
        self.multiprocessing = mp
        self.cumulative = cumulative
        self.secondary_indices: list[SecondaryIndex | None] = [
            SecondaryIndex(
                self.name,
                f"attribute_{i}",
                structure=secondary_structure,
            )
            if i != self.primary_key_col
            else None
            for i in range(self.num_columns)
        ]
        self.page_ranges: list[PageRange] = [
            PageRange(
                self.num_columns, self.page_directory, self.rid_generator, self.name, self.bufferpool, cumulative
            )
        ]
        self.page_ranges: list[PageRange] = [PageRange(self.num_columns, self.page_directory, self.rid_generator, self.name, self.bufferpool, cumulative)]
        self.continue_merge = True
        self.finished_merge = False
        self.merge_queue = queue.Queue()
        self.merge_thread = threading.Thread(target=self.__merge)
        self.merge_thread.daemon = True
        self.merge_thread.start()

    def prepare_unpickle(self):
        self.continue_merge = True
        self.finished_merge = False
        self.merge_queue = queue.Queue()
        self.merge_thread = threading.Thread(target=self.__merge)
        self.merge_thread.daemon = True
        self.merge_thread.start()

    def prepare_to_be_pickled(self):
        for page_range in self.page_ranges:
            page_range.full_tail_pages.append(page_range.tail_pages[-1])
            self.merge_queue.put((page_range.full_tail_pages.copy(), page_range.updated_base_pages.copy(), page_range.prev_tid))
            page_range.full_tail_pages.clear
            page_range.updated_base_pages.clear
        self.continue_merge = False
        while not self.finished_merge:
            pass
        self.merge_queue = None
        self.stop_merging = None
        self.merge_thread = None
        for secondary in self.secondary_indices:
            if secondary:
                secondary.save_index()

    def delete_record(self, primary_key: int) -> None:
        """
        #: has to delete the record from both primary and secondary indices
        """
        rid: int = self.index.get_rid(primary_key)
        page_range: PageRange = self.__find_page_range_with_rid(rid)
        vals = page_range.invalidate_record(
            rid, [i != None for i in self.secondary_indices]
        )
        for i, val in enumerate(vals):
            if val == None:
                continue
            self.secondary_indices[i].delete_record(val, rid)
        self.index.delete_key(primary_key)
        self.page_directory.delete_page(primary_key)

    def insert_record(self, columns: list) -> bool:
        """
        #: abort operation if index already contains primary key -- keeps operations atomic
        #: will update secondary indices when record is inserted, uses `mp` flag to determine if
        the multiprocessing or serial methods are used
        """
        assert not self.index.key_exists(columns[self.primary_key_col])
        last_page_range: PageRange = self.page_ranges[-1]
        if last_page_range.is_full():
            new_page_range: PageRange = PageRange(
                self.num_columns,
                self.page_directory,
                self.rid_generator,
                self.name,
                self.bufferpool,
                self.cumulative,
            )
            rid_from_insertion: int = new_page_range.insert_record(columns)
            self.page_ranges.append(new_page_range)
        else:
            rid_from_insertion: int = last_page_range.insert_record(columns)
        if rid_from_insertion != INVALID_RID:
            self.index.add_key_rid(columns[self.primary_key_col], rid_from_insertion)
            if self.multiprocessing:
                pass
            else:
                self.update_secondary_indices_serially(columns, rid_from_insertion)
            return True
        return False

    def brute_force_search(self, search_key: int, search_key_index: int) -> List[int]:
        """
        #: brute force search for all records with given key
        #: uses the keys within the primary key indexing structure
        """
        recordRids: List[int] = self.index.key_to_rid.values()
        matching_rids = []
        for rid in recordRids:
            page_range: PageRange = self.__find_page_range_with_rid(rid)
            attribute_value: int = page_range.get_latest_column_value(
                rid, search_key_index
            )
            if attribute_value == search_key:
                matching_rids.append(rid)
        return matching_rids

    def update_secondary_indices_serially(self, columns: list[int], rid: int) -> None:
        for i, attribute in enumerate(columns):
            if (
                i != self.primary_key_col
                and attribute != None
                and self.secondary_indices[i] != None
            ):
                self.secondary_indices[i].add_record(attribute, rid)

    def update_record(self, primary_key: int, columns: list[int]) -> bool:
        """
        #: index.get_rid() will throw assertion error and stop transaction if
        primary key does not exist in index -- keeps operations atomic
        #: has to update the values in both primary and secondary indices
        #: updates can only be performed on records by primary key
        """
        # One TA stated that database should not allow updates to primary key, another said we should, uncomment code below to disable PK updates
        """
        if(columns[self.primary_key_col]!=primary_key and columns[self.primary_key_col]!=None):
            return False
        """
        rid: int = self.index.get_rid(primary_key)
        page_range_with_record: PageRange = self.__find_page_range_with_rid(rid)
        self.index.delete_key(primary_key)
        newPrimaryKey: int = primary_key
        if columns[self.primary_key_col] != None:
            newPrimaryKey = columns[self.primary_key_col]
        self.index.add_key_rid(newPrimaryKey, rid)
        tid, diff_list = page_range_with_record.update_record(rid, columns)
        result = tid != INVALID_RID
        for i, (old_attribute, new_attribute) in enumerate(zip(diff_list, columns)):
            if old_attribute != None and self.secondary_indices[i] != None:
                self.secondary_indices[i].delete_record(old_attribute, rid)
                self.secondary_indices[i].add_record(new_attribute, rid)
        if result and page_range_with_record.full_tail_pages.__len__() == 3:
            self.merge_queue.put((page_range_with_record.full_tail_pages.copy(), page_range_with_record.updated_base_pages.copy(), page_range_with_record.prev_tid))
            page_range_with_record.full_tail_pages.clear
            page_range_with_record.updated_base_pages.clear
        page_range_with_record.prev_tid = tid
        return result

    def get_latest_column_values(
        self, ridList: int | List[int], projected_columns_index: list
    ) -> List[List[int | None]]:
        """
        Retrieve attributes of record given desired columns and rid
        :param ridList: the rid(s) of the record(s) you need to retreive. Can be multiple on secondary indices or ranges
        :param projected_columns_index: what columns to return. array of 1 or 0 values.
        :return: list of column values, where None is used to indicate that the column is not projected
        """
        assert len(projected_columns_index) == self.num_columns
        if isinstance(ridList, int):
            ridList = [ridList]
        """ index.get_rid() will throw assertion error and stop transaction if
            primary key does not exist in index -- keeps operations atomic """
        records: List[List[int]] = []
        for rid in ridList:
            page_range: PageRange = self.__find_page_range_with_rid(rid)
            col_vals: List[int] = []
            for col_ind in range(self.num_columns):
                if projected_columns_index[col_ind] == 1:
                    col_val: int = page_range.get_latest_column_value(rid, col_ind)
                    col_vals.append(col_val)
            records.append(col_vals)
        return records

    def __find_page_range_with_rid(self, rid: int):
        page_range_index: int = rid // self.num_records_in_page_range
        assert 0 <= page_range_index < len(self.page_ranges)
        return self.page_ranges[page_range_index]

    def get_indirection_value(self, rid: int):
        page_range: PageRange = self.__find_page_range_with_rid(rid)
        return page_range.get_latest_column_value(rid, -1)

    def get_versioned_rid(self, rid: int, relative_version: int):
        page_range: PageRange = self.__find_page_range_with_rid(rid)
        for _ in range(0, relative_version):
            rid = page_range.get_latest_column_value(rid, -1)
        return rid

    def __merge(self):
        tail_page_set: list
        updated_base_page_list: list
        latest_tid: int
        updated_rid = dict()
        original_base_pages = dict()
        copied_base_pages = dict()
        record_per_page = self.num_records_in_page_range // MAX_BASE_PAGES_IN_PAGE_RANGE
        while self.continue_merge or not self.merge_queue.empty():
            updated_rid.clear
            original_base_pages.clear
            copied_base_pages.clear
            tail_page_set, updated_base_page_list, latest_tid = self.merge_queue.get(True)
            for base_page in updated_base_page_list:
                copied_base_page = get_copy_of_base_page(base_page)
                assert copied_base_page != base_page
                original_base_pages[int(copied_base_page.get_starting_rid() / record_per_page)] = base_page
                copied_base_pages[int(copied_base_page.get_starting_rid() / record_per_page)] = copied_base_page
            last_tid = latest_tid
            tail_page_set.reverse()
            for tail_page in tail_page_set:
                starting_tid = tail_page.get_starting_rid()
                for tid in range(last_tid, starting_tid, 1):
                    # For a given tid, find which base rid it updated using the base_rid column/page
                    base_rid = tail_page.get_column_of_record(BASE_RID, self.rid_generator.get_slot_num(tid))
                    # Since we check tid from recent to least recent, if a record was updated already, it has latest values
                    if base_rid not in updated_rid:
                        record: list = []
                        for index in range(self.num_columns):
                            record.append(0)
                            record[index] = tail_page.get_column_of_record(index, self.rid_generator.get_slot_num(base_rid))
                        copied_base_pages[int(base_rid / record_per_page)].update_record(record, self.rid_generator.get_slot_num(base_rid))
                        updated_rid[base_rid] = copied_base_pages[int(base_rid / record_per_page)]
                last_tid = starting_tid + 1
            # Could add a lock for the page we are updating, loop on updated rids update mapping to value
            for base_page_index in copied_base_pages:
                copied_base_pages[base_page_index].tps = latest_tid
                # self.page_directory.update_page(updated_rid)
                # old_base_page = self.page_directory.get_page(copied_base_pages[base_page_index].get_starting_rid())
                self.page_directory.insert_page(copied_base_pages[base_page_index].get_starting_rid(), copied_base_pages[base_page_index])
                # new_base_page = self.page_directory.get_page(copied_base_pages[base_page_index].get_starting_rid())
                # assert(old_base_page != new_base_page)
        self.finished_merge = True
