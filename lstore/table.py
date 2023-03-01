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
import queue
import threading
from .bufferpool import Bufferpool

class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns


class Table:
    num_records_in_page_range = (
        MAX_BASE_PAGES_IN_PAGE_RANGE * PHYSICAL_PAGE_SIZE // ATTRIBUTE_SIZE
    )

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(
        self, name: str, num_columns: int, primary_key_col: int, bufferpool: Bufferpool, cumulative=True
    ):
        self.name: str = name
        self.primary_key_col: int = primary_key_col
        self.num_columns: int = num_columns
        self.bufferpool = bufferpool
        self.index: Index = Index(self)
        self.page_directory: PageDirectory = PageDirectory()
        self.rid_generator: RID_Generator = RID_Generator()
        self.cumulative = cumulative
        self.page_ranges: list[PageRange] = [
            PageRange(
                self.num_columns, self.page_directory, self.rid_generator, self.name, self.bufferpool, cumulative
            )
        ]
        self.merge_queue = queue.Queue()
        self.stop_merging = threading.Event()
        self.merge_thread = threading.Thread(target=self.__merge)
        self.merge_thread.daemon=True
        self.merge_thread.start()

    def prepare_unpickle(self):
        self.merge_queue = queue.Queue()
        self.stop_merging = threading.Event()
        self.merge_thread = threading.Thread(target=self.__merge)
        self.merge_thread.daemon=True
        self.merge_thread.start()

    def prepare_to_be_pickled(self):
        print("Stopping merging!")
        self.stop_merging.set()
        print("Merge thread join called")
        #self.merge_thread.join()
        print("Setting merge queue to none")
        self.merge_queue = None
        self.stop_merging = None
        self.merge_thread = None

    def delete_record(self, primary_key: int) -> None:
        rid: int = self.index.get_rid(primary_key)
        page_range: PageRange = self.__find_page_range_with_rid(rid)
        page_range.invalidate_record(rid)
        #self.index.delete_key(primary_key)

    def insert_record(self, columns: list) -> bool:
        """abort operation if index already contains primary key -- keeps operations atomic"""
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
            return True
        return False

    # def update_record(self, primary_key: int, columns: list) -> bool:
    #     """index.get_rid() will throw assertion error and stop transaction if
    #     primary key does not exist in index -- keeps operations atomic"""
    #     rid: int = self.index.get_rid(primary_key)
    #     page_range_with_record: PageRange = self.__find_page_range_with_rid(rid)
    #     self.index.delete_key(primary_key)
    #     newPrimaryKey: int = primary_key
    #     if columns[self.primary_key_col] != None:
    #         newPrimaryKey = columns[self.primary_key_col]
    #     self.index.add_key_rid(newPrimaryKey, rid)
    #     return page_range_with_record.update_record(rid, columns) != INVALID_RID

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
        tid: int = page_range_with_record.update_record(rid, columns) 
        result:bool = tid != INVALID_RID
        if(result and page_range_with_record.full_tail_pages.__len__()==3):
            self.merge_queue.put((page_range_with_record.full_tail_pages.copy(), page_range_with_record.updated_base_pages.copy(), page_range_with_record.prev_tid))
            page_range_with_record.full_tail_pages.clear
            page_range_with_record.updated_base_pages.clear
        page_range_with_record.prev_tid=tid
        return result

    def get_latest_column_values(self, rid: int, projected_columns_index: list):
        assert len(projected_columns_index) == self.num_columns
        """ index.get_rid() will throw assertion error and stop transaction if
            primary key does not exist in index -- keeps operations atomic """
        page_range: PageRange = self.__find_page_range_with_rid(rid)
        col_vals: list[int] = []
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
        return page_range.get_latest_column_value(rid, -1)

    def get_versioned_rid(self, rid: int, relative_version: int):
        page_range: PageRange = self.__find_page_range_with_rid(rid)
        for _ in range(0, relative_version):
            rid = page_range.get_latest_column_value(rid,-1)
        return rid

    def __merge(self):
        tail_page_set: list
        updated_base_page_list: list
        latest_tid: int
        updated_rid = dict()
        original_base_pages = dict()
        copied_base_pages = dict()
        record_per_page = self.num_records_in_page_range // MAX_BASE_PAGES_IN_PAGE_RANGE
        while(not self.stop_merging.is_set()):
            updated_rid.clear
            original_base_pages.clear
            copied_base_pages.clear
            tail_page_set, updated_base_page_list, latest_tid = self.merge_queue.get(True)
            for base_page in updated_base_page_list:
                copied_base_page = get_copy_of_base_page(base_page)
                original_base_pages[int(copied_base_page.get_starting_rid()/record_per_page)] = base_page
                copied_base_pages[int(copied_base_page.get_starting_rid()/record_per_page)] = copied_base_page
            last_tid = latest_tid
            tail_page_set.reverse()
            for tail_page in tail_page_set:
                starting_tid = tail_page.get_starting_rid()
                for tid in range(last_tid,starting_tid, 1):
                    #For a given tid, find which base rid it updated using the base_rid column/page
                    #base_rid = tail_page.get_column_of_record(BASE_RID, tid%record_per_page)
                    base_rid = tail_page.get_column_of_record(BASE_RID, self.rid_generator.get_slot_num(tid))
                    #Since we check tid from recent to least recent, if a record was updated already, it has latest values
                    if(base_rid not in updated_rid):
                        record: list = []
                        for index in range(self.num_columns):
                            record.append(0)
                            record[index] = tail_page.get_column_of_record(index, self.rid_generator.get_slot_num(base_rid))
                            #record[index] = tail_page.get_column_of_record(index, base_rid%record_per_page)
                        #Updates record at given base_rid to have most up to date values
                        #copied_base_pages[int(base_rid/record_per_page)].update_record(record, base_rid%record_per_page)
                        copied_base_pages[int(base_rid/record_per_page)].update_record(record, self.rid_generator.get_slot_num(base_rid))
                        updated_rid[base_rid]=copied_base_pages[int(base_rid/record_per_page)]
                last_tid=starting_tid+1
            #Could add a lock for the page we are updating, loop on updated rids update mapping to value
            for base_page_index in copied_base_pages:
                copied_base_pages[base_page_index].tps = latest_tid
                #self.page_directory.update_page(updated_rid)
                old_base_page = self.page_directory.get_page(copied_base_pages[base_page_index].get_starting_rid())
                self.page_directory.insert_page(copied_base_pages[base_page_index].get_starting_rid(), copied_base_pages[base_page_index])
                new_base_page = self.page_directory.get_page(copied_base_pages[base_page_index].get_starting_rid())
                #assert(old_base_page != new_base_page)
        print("Merging is done!")
        exit()