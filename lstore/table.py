from .config import (
    INVALID_RID,
    MAX_BASE_PAGES_IN_PAGE_RANGE,
    PHYSICAL_PAGE_SIZE,
    ATTRIBUTE_SIZE,
)
from .index import Index
from .rid import RID_Generator
from .page_range import PageRange
from .page_directory import PageDirectory
from .secondary import SecondaryIndex, DSAStructure
from typing import List


class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns


class Table:
    num_records_in_page_range = (
        MAX_BASE_PAGES_IN_PAGE_RANGE * PHYSICAL_PAGE_SIZE // ATTRIBUTE_SIZE
    )

    def __init__(
        self,
        name: str,
        num_columns: int,
        primary_key_col: int,
        cumulative=True,
        mp=False,
        secondary_structure: DSAStructure = DSAStructure.DICTIONARY_SET,
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
        self.index: Index = Index(self)
        self.page_directory: PageDirectory = PageDirectory()
        self.rid_generator: RID_Generator = RID_Generator()
        self.mp = mp
        self.cumulative = cumulative
        self.secondary_indices: list[SecondaryIndex | None] = [
            SecondaryIndex(
                self.name,
                f"attribute_{i}",
                multiprocess=False,
                structure=secondary_structure,
            )
            if i != self.primary_key_col
            else None
            for i in range(self.num_columns)
        ]
        self.page_ranges: list[PageRange] = [
            PageRange(
                self.num_columns, self.page_directory, self.rid_generator, cumulative
            )
        ]

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
                self.cumulative,
            )
            rid_from_insertion: int = new_page_range.insert_record(columns)
            self.page_ranges.append(new_page_range)
        else:
            rid_from_insertion: int = last_page_range.insert_record(columns)
        if rid_from_insertion != INVALID_RID:
            self.index.add_key_rid(columns[self.primary_key_col], rid_from_insertion)
            if self.mp:
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
        rid: int = self.index.get_rid(primary_key)
        page_range_with_record: PageRange = self.__find_page_range_with_rid(rid)
        self.index.delete_key(primary_key)
        newPrimaryKey: int = primary_key
        if columns[self.primary_key_col] != None:
            newPrimaryKey = columns[self.primary_key_col]
        self.index.add_key_rid(newPrimaryKey, rid)
        new_rid, diff_list = page_range_with_record.update_record(rid, columns)
        for i, (old_attribute, new_attribute) in enumerate(zip(diff_list, columns)):
            if old_attribute != None and self.secondary_indices[i] != None:
                self.secondary_indices[i].delete_record(old_attribute, rid)
                self.secondary_indices[i].add_record(new_attribute, rid)
        return new_rid != INVALID_RID

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
        print("merge is happening")
        pass
