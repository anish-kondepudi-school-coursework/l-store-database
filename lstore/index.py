"""
A data structure holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
import lstore.table as Table


class Index:
    def __init__(self, table: Table):
        # One index for each table. All our empty initially.
        self.indices: list = [None] * table.num_columns
        self.key_to_rid = dict()

    def add_key_rid(self, key: int, rid: int) -> None:
        assert key not in self.key_to_rid
        self.key_to_rid[key] = rid

    def get_rid(self, key: int) -> None:
        assert key in self.key_to_rid
        return self.key_to_rid[key]

    def key_exists(self, key: int) -> bool:
        return key in self.key_to_rid

    def delete_key(self, key: int) -> None:
        assert key in self.key_to_rid
        del self.key_to_rid[key]

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
