from lstore.table import Table
from lstore.bufferpool import Bufferpool
from lstore.config import MAX_BUFFERPOOL_SIZE

class Database:
    def __init__(self):
        self.table_name_to_table = {}
        self.bufferpool = None
        pass

    # Not required for milestone1
    def open(self, path):
        self.bufferpool = Bufferpool(MAX_BUFFERPOOL_SIZE, path)
        # reload page directory and index from disk

    def close(self):
        self.bufferpool.evict_all_pages()

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def create_table(self, name, num_columns, key_index):
        if self.bufferpool is None:
            self.bufferpool = Bufferpool(MAX_BUFFERPOOL_SIZE, "")
        table = Table(name, num_columns, key_index, self.bufferpool)
        self.table_name_to_table[name] = table
        return table

    """
    # Deletes the specified table
    """

    def drop_table(self, name):
        pass

    """
    # Returns table with the passed name
    """

    def get_table(self, name):
        if name not in self.table_name_to_table:
            return Table(name, 5, 0, self.bufferpool)
        return self.table_name_to_table.get(name)
