from lstore.table import Table
from lstore.bufferpool import Bufferpool
from lstore.config import MAX_BUFFERPOOL_SIZE
import _pickle as pickle
import os


class Database:
    database_file_name: str = "database.db"

    def __init__(self):
        self.path = None
        self.table_name_to_table = {}
        self.bufferpool = None
        pass

    def open(self, path):
        self.path = path
        self.bufferpool = Bufferpool(MAX_BUFFERPOOL_SIZE, path)
        if self.__file_exists(Database.database_file_name):
            self.table_name_to_table = self.__load_data_from_disk(Database.database_file_name)
            for name in self.table_name_to_table:
                self.table_name_to_table[name].prepare_unpickle()

    def close(self):
        self.bufferpool.evict_all_pages()
        for name in self.table_name_to_table:
            self.table_name_to_table[name].prepare_to_be_pickled()
        self.__save_data_to_disk(Database.database_file_name, self.table_name_to_table)

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

    def get_table(self, name: str) -> Table:
        if name not in self.table_name_to_table:
            return Table(name, 5, 0, self.bufferpool)
        return self.table_name_to_table.get(name)

    def __file_exists(self, filename: str) -> bool:
        return os.path.exists(f"{self.path}/{filename}")

    def __save_data_to_disk(self, filename: str, data) -> None:
        with open(f"{self.path}/{filename}", "wb") as file:
            pickle.dump(data, file)

    def __load_data_from_disk(self, filename: str):
        with open(f"{self.path}/{filename}", "rb") as file:
            return pickle.load(file)
