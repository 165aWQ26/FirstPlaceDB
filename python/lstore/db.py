from lstore.table import Table
from lstore._core import CoreDatabase


class Database:
    def __init__(self):
        self.tables = []
        self._core = CoreDatabase()

    # Not required for milestone1
    def open(self, path):
        self._core.open(path)

    def close(self):
        self._core.close()

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def create_table(self, name, num_columns, key_index):
        self._core.create_table(name, num_columns, key_index)
        table = Table(name, num_columns, key_index)
        table._core_db = self._core
        return table

    """
    # Deletes the specified table
    """

    def drop_table(self, name):
        self._core.drop_table(name)

    """
    # Returns table with the passed name
    """

    def get_table(self, name):
        result = self._core.get_table(name)
        if result is None:
            return None
        num_columns, key_index = result
        table = Table(name, num_columns, key_index)
        table._core_db = self._core
        return table
