from lstore.table import Table
from lstore._core import CoreDatabase

class Database:

    def __init__(self):
        self._core = CoreDatabase()
        self._tables = {}

    def open(self, path):
        self._core.open(path)

    def close(self):
        self._core.close()

    def create_table(self, name, num_columns, key_index):
        self._core.create_table(name, num_columns, key_index)
        table = Table(name, num_columns, key_index, self._core)
        self._tables[name] = table
        return table

    def drop_table(self, name):
        self._core.drop_table(name)
        self._tables.pop(name, None)

    def get_table(self, name):
        if name in self._tables:
            return self._tables[name]
        info = self._core.get_table(name)
        if info is None:
            return None
        num_columns, key_index = info
        table = Table(name, num_columns, key_index, self._core)
        self._tables[name] = table
        return table
