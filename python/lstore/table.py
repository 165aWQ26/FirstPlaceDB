from lstore.index import Index
from time import time

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
    def __init__(self, name, num_columns, key_index, core_db=None):
        self.name = name
        self.num_columns = num_columns
        self.key_index = key_index
        self._core_db = core_db
        self.index = Index(self)
