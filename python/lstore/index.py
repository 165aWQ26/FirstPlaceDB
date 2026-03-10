"""
A data structure holding indices for various columns of a table. Key column should be indexed by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each column. All our empty initially.
        self.table = table

    def create_index(self, column_number):
        if self.table._core_db is not None:
            from lstore._core import CoreQuery as _CoreQuery
            _CoreQuery(self.table.name, self.table._core_db).create_index(column_number)

    def drop_index(self, column_number):
        if self.table._core_db is not None:
            from lstore._core import CoreQuery as _CoreQuery
            _CoreQuery(self.table.name, self.table._core_db).drop_index(column_number)