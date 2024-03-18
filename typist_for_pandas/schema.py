from dataclasses import dataclass

from typist_for_pandas.types import ColumnType

@dataclass
class Schema:
    def __init__(self, **kwargs: ColumnType):
        for column, column_type in kwargs.items():
            setattr(self, column, column_type)
