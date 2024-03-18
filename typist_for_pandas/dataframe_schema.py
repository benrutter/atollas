from dataclasses import dataclass

from typist_for_pandas.types import ColumnType

@dataclass
class Schema:
    def __init__(self, **kwargs: ColumnType):
        for column, column_type in kwargs.items():
            setattr(self, column, column_type)

    def __is_column_attr__(self, attr: str) -> bool:
        return not (attr.startswith("__") or attr == "to_dict")

    def __column_attrs__(self) -> dict:
        return {
            k: getattr(self, k) for k in dir(self)
            if self.__is_column_attr__(k)
        }

    def to_dict(self):
        return self.__column_attrs__()

    def __add__(self, other: ColumnType):
        return Schema(**self.__column_attrs__(), **other.__column_attrs__())


    def __str__(self):
        return str(self.__column_attrs__())

    def __repr__(self):
        return self.__str__()
