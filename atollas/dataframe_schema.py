from typing import List, Dict
from dataclasses import dataclass

from atollas.types import ColumnType, nullable, not_unique

@dataclass
class Schema:
    def __init__(self, **kwargs: ColumnType):
        for column, column_type in kwargs.items():
            setattr(self, column, column_type)

    def to_dict(self):
        return {k: v for k, v in self}


    def __add__(self, other: "Schema"):
        return Schema(**self.to_dict(), **other.to_dict())

    def __iter__(self):
        for column in dir(self):
            if not column.startswith("__") and column not in ["to_dict", ""]:
                yield (column, getattr(self, column))

    def __str__(self):
        return str({k: v for k, v in self})

    def __repr__(self):
        return self.__str__()

    def __getitem__(self, i):
        return self.to_dict()[i]

def _identity(x):
    return x

def merge_schemas(
    right: Schema,
    left: Schema,
    l_on: List[str],
    r_on: List[str],
    l_suffix: str,
    r_suffix: str,
    inner: bool = False,
    one_to_one: bool = False,
) -> Schema:
    init_kwargs: Dict[str, ColumnType] = {}
    for left_col, right_col in zip(l_on, r_on):
        if right[left_col].representation != left[right_col].representation:
            message: str = (
                f"Column {left_col} cannot be merged as it has a different "
                "datatype in each dataframe.\n\n"
                f"Left datatype: {left_col.representation}\n"
                f"Right datatype: {right_col.representation}"
            )
            raise TypeError(message)
        merged_type: ColumnType = ColumnType(
            right[left_col].representation,
            not inner and right[left_col].nullable or left[right_col].nullable,
            right[left_col].unique and left[right_col].unique,
        )
        init_kwargs[left_col] = merged_type
        if left_col != right_col:
            init_kwargs[right_col] = merged_type
    shared: List[str] = list(set(right.to_dict()) & set(left.to_dict()))
    for col in shared:
        if col in l_on:
            continue
        null_applicator = _identity if inner else nullable
        unique_applicator = _identity if one_to_one else not_unique
        init_kwargs[f"{col}{l_suffix}"] = null_applicator(unique_applicator(left[col]))
        init_kwargs[f"{col}{r_suffix}"] = null_applicator(unique_applicator(right[col]))
    for col, col_type in right:
        if col in shared + l_on:
            continue
        null_applicator = _identity if inner else nullable
        unique_applicator = _identity if one_to_one else not_unique
        init_kwargs[col] = null_applicator(unique_applicator(col_type))
        init_kwargs[col] = null_applicator(unique_applicator(col_type))
    for col, col_type in left:
        if col in shared + r_on:
            continue
        null_applicator = _identity if inner else nullable
        unique_applicator = _identity if one_to_one else not_unique
        init_kwargs[col] = null_applicator(unique_applicator(col_type))
        init_kwargs[col] = null_applicator(unique_applicator(col_type))
    return Schema(**init_kwargs)
