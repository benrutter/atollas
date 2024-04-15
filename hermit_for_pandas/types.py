from copy import deepcopy
from dataclasses import dataclass


@dataclass
class ColumnType:
    representation: str
    nullable: bool = False
    unique: bool = False

def unique(column_type: ColumnType) -> ColumnType:
    return_type = deepcopy(column_type)
    return_type.unique = True
    return return_type

def not_unique(column_type: ColumnType) -> ColumnType:
    return_type = deepcopy(column_type)
    return_type.unique = False
    return return_type

def nullable(column_type: ColumnType) -> ColumnType:
    return_type = deepcopy(column_type)
    return_type.nullable = True
    return return_type

integer = ColumnType("int64[pyarrow]")
double = ColumnType("double[pyarrow]")
string = ColumnType("str[pyarrow]")
boolean = ColumnType("bool[pyarrow]")
date = ColumnType("date64[pyarrow]")
datetime_naive = ColumnType("datetime64[ns]")

def datetime_tz(tz=str):
    """
    Return datetime_tz type for a given timezone.
    Will re represented as a numpy type, for intance
    if "UTC" is given, then "datetime64[ns, UTC}"
    will be used.
    """
    return ColumnType(f"datetime64[ns, {tz}]")


