import abc
from pandas import NamedAgg

from atollas.types import ColumnType

class Aggregation(abc.ABC):
    def __init__(self, column: str):
        self.column = column

    @abc.abstractmethod
    def output_type(self, input: ColumnType) -> ColumnType:
        ...

    @abc.abstractmethod
    def pandas_aggregation(self) -> NamedAgg:
        ...

class SimpleAggregation(Aggregation):
    @abc.abstractproperty
    def func_name(self):
        pass

    def output_type(self, input: ColumnType) -> ColumnType:
        return input

    def pandas_aggregation(self) -> NamedAgg:
        return NamedAgg(column=self.column, aggfunc=self.func_name)

class Max(SimpleAggregation):
    func_name = "max"

class Min(SimpleAggregation):
    func_name = "min"

class Mean(SimpleAggregation):
    func_name = "mean"

class Median(SimpleAggregation):
    func_name = "median"

class Mode(SimpleAggregation):
    func_name = "mode"

class First(SimpleAggregation):
    func_name = "first"

class Last(SimpleAggregation):
    func_name = "last"

class Sum(SimpleAggregation):
    func_name = "sum"
