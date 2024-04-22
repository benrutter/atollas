from datetime import datetime

import pandas as pd

from atollas import (
    dataframe_schema as s,
    types as t,
    dataframe as df,
)
from atollas.aggregations import Max, Min



print("enforcing a schema")
pdf = pd.DataFrame(
    {
        "a": [1, 2, 3],
        "b": ["cat", "bat", "hat"],
    },
)
tdf = df.DataFrame(pdf, s.Schema(a=t.integer, b=t.string))
tdf.enforce_schema()
print(tdf.df)


print("combining schemas")
one = s.Schema(a=t.double, b=t.datetime_naive)
two = s.Schema(c=t.date)
three = one + two
print(three)


print("throwing errors")
pdf = pd.DataFrame(
    {
        "a": [1, 2, 3, None],
        "b": ["cat", "bat", "hat", "bat"],
    },
)
tdf = df.DataFrame(pdf, s.Schema(a=t.nullable(t.integer), b=t.unique(t.string)))
try:
    tdf.enforce_schema()
except TypeError as e:
    print(e)


print("assign")
pdf = pd.DataFrame(
    {
        "a": [1, 2, 3, None],
        "b": ["cat", "bat", "hat", "bat"],
    },
)
tdf = df.DataFrame(pdf, s.Schema(a=t.nullable(t.integer), b=t.string))
tdf = tdf.assign(c=3, schema=s.Schema(c=t.integer))
print(tdf)


print("groupby")
pdf = pd.DataFrame(
    {
        "a": [1, 2, 3],
        "b": ["cat", "bat", "hat"],
    },
)
tdf = df.DataFrame(pdf, s.Schema(a=t.integer, b=t.string))
tdf = tdf.aggregate("b", cool=Max("a"))
print(tdf)


print("combining_everything")
pdf = pd.DataFrame(
    {
        "a": [1, 2, 3],
        "b": ["cat", "bat", "hat"],
    },
)
tdf2 = df.DataFrame(pd.DataFrame({"a": [2], "z": [datetime(2021, 3, 4)]}), schema=s.Schema(a=t.unique(t.integer), z=t.datetime_naive))
tdf = (
    df.DataFrame(pdf, s.Schema(a=t.unique(t.integer), b=t.unique(t.string)))
    .enforce_schema(detailed=True)
    .assign(c=3, schema=s.Schema(c=t.integer))
    .merge(tdf2, cardinality="one-to-one", how="left", on="a")
    .aggregate(by="a", nice=Max("b"), cool=Min("z"))
)
print(tdf)
