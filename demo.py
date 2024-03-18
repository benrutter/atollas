import pandas as pd

from typist_for_pandas import (
    dataframe_schema as s,
    types as t,
    dataframe as df,
)



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
tdf.assign(c=3, schema=s.Schema(c=t.integer))
print(tdf.df)
print(tdf.schema)
