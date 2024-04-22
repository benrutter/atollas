# atollas 🪼

A follow along column-level typed runner for pandas workflows.

## ⚠️ Warning

This is still very *pre-alpha and experimental*, have fun experimenting with it, but it isn't ready for production workflows just yet.

## Motivation

Let's work through this series of operations and consider things that could possibly throw errors:

```python
import pandas as pd

spot_the_fail = (
  pd.read_csv("some_file.csv")  # <- file doesn't exist (obviously)
  .groupby(["col_1", "col_2"]) # <- columns aren't in dataframe
  .agg({"col_3": "sum"}) # <- column not in dataframe, or not of summable type
  .merge(
    pd.read_parquet("another_file.parquet"),  # <- file doesn't exist
    on="col_1",  # <- col_1 not of matching type
  )
  [["col_1", "col_2", "col_4"]]  # <- col_4 not resent in joined dataframe
                                 # <- col_2 also present in joined dataframe
                                 #    so now col_2_x and col_2_y exist but not
                                 #    col_2
)
```

That's also not factoring in items that might confuse business logic or otherwise cause
problems, for instance if col_1 has meant to be unique across both dataframes, but has duplicates in one or both, records would be duplicated.

Essentially, all these errors are problems with the data not matching our implicit assumptions. This is a natural consequence of the fact that pandas is designed for exploration and experimentation, and not column level type safety.

*Atollas* is a column-level type safe runner for pandas, the above code using Atollas would look like this:

```python
import atollas
import atollas.types as at
import atollas.aggregations as ag

spot_the_fail = (
  atollas.read_csv(
    "some_file.csv",
    schema={"col_1": at.unique(at.integer), "col_2": at.string, "col_3": at.double},
  )  # <- specified schema is incorrect
  aggregate(
    by=["col_1", "col_2"],
    col_3=ag.Sum("col_3"),
  )
  .merge(
    atollas.read_parquet(
       "another_file.parquet",
       schema={"col_1": at.unique(at.integer), "col_4": at.datetime_tz("UTC")},
    ),  # <- specified schema is incorrect
    cardinality="one-to-one",
    on="col_1",
  )
  .filter_columns("col_1", "col_2", "col_3")
)
```

Note that we have to take on more lines of code, because we're having to express not just what we're doing with the data, but also *what we're expecting the data to look like*. The upshot of that, is there are now only two possible places for runtime errors - when we're reading in the csv, and when we're reading in the parquet.

This is a massive benefit for production workflows, because all dataframe errors are reduced to something like:

```
atollas.read_csv raise TypeError:
  file "some_file.csv" did not match specified type,
  "col_1" is meant to be unique, but has duplicate records
```

Which means we can quickly track down any issues.

Atollas monitors the types at read time, and will also raise an error if we try to carry out operations that are incorrect for our given schema, for instance, we can only merge with a "one-to-one" cardinality because `col_1` is unique in both dataframes (as indicated by the `at.unique(at.integer)` type, if that hadn't been specified, atollas would throw an error during testing or any run time telling us those instructions aren't valid.

