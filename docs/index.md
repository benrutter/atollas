# atollas 🪼

A column level typed runner for pandas workflows.

## ⚠️ Warning

This is still very *pre-alpha and experimental*, have fun experimenting with it, but it isn't ready for production workflows just yet.


## Motivation

Pandas is an amazing tool for interactive data analysis - but that's a little at odds with reliability in a production setting. Atollas is designed to be a type checking container for pandas workflows, to allow greater reliability in production.

Let's work through this series of operations and consider things that could possibly throw run-time errors:

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
import atollas as atl
from atollas import unique, integer, string, double, datetime_tz
from atollas.aggregations import Sum

spot_the_fail = (
  atl.read_csv(
    "some_file.csv",
    schema={"col_1": unique(integer), "col_2": string, "col_3": double},
  )  # <- specified schema is incorrect
  .aggregate(
    by=["col_1", "col_2"],
    col_3=Sum("col_3"),
  )
  .merge(
    atl.read_parquet(
       "another_file.parquet",
       schema={"col_1": unique(integer), "col_4": datetime_tz("UTC")},
    ),  # <- specified schema is incorrect
    cardinality="one-to-one",
    on="col_1",
  )
  .filter_columns(["col_1", "col_2", "col_3"])
)
```

We have to bear some additional boilerplate, but our code is now a lot more predictable, huzzah! Note that IO operations are the only ones that can fail indeterminately now. This is the core idea behind `atollas`: If something runs *once*, it should *run* everytime, only throwing errors at io boundaries.

This is a massive benefit for production workflows, because all errors are reduced to something like:

```
atollas.read_csv raise TypeError:
  file "some_file.csv" did not match specified type,
  "col_1" is meant to be unique, but has duplicate records
```

Which means we can track down issues a *lot* faster.

Atollas moniters types at read time, and additionally will raise errors when operations are incorrect for the specified schema. For instance, if merging with `one-to-one` cardinality, both sides of the merge operation must be unique (i.e. a `unique(integer)` type rather than simply `integer`). This helps you catch errors you didn't even think of at development time, neato-burrito!


## Comparison to Pandera

The most similar project (at least that I know about) is `pandera` - `pandera` is a really awesome, and much more developed project that `atollas`. It's probably the most established tool for schema enforcement of type level dataframes, but is a little different in aims from Atollas.

`pandera` gives you decorators and function to check schemas at various points, but your code is still essentially pandas code. Whilst `atollas` tries to provide a wrapper for additional column level type safety.

