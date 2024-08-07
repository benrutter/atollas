If you know pandas, you're in pretty good stead for atollas (if not, start there first!)

The main thing to get used to with atollas, is that everything is chained through functions, meaning they're a few differences from pandas to atollas. For instance, "loc" which works like this in pandas:

```python
df.loc[lambda df: df["a"] > 4]
```

has a function equivalent in atollas that works much the same way:

```python
df.filter(lambda df: df["a"] > 4)
```

For the most part, learning atollas, from pandas, is just a case of knowing what pandas operations have an equivalent (such as loc). Since atollas is just pandas operations as its heart, wherever possible, methods have the exact same names and keyword arguments.

## Schemas

Schemas will seem a little new, there *is* a schema class in atollas, but you rarely need to use it. For instance, whilst this is a perfectly valid operation:

```python
import atollas as atl
from atollas import unique, integer, string
from atollas.dataframe_schema import Schema

schema = Schema(a=unique(integer), b=string)
df = (
  atl.read_parquet(
    "somefile.parquet",
    schema,
)
```

You don't actually need to bother, and using dictionaries will work as long as the values are atollas types:

```python
import atollas as atl
from atollas import unique, integer, string
from atollas.dataframe_schema import Schema

schema = Schema(a=unique(integer), b=string)
df = (
  atl.read_parquet(
    "somefile.parquet",
    {"a": unique(integer), "b": string},
)
```

Aside from that, have fun! Atollas is a *very early stage* and *experimental* project, and there are probably a lot of sharp edges that haven't been discovered yet. Please raise an issue if you bump into one!
