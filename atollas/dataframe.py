from typing import Callable, Tuple, Union, Dict

import pandas as pd

from atollas.dataframe_schema import Schema, merge_schemas
from atollas.types import unique, ColumnType
from atollas.aggregations import Aggregation


def _output_wrapper(pandas_function: Callable, filetype: str):
    def wrapped_function(self, path: str, **kwargs) -> "DataFrame":
        f"""
        Output function for storing dataframe via {filetype}.

        Additional keyword arguments are passed into pandas to {filetype} function.
        """
        pandas_function(self.df, path, **kwargs)
        return self

    return wrapped_function


class DataFrame:
    def __init__(self, df: pd.DataFrame, schema: Union[Dict[str, ColumnType], Schema]):
        """
        Initialise atollas dataframe based on a pandas dataframe and schema
        """
        if isinstance(schema, dict):
            schema = Schema(**schema)
        self.df = df
        self.schema = schema

    def to_pandas(self):
        """
        Convert types atollas dataframe into an ordinary pandas dataframe
        """
        return self.df.copy()

    def enforce_schema(self, full_check=True) -> "DataFrame":
        """
        Ensure schema is accurate, will convert types where possible (such as float
        to doubles) and will drop columns that are not present in schema.

        If full_check is true (as per default) it will also ensure non-null
        and uniqueness values.
        """
        schema_dict = self.schema.to_dict()
        if missing_columns := [i for i in schema_dict if i not in self.df.columns]:
            raise TypeError(
                f"{', '.join(missing_columns)} not present in returned df",
            )
        try:
            self.df = self.df[list(schema_dict)].astype(
                {k: v.representation for k, v in schema_dict.items()}
            )
        except ValueError as exception:
            message = (
                "The given datatypes are different from as they have been "
                "specified in the schema, and atollas is not able to enforce "
                "or convert them into their expected type.\n\n"
                f"Actual types: {self.df.dtypes.to_dict()}\n"
                f"Expected schema: { {k: v.representation for k, v in self.schema} }"
            )
            raise ValueError(message) from exception
        if not full_check:
            return self
        for non_nullable_column in [
            k for k, v in schema_dict.items() if not v.nullable
        ]:
            if any(self.df[non_nullable_column].isnull()):
                raise TypeError(
                    f'Column "{non_nullable_column}" contains nulls but is typed as non-nullable'
                )
        for unique_column in [k for k, v in schema_dict.items() if v.unique]:
            if any(self.df[unique_column].duplicated()):
                raise TypeError(
                    f'Column "{non_nullable_column}" contains duplicates but is typed as unique'
                )

        return self

    def assign(
        self, schema: Union[Dict[str, ColumnType], Schema], **kwargs
    ) -> "DataFrame":
        """
        Assign columns into the dataframe. See pandas documentation on assign
        for further details.

        Atollas also requires a "schema" argument of the output schema from
        after the operation.
        """
        if isinstance(schema, dict):
            schema = Schema(**schema)
        return DataFrame(
            df=self.df.assign(**kwargs),
            schema=self.schema + schema,
        ).enforce_schema()

    def filter(self, expression: Callable) -> "DataFrame":
        """
        Filters a dataframe based on a given expression (function taking a pandas
        dataframe)

        For example:

        ```python
        df.filter(lambda df: df.some_column == 8)
        ```

        Will filter the dataframe to only entries where "some_column" is equal
        to 8. This is the equivalent of the following pandas code:

        ```python
        df.loc[lambda df: df.some_column == 8]
        ```
        """
        return DataFrame(
            df=self.df.loc[expression],
            schema=self.schema,
        )

    def pipe(
        self,
        func: Callable,
        schema: Union[Dict[str, ColumnType], Schema],
        *args,
        **kwargs,
    ) -> "DataFrame":
        """
        Pipe is an escape hatch for when you either want to:
            - Wrap up more complicated logic into a single line for readability
            - Execute pandas code that isn't supported within attolas

        The given function will be called on the *pandas* (i.e. not atollas) dataframe.

        Note, that you'll need to also give a schema of the output, so that attolas
        can understand what to expect after the fact. Atollas will automatically enforce
        the given schema after the pipe call, and raise an error if not possible.
        """
        if isinstance(schema, dict):
            schema = Schema(**schema)
        return DataFrame(
            df=self.df.pipe(func, *args, **kwargs),
            schema=schema,
        ).enforce_schema()

    def rename(self, columns: dict) -> "DataFrame":
        """
        Rename columns in dataframe
        """
        new_schema = {k: v if k not in columns else columns[k] for k, v in self.schema}
        return DataFrame(
            df=self.df.rename(columns=columns),
            schema=Schema(**new_schema),
        )

    def drop(self, columns: list[str], errors: str = "raise") -> "DataFrame":
        """
        Drop columns from dataframe
        """
        return DataFrame(
            df=self.df.drop(columns=columns, errors=errors),
            schema=Schema(
                **{k: v for k, v in self.schema.to_dict().items() if k not in columns}
            ),
        )

    def astype(self, schema: Union[Dict[str, ColumnType], Schema]):
        """
        Will convert the type of columns based on the given schema
        """
        if isinstance(schema, dict):
            schema = Schema(**schema)
        schema |= {k: v for k, v in self.schema if k not in schema}
        return DataFrame(
            df=self.df,
            schema=schema,
        ).enforce_schema()

    def dropna(self, how: str = "any", subset: list[str] | None = None) -> "DataFrame":
        """
        Will drop null fields form the dataframe.
        """
        new_schema = {}
        for k, v in self.schema.to_dict().items():
            if subset is None or k in subset:
                setattr(v, "nullable", False)
            new_schema[k] = v
        return DataFrame(
            df=self.df.dropna(how=how, subset=subset),
            schema=Schema(**new_schema),
        )

    def merge(
        self,
        right: "DataFrame",
        cardinality: str,
        how: str = "inner",
        on: str | list[str] | None = None,
        left_on: str | list[str] = None,
        right_on: str | list[str] = None,
        suffixes: Tuple[str, str] = ("_x", "_y"),
    ) -> "DataFrame":
        """
        Join operation for two attolas dataframes.

        `on` keyword will overwrite any "left_on" and "right_on" arguments.

        `cardinality` keyword must be one of the following:
            - "many-to-many"
            - "one-to-many"
            - "many-to-one"
            - "one-to-one"
        """
        if on:
            left_on, right_on = on, on
        left_on = [left_on] if isinstance(left_on, str) else left_on
        right_on = [right_on] if isinstance(right_on, str) else right_on
        if not on and not (left_on and right_on):
            raise ValueError("either 'on' or 'left_on' and 'right_on' must be given")
        if cardinality not in [
            "many-to-many",
            "one-to-many",
            "many-to-one",
            "one-to-one",
        ]:
            error_message: str = (
                f"Cardinality of {cardinality} is not valid. "
                "must be one of 'many-to-many', 'one-to-many', 'many-to-one'"
                " or 'one-to-one'"
            )
            raise ValueError(error_message)
        left_cardinality, _, right_cardinality = cardinality.split("-")
        if left_cardinality == "one":
            for column, column_type in self.schema:
                if column not in left_on:
                    continue

                if not column_type.unique or column_type.nullable:
                    error_message: str = (
                        f"Type of column {column} is invalid for a one-to-x "
                        "join (must be unique and non-nullable)"
                    )
                    raise ValueError(error_message)
        if right_cardinality == "one":
            for column, column_type in self.schema:
                if column not in right_on:
                    continue
                if not column_type.unique or column_type.nullable:
                    error_message: str = (
                        f"Type of column {column} is invalid for a x-to-one "
                        "join (must be unique and non-nullable)"
                    )
        new_schema = merge_schemas(
            self.schema, right.schema, left_on, right_on, suffixes[0], suffixes[1]
        )
        return DataFrame(
            self.df.merge(
                right.df, how=how, left_on=left_on, right_on=right_on, suffixes=suffixes
            ),
            schema=new_schema,
        )

    def aggregate(self, by=list[str] | str, **kwargs: Aggregation) -> "DataFrame":
        """
        Will aggregate a dataframe (see attolas.aggregations for possible options)

        by keyword must be given as a group of fields to groupby.

        Any additional keyword arguments are the names of fields, and must be an
        atollas aggregation object.
        """
        if isinstance(by, str):
            by = [by]
        new_df = self.df.groupby(by, as_index=False).agg(
            **{k: v.pandas_aggregation() for k, v in kwargs.items()}
        )
        new_schema = {}
        for column in by:
            if len(by) == 1:
                new_schema[column] = unique(self.schema[column])
            else:
                new_schema[column] = self.schema[column]
        for column, aggregation in kwargs.items():
            new_schema[column] = aggregation.output_type(
                self.schema[aggregation.column]
            )

        return DataFrame(
            new_df,
            schema=Schema(**new_schema),
        )

    def filter_columns(self, columns: list[str]) -> "DataFrame":
        """
        Filter columns to a given subset.

        Note that `df.filter_columns(["a", "b"])` is the atollas equivalent
        of pandas' `df.loc[["a", "b"]]`
        """
        new_df = self.df[columns]
        new_schema = {k: v for k, v in self.schema if k in columns}
        return DataFrame(new_df, schema=Schema(**new_schema))

    to_csv = _output_wrapper(pd.DataFrame.to_csv, "csv")
    to_json = _output_wrapper(pd.DataFrame.to_json, "json")
    to_html = _output_wrapper(pd.DataFrame.to_html, "html")
    to_latex = _output_wrapper(pd.DataFrame.to_latex, "latex")
    to_xml = _output_wrapper(pd.DataFrame.to_xml, "xml")
    to_excel = _output_wrapper(pd.DataFrame.to_excel, "excel")
    to_hdf = _output_wrapper(pd.DataFrame.to_hdf, "hdf")
    to_feather = _output_wrapper(pd.DataFrame.to_feather, "feather")
    to_parquet = _output_wrapper(pd.DataFrame.to_parquet, "parqet")
    to_orc = _output_wrapper(pd.DataFrame.to_orc, "orc")
    to_stata = _output_wrapper(pd.DataFrame.to_stata, "stata")
    to_sql = _output_wrapper(pd.DataFrame.to_sql, "sql")
    to_gbq = _output_wrapper(pd.DataFrame.to_gbq, "gbq")

    def __str__(self):
        return str(self.df) + "\n\n" + "\n".join(f"{k}: {v}" for k, v in self.schema)

    def __repr__(self):
        return self.__str__()
