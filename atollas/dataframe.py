from typing import Callable, Tuple

import pandas as pd

from atollas.dataframe_schema import Schema, merge_schemas
from atollas.types import unique
from atollas.aggregations import Aggregation


class DataFrame:
    def __init__(self, df: pd.DataFrame, schema: Schema):
        self.df = df
        self.schema = schema

    def to_pandas(self):
        return self.df.copy()

    def enforce_schema(self, detailed=True) -> "DataFrame":
        schema_dict = self.schema.to_dict()
        if missing_columns := [i for i in schema_dict if i not in self.df.columns]:
            raise TypeError(
                f"{', '.join(missing_columns)} not present in returned df",
            )
        self.df = self.df[list(schema_dict)].astype(
            {k: v.representation for k, v in schema_dict.items()}
        )
        if not detailed:
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

    def assign(self, schema: Schema, **kwargs) -> "DataFrame":
        return DataFrame(
            df=self.df.assign(**kwargs),
            schema=self.schema + schema,
        ).enforce_schema()

    def filter(self, expression: Callable) -> "DataFrame":
        return DataFrame(
            df=self.df.loc[expression],
            schema=self.schema,
        )

    def pipe(self, func, schema: Schema, *args, **kwargs) -> "DataFrame":
        return DataFrame(
            df=self.df.pipe(func, *args, **kwargs),
            schema=schema,
        ).enforce_schema()

    def rename(self, columns) -> "DataFrame":
        return DataFrame(
            df=self.df.rename(columns=columns),
            schema=Schema(**{k: v for k, v in self.schema.to_dict().items()}),
        )

    def drop(self, columns: list[str], errors: str = "raise") -> "DataFrame":
        return DataFrame(
            df=self.df.drop(columns=columns, errors=errors),
            schema=Schema(
                **{k: v for k, v in self.schema.to_dict().items() if k not in columns}
            ),
        )

    def astype(self, schema: Schema):
        return DataFrame(
            df=self.df,
            schema=schema,
        ).enforce_schema()

    def dropna(self, how: str = "any", subset: list[str] | None = None) -> "DataFrame":
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
        Groupby and Aggregate function
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
        raise NotImplementedError

    def __str__(self):
        return str(self.df) + "\n\n" + "\n".join(f"{k}: {v}" for k, v in self.schema)

    def __repr__(self):
        return self.__str__()
