from typing import Dict, Any, Callable, Union

import pandas as pd

from atollas import DataFrame
from atollas import types as at
from atollas.dataframe_schema import Schema


def _input_wrapper(pandas_function: Callable, filetype: str):
    def wrapped_function(
        io: Any, schema: Union[Dict[str, at.ColumnType], Schema], **kwargs
    ):
        f"""
        Input function for {filetype} ingestion.

        Additional keyword arguments are passed into pandas read {filetype}
        function.
        """
        df = pandas_function(io, **kwargs)
        if isinstance(schema, dict):
            schema = Schema(**schema)
        return DataFrame(df, schema).enforce_schema()

    return wrapped_function


read_csv = _input_wrapper(pd.read_csv, "csv")
read_parquet = _input_wrapper(pd.read_parquet, "parquet")
read_excel = _input_wrapper(pd.read_excel, "excel")
read_json = _input_wrapper(pd.read_json, "json")
json_normalize = _input_wrapper(pd.json_normalize, "json")
read_html = _input_wrapper(pd.read_html, "html")
read_xml = _input_wrapper(pd.read_xml, "xml")
read_feather = _input_wrapper(pd.read_feather, "feather")
read_sql = _input_wrapper(pd.read_sql, "sql")
read_orc = _input_wrapper(pd.read_orc, "orc")
read_sas = _input_wrapper(pd.read_sas, "sas")
read_stata = _input_wrapper(pd.read_stata, "stata")
read_spss = _input_wrapper(pd.read_spss, "spss")
read_gbq = _input_wrapper(pd.read_gbq, "gbq")
read_fwf = _input_wrapper(pd.read_fwf, "fixed width file")
