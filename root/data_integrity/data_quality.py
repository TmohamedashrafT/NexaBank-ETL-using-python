from datetime import datetime
from typing import Any
import pandas as pd
import logging
logger = logging.getLogger(__name__)

class DataQuality:
    def __init__(self, schema: dict[str, list[str]]):
        self.schema = schema

    def check_columns(self, table_name: str, columns: list[str]) -> None:
        table_schema = self.schema.get(table_name)

        if len(columns) != len(table_schema):
            raise ValueError(
                f"Column count mismatch for table '{table_name}'. Expected {len(table_schema)}, got {len(columns)}."
            )
        for column in columns:
            if column not in table_schema:
                raise ValueError(
                    f"Unexpected column '{column}' found in table '{table_name}'."
                )


    def add_quality_columns(self, dataframe: Any, date: str, hour: int) -> Any:
        current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        dataframe['processing_time'] = current_date
        dataframe['partition_date'] = date
        dataframe['partition_hour'] = hour
        return dataframe

    def apply_checks(self, dataframe: Any, table_name: str, date: str, hour: int) -> Any:
        self.check_columns(table_name, dataframe.columns)
        dataframe = self.enforce_schema(dataframe, table_name)
        dataframe = self.add_quality_columns(dataframe, date, hour)
        return dataframe

    def enforce_schema(self, dataframe: pd.DataFrame, table_name: str) -> pd.DataFrame:
        dtypes = self.schema[table_name]
        for column, dtype in dtypes.items():
            try:
                dataframe[column] = dataframe[column].astype(dtype)
            except Exception as e:
                raise ValueError(f"Failed to cast column '{column}' to {dtype}: {e}")

        return dataframe
