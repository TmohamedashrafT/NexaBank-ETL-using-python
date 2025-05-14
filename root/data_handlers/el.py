import csv
import json
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from io import BytesIO
from typing import Union, Callable, Any


class HdfsEL:
    def __init__(self, hdfs_client: Any) -> None:
        self.hdfs_client = hdfs_client

    def extract(self) -> None:
        pass

    def load_from_dataframe_as_parquet(
        self,
        df: pd.DataFrame,
        file_path: str,
        overwrite: bool = True
    ) -> None:
        table: pa.Table = pa.Table.from_pandas(df)
        buffer: BytesIO = BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)
        with self.hdfs_client.write(file_path, overwrite=overwrite) as f:
            f.write(buffer.read())


class DatalakeEl:
    def __init__(self, credentials: Union[None, dict] = None):
        self.credentials = credentials

    def open_file(self, reader_function: Callable[[Any], Any], file_path: str, mode: str = 'r', **kwargs) -> Any:
        with open(file_path, mode=mode, encoding='utf-8') as file:
            return reader_function(file, **kwargs)

    def extract_from_file(self, file_path: str, mode: str, as_dataframe: bool, delimiter: str, **kwargs) -> Union[list, pd.DataFrame]:
        def file_reader(file, delimiter: str, **kwargs):
            reader = csv.reader(file, delimiter=delimiter)
            data = list(reader)
            return pd.DataFrame(data[1:], columns=data[0], **kwargs) if as_dataframe else data

        return self.open_file(file_reader, file_path, mode, delimiter=delimiter, **kwargs)

    def extract_from_csv(self, file_path: str, mode: str = 'r', as_dataframe: bool = True, delimiter: str = ',', **kwargs) -> Union[list, pd.DataFrame]:
        return self.extract_from_file(file_path, mode, as_dataframe, delimiter, **kwargs)

    def extract_from_txt(self, file_path: str, mode: str = 'r', as_dataframe: bool = True, **kwargs) -> Union[list, pd.DataFrame]:
        delimiter = kwargs.pop("delimiter", "|")
        return self.extract_from_file(file_path, mode, as_dataframe, delimiter, **kwargs)

    def extract_from_json(self, file_path: str, mode: str = 'r', as_dataframe: bool = True, **kwargs) -> Union[dict, pd.DataFrame]:
        def json_reader(file, **kwargs):
            return json.load(file)

        data = self.open_file(json_reader, file_path, mode, **kwargs)
        return pd.DataFrame(data, **kwargs) if as_dataframe else data

    def extract_from_datalake(self, file_path: str, as_dataframe: bool = True, **kwargs) -> Union[list, dict, pd.DataFrame]:
        if file_path.endswith(".csv"):
            return self.extract_from_csv(file_path, as_dataframe=as_dataframe, **kwargs)
        elif file_path.endswith(".json"):
            return self.extract_from_json(file_path, as_dataframe=as_dataframe, **kwargs)
        elif file_path.endswith(".txt"):
            return self.extract_from_txt(file_path, as_dataframe=as_dataframe, **kwargs)
        else:
            raise ValueError(f"Unsupported file type: {file_path}")

    def load(self, **kwargs):
        pass

