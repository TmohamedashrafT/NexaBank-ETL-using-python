import logging
import os.path
from typing import Dict, Any

logger = logging.getLogger(__name__)


def extract_all_tables(datalake_el_obj: Any, files: Dict[str, str]) -> Dict[str, Any]:
    """
    Extracts all tables from the given files, processes them, and returns a dictionary of dataframes.

    :param datalake_el_obj: Object responsible for extracting data from the datalake.
    :param files: Dictionary where keys are filenames and values are file paths.
    :return: Dictionary of extracted dataframes with table names as keys.
    """
    dict_of_dataframes: Dict[str, Any] = {}

    for file, path in files.items():
        logger.info("Processing file: %s", file)

        # Extract table name from the file (assuming the file name is the table name)
        table_name = file.split(".")[0]

        try:
            logger.info("Extracting data for table: %s", table_name)
            dataframe = datalake_el_obj.extract_from_datalake(path)
            dict_of_dataframes[table_name] = dataframe

            logger.info("Data extracted successfully for table: %s with %d rows", table_name, dataframe.shape[0])

        except Exception as e:
            logger.error("Failed to extract data for table %s from file %s. Error: %s", table_name, file, str(e))

    return dict_of_dataframes


def apply_checks_on_all_tables(data_quality_obj: Any, dict_of_dataframes: Dict[str, Any], date: str, hour: int, email_sender_obj: Any, recipient_email: str) -> Dict[
    str, Any]:
    """
    Applies data quality checks on all tables in the dictionary and returns the filtered tables.

    :param data_quality_obj: Object responsible for applying data quality checks.
    :param dict_of_dataframes: Dictionary containing table names and dataframes.
    :param date: Current date string.
    :param hour: Current hour as integer.
    :return: Dictionary with table names and dataframes after applying checks.
    """
    dict_of_checked_dataframes: Dict[str, Any] = {}

    for table_name, dataframe in dict_of_dataframes.items():
        rows_before = dataframe.shape[0]
        logger.info("Applying data quality checks for table: %s with %d rows before checks", table_name, rows_before)

        try:
            results = data_quality_obj.apply_checks(dataframe, table_name, date, hour)

            if not results.empty:
                dict_of_checked_dataframes[table_name] = results
                rows_after = results.shape[0]
                logger.info("Data quality checks filtered table: %s. %d rows passed the checks, %d rows after filter",
                            table_name, rows_after, rows_after)
            else:
                logger.info("Data quality checks filtered all rows for table: %s. %d rows filtered out", table_name,
                            rows_before)

        except Exception as e:
            error_msg = f"Error applying data quality checks for table {table_name}. Error: {str(e)}"
            subject = f'Data Quality Check Failed - {table_name}'
            logger.error(error_msg)
            email_sender_obj.send_email(recipient_email, subject, error_msg)

    return dict_of_checked_dataframes


def apply_transformations_on_all_tables(transformer_obj: Any, dict_of_dataframes: Dict[str, Any]) -> Dict[str, Any]:
    """
    Applies transformations to all tables and returns the transformed dataframes.

    :param transformer_obj: Object responsible for applying transformations.
    :param dict_of_dataframes: Dictionary containing table names and dataframes.
    :return: Dictionary with transformed dataframes.
    """
    dict_of_transformed_dataframes: Dict[str, Any] = {}

    for table_name, dataframe in dict_of_dataframes.items():
        rows_before = dataframe.shape[0]
        logger.info("Starting transformation for table: %s with %d rows", table_name, rows_before)

        try:
            transformed_dataframe = transformer_obj.run_transform(table_name, dataframe)
            rows_after = transformed_dataframe.shape[0]
            dict_of_transformed_dataframes[table_name] = transformed_dataframe
            logger.info("Transformation complete for table: %s. %d rows after transformation", table_name, rows_after)

        except Exception as e:
            logger.error("Error applying transformation for table %s. Error: %s", table_name, str(e))

    return dict_of_transformed_dataframes


def save_all_dataframes(hdfsEL_obj: Any, dataframes: Dict[str, Any], hdfs_writing_path: str, date: str, hour: int) -> None:
    """
    Saves all dataframes to HDFS.

    :param hdfsEL_obj: Object responsible for loading data to HDFS.
    :param dataframes: Dictionary containing table names and dataframes.
    :param date: Current date string.
    :param hour: Current hour as integer.
    """
    success_count = 0

    for key, df in dataframes.items():
        hdfs_full_path = os.path.join(hdfs_writing_path, key, 'data_' + date + '_' + str(hour) + '.parquet')
        logger.info("Starting to save table: %s to HDFS path: %s at %s %s", key, hdfs_full_path, date, hour)

        try:
            hdfsEL_obj.load_from_dataframe_as_parquet(df, hdfs_full_path)
            logger.info("Successfully saved table: %s to HDFS path: %s at %s %s", key, hdfs_full_path, date, hour)
            success_count += 1
        except Exception as e:
            logger.error("Error saving table: %s to HDFS path: %s at %s %s. Error: %s", key, hdfs_full_path, date, hour,
                         str(e))
    return success_count