import os
import json
import configparser
from concurrent.futures import ThreadPoolExecutor
import time
import logging

from stream_services.stream import Stream
from data_integrity.data_quality import DataQuality
from transformers.transformer import Transformer
from data_handlers.el import DatalakeEl, HdfsEL
from email_services.email_service import EmailSender
from db_connections.hdfs_connection import HdfsConnection
from utilities.utils_function import setup_logger
from pipeline import (
    extract_all_tables,
    apply_checks_on_all_tables,
    apply_transformations_on_all_tables,
    save_all_dataframes
)

# Setup logger
setup_logger()
logger = logging.getLogger(__name__)

# Load configuration
config = configparser.ConfigParser(allow_no_value=True)
config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.join('configs','config.ini')))
config.read(config_path)

main_dir = config['DataLake']['main_dir']
start_date = config['DataLake']['date']
start_hour = int(config['DataLake']['hour'])
schema = json.loads(config['DataLake']['src_tables_schema'])
hdfs_host = config['HDFS']['host']
hdfs_port = config['HDFS']['port']
hdfs_writing_path = config['HDFS']['writing_path']
recipient_email = config['email_service']['recipient_email']
smtp_server = config['email_service']['smtp_server']
smtp_port = config['email_service']['smtp_port']
logger.info("Configuration loaded: Main Dir=%s | Start Date=%s | Start Hour=%d", main_dir, start_date, start_hour)

# Initialize reusable objects
stream_obj = Stream(main_dir, start_date, start_hour)
data_quality_obj = DataQuality(schema)
datalake_el_obj = DatalakeEl()
transformer_obj = Transformer()
email_sender_obj = EmailSender(smtp_server, smtp_port)



logger = logging.getLogger(__name__)

def process_stream(new_files, date, hour, hdfs_el_obj, max_retries=3):
    """Process new files through the pipeline with retry logic."""
    for attempt in range(1, max_retries + 1):
        try:
            logger.info("Processing %d new files for date=%s hour=%d", len(new_files), date, hour)

            # Step 1: Extract
            dataframes = extract_all_tables(datalake_el_obj, new_files)
            logger.info("Extracted data for %d tables", len(dataframes))

            # Step 2: Data Quality Checks
            checked_dfs = apply_checks_on_all_tables(
                data_quality_obj,
                dataframes,
                date,
                hour,
                email_sender_obj,
                recipient_email
            )
            logger.info("Data quality checks passed for %d tables", len(checked_dfs))

            # Step 3: Transform
            transformed_dfs = apply_transformations_on_all_tables(transformer_obj, checked_dfs)
            logger.info("Transformations applied on %d tables", len(transformed_dfs))

            # Step 4: Save
            success_count = save_all_dataframes(hdfs_el_obj, transformed_dfs, hdfs_writing_path, date, hour)
            logger.info("Successfully saved %d out of %d transformed tables to HDFS", success_count, len(transformed_dfs))

            # Success case
            logger.info("Pipeline succeeded for %d tables", success_count)
            return  # Exit after a successful run

        except Exception as e:
            # Log the failure attempt and error details
            logger.error("Attempt %d: Pipeline failed - %s", attempt, str(e), exc_info=True)
            email_sender_obj.send_email(
                recipient_email,
                f"Pipeline Failure - Attempt {attempt}",
                f"Pipeline failed on attempt {attempt}:\n\n{str(e)}"
            )

            # Retry logic
            if attempt < max_retries:
                logger.info("Retrying in 5 seconds... (Attempt %d of %d)", attempt + 1, max_retries)
                time.sleep(5)
            else:
                logger.error("Pipeline failed after %d attempts.", max_retries)
                email_sender_obj.send_email(
                    recipient_email,
                    "Pipeline Failure - Max Retries Reached",
                    f"Pipeline failed after {max_retries} attempts for date={date} hour={hour}."
                )

if __name__ == '__main__':
    with HdfsConnection(hdfs_host, hdfs_port) as hdfs_client:
        hdfs_el_obj = HdfsEL(hdfs_client)

        with ThreadPoolExecutor(max_workers=5) as executor:
            while True:
                try:
                    new_files, date, hour = stream_obj.stream()
                    if new_files:
                        logger.info("Streamed %d new files: %s", len(new_files), new_files)
                        executor.submit(process_stream, new_files, date, hour, hdfs_el_obj)
                    else:
                        logger.info("No new files found. Sleeping for 10 seconds.")
                        time.sleep(10)
                except Exception as e:
                    logger.error("Error during streaming: %s", str(e), exc_info=True)
                    email_sender_obj.send_email(
                        recipient_email,
                        "Streaming Failure",
                        f"Error during streaming loop:\n\n{str(e)}"
                    )
                    time.sleep(10)
