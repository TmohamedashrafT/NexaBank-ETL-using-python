import logging
import os
import time

def setup_logger():
    # Define global format
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s => %(message)s'
    )

    # Create file handler
    file_handler = logging.FileHandler('ETL.log')
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

def is_file_stable(file_path, wait_time=1):
    initial_size = os.path.getsize(file_path)
    initial_time = os.path.getmtime(file_path)

    time.sleep(wait_time)

    current_size = os.path.getsize(file_path)
    current_time = os.path.getmtime(file_path)

    return current_size == initial_size and current_time == initial_time

def load_english_words(file_path):
    with open(file_path,'r') as f:
        words = set()
        for word in f:
            return  word.strip().lower().split()