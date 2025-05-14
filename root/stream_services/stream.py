import os
from datetime import datetime, timedelta
import logging
from utilities.utils_function import is_file_stable
from typing import Dict, Tuple, List
logger = logging.getLogger(__name__)


class TimeManager:
    def __init__(self, current_date: str, current_hour: int) -> None:
        self.current_date: str = current_date
        self.current_hour: int = current_hour

    def update_if_needed(self) -> None:
        """Updates the time if the hour has changed."""
        if datetime.now().hour != self.current_hour:
            self.increment_hour()
            return True

    def increment_hour(self) -> None:
        """Increments the current hour or moves to the next day if the hour exceeds 23."""
        if self.current_hour < 23:
            self.current_hour += 1
        else:
            date_obj: datetime = datetime.strptime(self.current_date, "%Y-%m-%d")
            next_date: datetime = date_obj + timedelta(days=1)
            self.current_date = next_date.strftime("%Y-%m-%d")
            self.current_hour = 0

    def get_current_path(self, base_dir: str) -> str:
        """Generates the full path based on the current date and hour."""
        return os.path.join(base_dir, self.current_date, str(self.current_hour).zfill(2))


class Stream:
    def __init__(self, main_dir: str, date: str, hour: int) -> None:
        self.main_dir: str = main_dir
        self.time_manager: TimeManager = TimeManager(date, hour)
        self.cache_current_files: Dict[str, str] = {}

    def stream(self) -> Tuple[Dict[str, str], str, int]:
        """Streams the files, checking if new ones are available, and returns new files along with current date and hour."""
        while True:
            hour_changed = self.time_manager.update_if_needed()
            if hour_changed:
                self.cache_current_files.clear()
            full_path: str = self.time_manager.get_current_path(self.main_dir)

            files: list[str] = self.get_files(full_path)
            if not self.files_exist(files):
                continue

            new_files: Dict[str, str] = self.get_new_files(files, full_path)
            if new_files:
                return new_files, self.time_manager.current_date, self.time_manager.current_hour

    def get_files(self, path: str) -> list[str]:
        """Returns a list of files in the given path."""
        return os.listdir(path) if os.path.exists(path) else []

    def files_exist(self, files: list[str]) -> bool:
        """Checks if any files exist."""
        return bool(files)

    def get_new_files(self, files: List[str], path: str) -> Dict[str, str]:
        """Filters and returns stable, uncached new files."""
        new_files: Dict[str, str] = {}
        for file in files:
            if file not in self.cache_current_files:
                file_path = os.path.join(path, file)
                if is_file_stable(file_path):
                    self.cache_current_files[file] = file_path
                    new_files[file] = file_path
        return new_files

    def remove_from_cache(self, file: str) -> bool:
        """Removes a file from the cache if it exists."""
        if file in self.cache_current_files:
            del self.cache_current_files[file]
            return True
        return False
