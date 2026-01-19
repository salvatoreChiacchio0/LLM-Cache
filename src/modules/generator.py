import csv
import time
import os
from ..core.config import LOG_FILE, LOG_DELIMITER, FILE_ENCODING

class TrafficGenerator:
    def __init__(self, log_file=LOG_FILE):
        self.log_file = log_file
        self._check_file()

    def _check_file(self):
        if not os.path.exists(self.log_file):
            raise FileNotFoundError(f"Log file not found: {self.log_file}")

    def generate_events(self, shutdown_event=None):
        while True:
            with open(self.log_file, "r", encoding=FILE_ENCODING, errors="ignore") as f:
                print(f"[DEBUG] Generator opened log file: {self.log_file}")
                reader = csv.reader(f, delimiter=LOG_DELIMITER)
                try:
                    next(reader)
                except (StopIteration, EOFError):
                    pass

                for row in reader:
                    if shutdown_event and shutdown_event.is_set():
                        return
                    
                    if len(row) < 4:
                        continue

                    item, user, action = row[0], row[1], row[2]
                    timestamp = row[3]
                    
                    try:
                        item_id = int(item)
                        yield item_id, user, action, timestamp
                    except ValueError:
                        continue
