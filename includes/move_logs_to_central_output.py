import os
import logging

from helpers.copy_and_move_with_progress import move_with_progress
from config import LOGS_DIR
from helpers.logging_utils import LOG_FILE, ERROR_LOG_FILE
from helpers.remove_path import remove_path

def move_logs_to_central_output():
    try:
        logging.shutdown()
        
        # Move log files now that handlers are released
        move_with_progress(LOG_FILE, os.path.join(LOGS_DIR, os.path.basename(LOG_FILE)))
        # print(f"Moved main log to {LOGS_DIR}")

        if os.path.exists(ERROR_LOG_FILE):
            if os.path.getsize(ERROR_LOG_FILE) > 0:
                move_with_progress(ERROR_LOG_FILE, os.path.join(LOGS_DIR, os.path.basename(ERROR_LOG_FILE)))
                # print(f"Moved error log to {LOGS_DIR}")
            else:
                remove_path(ERROR_LOG_FILE)
                # print(f"Deleted empty error log: {ERROR_LOG_FILE}")
    except Exception as e:
        print(f"Failed to move logs to {LOGS_DIR}: {e}")
