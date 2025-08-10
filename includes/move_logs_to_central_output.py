import os
import logging

from helpers.copy_and_move_with_progress import move_with_progress
from config import LOGS_DIR
import helpers.logging_utils as logging_utils
from helpers.remove_path import remove_path

def move_logs_to_central_output(restart_logging = False):
    try:
        current_log_file = logging_utils.LOG_FILE
        current_error_file = logging_utils.ERROR_LOG_FILE
        remote_log_file = os.path.join(LOGS_DIR, os.path.basename(current_log_file))
        logging.shutdown()
        if restart_logging:
            logging_utils.setup_logging()

        move_with_progress(current_log_file, remote_log_file)

        if os.path.exists(current_error_file):
            if os.path.getsize(current_error_file) > 0:
                move_with_progress(current_error_file, os.path.join(LOGS_DIR, os.path.basename(current_error_file)))
            else:
                remove_path(current_error_file)
    except Exception as e:
        print(f"Failed to move logs to {LOGS_DIR}: {e}")
