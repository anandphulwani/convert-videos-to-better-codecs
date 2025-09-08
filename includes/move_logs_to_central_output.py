import os
import logging

from helpers.copy_and_move_with_progress import move_with_progress
from config import LOGS_DIR
from helpers.logging_utils import redirect_logs_to_new_file, log_files_shared_state
from helpers.remove_path import remove_path

def move_logs_to_central_output(event_queue, restart_logging = False):
    try:
        current_log_file = log_files_shared_state.LOG_FILE
        current_error_file = log_files_shared_state.ERROR_LOG_FILE
        remote_log_file = os.path.join(LOGS_DIR, os.path.basename(current_log_file))
        if restart_logging:
            redirect_logs_to_new_file()
        move_with_progress(current_log_file, remote_log_file, event_queue)

        if os.path.exists(current_error_file):
            if os.path.getsize(current_error_file) > 0:
                move_with_progress(current_error_file, os.path.join(LOGS_DIR, os.path.basename(current_error_file)), event_queue)
            else:
                remove_path(current_error_file)
    except Exception as e:
        print(f"Failed to move logs to {LOGS_DIR}: {e}")
