import logging
import datetime
import os

from config import args, LOGS_DIR
from helpers.copy_and_move_with_progress import move_with_progress

class ErrorWarningFilter(logging.Filter):
    def filter(self, record):
        return record.levelno >= logging.WARNING

LOG_FILE = None
ERROR_LOG_FILE = None

def setup_logging():
    global LOG_FILE, ERROR_LOG_FILE

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    LOG_FILE = f'./av1_job_{timestamp}.log'
    ERROR_LOG_FILE = f'./errors_av1_job_{timestamp}.log'

    # Remove old handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # General log file handler
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setLevel(logging.DEBUG if args.debug else logging.INFO)
    file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))

    # Error/Warning file handler
    error_handler = logging.FileHandler(ERROR_LOG_FILE)
    error_handler.setLevel(logging.WARNING)
    error_handler.addFilter(ErrorWarningFilter())
    error_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))

    # Console output handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG if args.debug else logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))

    logging.getLogger().setLevel(logging.DEBUG if args.debug else logging.INFO)
    logging.getLogger().handlers = [file_handler, error_handler, console_handler]

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
                os.remove(ERROR_LOG_FILE)
                # print(f"Deleted empty error log: {ERROR_LOG_FILE}")
    except Exception as e:
        print(f"Failed to move logs to {LOGS_DIR}: {e}")
