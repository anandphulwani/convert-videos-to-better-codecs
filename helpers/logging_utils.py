import logging
import datetime
from tqdm import tqdm

from config import args

LOG_FILE = None
ERROR_LOG_FILE = None

# --- Logging Setup ---
class ErrorWarningFilter(logging.Filter):
    def filter(self, record):
        return record.levelno >= logging.WARNING

def setup_logging():
    global LOG_FILE, ERROR_LOG_FILE

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    LOG_FILE = f'./av1_job_{timestamp}.log'
    ERROR_LOG_FILE = f'./errors_av1_job_{timestamp}.log'

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setLevel(logging.DEBUG if args.debug else logging.INFO)
    file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))

    error_handler = logging.FileHandler(ERROR_LOG_FILE)
    error_handler.setLevel(logging.WARNING)
    error_handler.addFilter(ErrorWarningFilter())
    error_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG if args.debug else logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))

    logging.getLogger().setLevel(logging.DEBUG if args.debug else logging.INFO)
    # logging.getLogger().handlers = [file_handler, error_handler, console_handler]
    logging.getLogger().handlers = [file_handler, error_handler]

def log(msg, level="info"):
    level = level.lower()
    level_map = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL
    }

    log_level_num = level_map.get(level, logging.INFO)
    configured_level = logging.DEBUG if args.debug else logging.INFO

    # Only write to console if level >= current logging level
    if log_level_num >= configured_level:
        tqdm.write(f"[{level.upper()}] {msg}")

    logger_fn = {
        'debug': logging.debug,
        'info': logging.info,
        'warning': logging.warning,
        'error': logging.error,
        'critical': logging.critical
    }.get(level, logging.info)

    logger_fn(msg)
