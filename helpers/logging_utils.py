import logging
import datetime
import multiprocessing as mp
from queue import Empty
from dataclasses import dataclass

from config import args

# Global log file names
manager = mp.Manager()
log_files_shared_state = manager.Namespace()
log_files_shared_state.LOG_FILE = None
log_files_shared_state.ERROR_LOG_FILE = None

# Lock for safe handler changes (not strictly required in process model,
# but still useful if you want to support dynamic redirection safely)
LOG_HANDLER_LOCK = mp.Lock()

LOG_QUEUE = None
log_process = None

# --- Log Message Structure ---
@dataclass
class LogMessage:
    level: str
    message: str

# --- Logging Filter ---
class ErrorWarningFilter(logging.Filter):
    def filter(self, record):
        return record.levelno >= logging.WARNING

# --- Internal Utility Functions ---
def _generate_log_filenames():
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = f'./av1_job_{timestamp}.log'
    error_log_file = f'./errors_av1_job_{timestamp}.log'
    return log_file, error_log_file

def _create_logger_handlers(log_file, error_log_file):
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG if args.debug else logging.INFO)
    file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    error_handler = logging.FileHandler(error_log_file)
    error_handler.setLevel(logging.WARNING)
    error_handler.addFilter(ErrorWarningFilter())
    error_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))

    return [file_handler, error_handler]

def _reset_logger_handlers(handlers):
    logger = logging.getLogger()
    for handler in logger.handlers[:]:
        try:
            handler.flush()
        except Exception:
            pass
        handler.close()
        logger.removeHandler(handler)

    for handler in handlers:
        logger.addHandler(handler)

# --- Setup Logging ---
def setup_logging(event_queue, tqdm_manager):
    global LOG_QUEUE, log_process, LOG_HANDLER_LOCK
    LOG_QUEUE = mp.Queue()

    log_process = mp.Process(
        target=_log_consumer_process,
        args=(event_queue, tqdm_manager, LOG_QUEUE, LOG_HANDLER_LOCK, args.debug),
        daemon=True
    )
    log_process.start()

def redirect_logs_to_new_file():
    global LOG_HANDLER_LOCK
    with LOG_HANDLER_LOCK:
        new_log_file, new_error_log_file = _generate_log_filenames()
        handlers = _create_logger_handlers(new_log_file, new_error_log_file)

        _reset_logger_handlers(handlers)

        log_files_shared_state.LOG_FILE = new_log_file
        log_files_shared_state.ERROR_LOG_FILE = new_error_log_file


# --- Logging Function ---
def log(msg, level="info"):
    """
    Thread-safe and process-safe log entry via queue.
    """
    try:
        LOG_QUEUE.put_nowait(LogMessage(level=level.lower(), message=msg))
    except Exception:
        pass  # avoid crashing in case of failure

# --- Log Consumer (Main Process) ---
def _log_consumer_process(event_queue, tqdm_manager, log_queue, lock, debug):
    log_file, error_log_file = _generate_log_filenames()
    log_files_shared_state.LOG_FILE = log_file
    log_files_shared_state.ERROR_LOG_FILE = error_log_file

    handlers = _create_logger_handlers(log_file, error_log_file)

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    _reset_logger_handlers(handlers)

    while True:
        try:
            record = log_queue.get(timeout=0.01)
            if record is None:
                break  # graceful shutdown
            _emit_log(record, event_queue, tqdm_manager, lock, debug)
        except Empty:
            continue
        except Exception:
            pass  # never crash consumer
    # Cleanup
    for h in logger.handlers[:]:
        try:
            h.flush()
        except Exception:
            pass
        h.close()
        logger.removeHandler(h)

def _emit_log(record: LogMessage, event_queue, tqdm_manager, lock, debug):
    """
    Emit log from the main process logger.
    """
    with lock:
        level_map = {
            'debug': logging.DEBUG,
            'info': logging.INFO,
            'warning': logging.WARNING,
            'error': logging.ERROR,
            'critical': logging.CRITICAL
        }

        log_level_num = level_map.get(record.level, logging.INFO)
        configured_level = logging.DEBUG if debug else logging.INFO

        # Console output via tqdm if appropriate
        if log_level_num >= configured_level:
            clear_code = "\033[J"
            event_queue.put({
                        "op": "print_statement",
                        "message": f"[{record.level.upper()}] {record.message}{clear_code}"})

        if logging.getLogger().handlers:
            # Emit via logger
            logger_fn = {
                'debug': logging.debug,
                'info': logging.info,
                'warning': logging.warning,
                'error': logging.error,
                'critical': logging.critical
            }.get(record.level, logging.info)

            logger_fn(record.message)

# --- Shutdown Cleanly ---
def stop_logging():
    """
    Call from the main process before exit to stop log consumer cleanly.
    """
    global log_process
    try:
        LOG_QUEUE.put_nowait(None)
        log_process.join(timeout=5)  # optionally add timeout
        LOG_QUEUE.close()
        LOG_QUEUE.join_thread()
    except Exception:
        pass
