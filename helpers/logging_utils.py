import logging
import datetime
import platform
import threading
import multiprocessing as mp
from tqdm import tqdm
from queue import Empty
from dataclasses import dataclass

from config import args

# Global log file names
manager = mp.Manager()
log_files_shared_state = manager.Namespace()
log_files_shared_state.LOG_FILE = None
log_files_shared_state.ERROR_LOG_FILE = None

# Global logging queue
if platform.system() == "Windows":
    ctx = mp.get_context("spawn")
else:
    # Use 'fork' where available (typically Unix-based systems), else fallback to 'spawn'
    try:
        ctx = mp.get_context("fork")
    except ValueError:
        ctx = mp.get_context("spawn")

LOG_QUEUE = ctx.Queue()

# --- Log Message Structure ---
@dataclass
class LogMessage:
    level: str
    message: str

# --- Logging Filter ---
class ErrorWarningFilter(logging.Filter):
    def filter(self, record):
        return record.levelno >= logging.WARNING

# --- Setup Logging ---
def setup_logging(event_queue):

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_files_shared_state.LOG_FILE = f'./av1_job_{timestamp}.log'
    log_files_shared_state.ERROR_LOG_FILE = f'./errors_av1_job_{timestamp}.log'

    # Clear any existing handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # File handler (debug/info)
    file_handler = logging.FileHandler(log_files_shared_state.LOG_FILE)
    file_handler.setLevel(logging.DEBUG if args.debug else logging.INFO)
    file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))

    # Error log handler (warnings and above)
    error_handler = logging.FileHandler(log_files_shared_state.ERROR_LOG_FILE)
    error_handler.setLevel(logging.WARNING)
    error_handler.addFilter(ErrorWarningFilter())
    error_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))

    # Register handlers
    logging.getLogger().setLevel(logging.DEBUG if args.debug else logging.INFO)
    logging.getLogger().handlers = [file_handler, error_handler]

    # Start log consumer thread
    threading.Thread(target=_log_consumer, args=(event_queue,), daemon=True).start()

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
def _log_consumer(event_queue):
    while True:
        try:
            record = LOG_QUEUE.get(timeout=0.05)
            if record is None:
                break  # graceful shutdown
            _emit_log(record, event_queue)
        except Empty:
            continue
        except Exception:
            pass  # never crash consumer

def _emit_log(record: LogMessage, event_queue):
    """
    Emit log from the main process logger.
    """
    level_map = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL
    }

    log_level_num = level_map.get(record.level, logging.INFO)
    configured_level = logging.DEBUG if args.debug else logging.INFO

    # Console output via tqdm if appropriate
    if log_level_num >= configured_level:
        clear_code = "\033[J"
        tqdm.write(f"[{record.level.upper()}] {record.message}{clear_code}")
        event_queue.put({"op": "change_state_of_bars", "state": True})
        event_queue.put({"op": "change_state_of_bars", "state": False})

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
    try:
        LOG_QUEUE.put_nowait(None)
    except Exception:
        pass
