import os
import time
import datetime
import logging

from config import LOCKS_DIR

def try_acquire_lock_loop():
    lock_path = os.path.join(LOCKS_DIR, f"{os.getenv('MACHINE_ID', '') or ''}.lock")
    # If env var MACHINE_ID not set, IN_PROGRESS path already contains it via config; however, the original
    # script uses the same lock filename derived from config.MACHINE_ID. We'll derive from config directly.
    from config import MACHINE_ID
    lock_path = os.path.join(LOCKS_DIR, f"{MACHINE_ID}.lock")

    while True:
        if os.path.exists(lock_path):
            mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(lock_path))
            age = datetime.datetime.now() - mod_time
            if age.total_seconds() > 900:
                logging.warning(f"Found stale lock for {MACHINE_ID}, removing...")
                os.remove(lock_path)
        try:
            with open(lock_path, 'x'):
                logging.info(f"Lock acquired by {MACHINE_ID}")
                return
        except FileExistsError:
            logging.info(f"{MACHINE_ID} waiting for lock... retrying in 5 min")
            time.sleep(300)

def renew_lock(stop_event):
    from config import MACHINE_ID
    lock_path = os.path.join(LOCKS_DIR, f"{MACHINE_ID}.lock")
    while not stop_event.is_set():
        try:
            with open(lock_path, 'w') as f:
                f.write(f"Updated at {datetime.datetime.now()}")
            logging.debug("Lock file renewed")
        except Exception as e:
            logging.error(f"Failed to renew lock file: {e}")
        stop_event.wait(600)
