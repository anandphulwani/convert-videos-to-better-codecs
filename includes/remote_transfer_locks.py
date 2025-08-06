import datetime
import os
import time
from config import LOCKS_DIR, MACHINE_ID
from helpers.logging_utils import log

def try_acquire_remote_transfer_lock_loop():
    lock_path = os.path.join(LOCKS_DIR, f"{MACHINE_ID}.lock")
    while True:
        if os.path.exists(lock_path):
            mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(lock_path))
            age = datetime.datetime.now() - mod_time
            if age.total_seconds() > 900:
                log(f"Found stale lock for {MACHINE_ID}, removing...", level="warning")
                os.remove(lock_path)
        try:
            with open(lock_path, 'x'):
                log(f"Lock acquired by {MACHINE_ID}")
                return
        except FileExistsError:
            log(f"{MACHINE_ID} waiting for lock... retrying in 5 min")
            time.sleep(300)

def renew_remote_transfer_lock(stop_event):
    lock_path = os.path.join(LOCKS_DIR, f"{MACHINE_ID}.lock")
    while not stop_event.is_set():
        try:
            with open(lock_path, 'w') as f:
                f.write(f"Updated at {datetime.datetime.now()}")
            log("Lock file renewed", level="debug")
        except Exception as e:
            log(f"Failed to renew lock file: {e}", level="error")
        stop_event.wait(600)
