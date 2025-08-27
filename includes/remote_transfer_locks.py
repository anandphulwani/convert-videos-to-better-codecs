import datetime
import os
import time
from config import LOCKS_DIR, MACHINE_ID
from helpers.logging_utils import log
from helpers.remove_path import remove_path

def try_acquire_remote_transfer_lock_loop():
    lock_path = os.path.join(LOCKS_DIR, f"remote_fetching.lock")
    while True:
        if os.path.exists(lock_path):
            try:
                mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(lock_path))
                age = (datetime.datetime.now() - mod_time).total_seconds()

                if age > 900:
                    try:
                        remove_path(lock_path)
                        log(f"Removed stale lock file: {os.path.basename(lock_path)}", level="debug")                
                    except Exception as e:
                        log(f"Failed to remove stale lock {lock_path}: {e}", level="error")
                else:
                    log(f"Lock file {os.path.basename(lock_path)} is still fresh ({int(age)}s old)", level="debug")
                    log(f"Waiting for all locks to expire... retrying in 5 min")
                    time.sleep(300)
                    continue
            except FileNotFoundError:
                continue  # File may be deleted during iteration


        # All lock files are either stale and removed, or there were none
        try:
            # Atomically create the lock file
            with open(lock_path, 'x') as f:
                pass  # Just create it

            # Re-open in write mode to add machine info
            with open(lock_path, 'w') as f:
                f.write(f"MACHINE_ID={MACHINE_ID}\n")
                f.write(f"CreatedAt={datetime.datetime.now().isoformat()}\n")
            return
        except FileExistsError:
            log(f"{MACHINE_ID} failed to acquire lock after cleanup, retrying in 5 min")
            time.sleep(300)

def renew_remote_transfer_lock(stop_event):
    lock_path = os.path.join(LOCKS_DIR, f"remote_fetching.lock")
    while not stop_event.is_set():
        try:
            with open(lock_path, 'a') as f:
                f.write(f"Updated at {datetime.datetime.now()}\n")
            log("Lock file renewed", level="debug")
        except Exception as e:
            log(f"Failed to renew lock file: {e}", level="error")
        stop_event.wait(600)
