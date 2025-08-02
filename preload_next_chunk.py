import time
import logging
import os
import psutil

from config import (
    MAX_WORKERS, IN_PROGRESS, TMP_PROCESSING
)
from file_ops import claim_files
from helpers.copy_and_move_with_progress import copy_with_progress
from state import task_queue, next_chunk_ready, preload_lock

# Add this function to preload chunk in the background
def preload_next_chunk(stop_event):
    logging.info("Preloader thread started.")
    while not stop_event.is_set():
        try:
            cpu = psutil.cpu_percent(interval=2)
            pending_tasks = task_queue.qsize()

            # CPU usage check or fewer tasks running
            if cpu < 60 or pending_tasks < max(2, MAX_WORKERS // 2):
                with preload_lock:
                    if not next_chunk_ready.is_set():
                        chunk = claim_files()
                        if chunk:
                            for src, rel in chunk:
                                dst = os.path.join(TMP_PROCESSING, rel)
                                os.makedirs(os.path.dirname(dst), exist_ok=True)
                                copy_with_progress(os.path.join(IN_PROGRESS, rel), dst,
                                                   desc=f"Preloading {os.path.basename(rel)}")
                                logging.debug(f"Preloaded to processing dir: {rel}")
                            task_queue.put(chunk)
                            next_chunk_ready.set()
        except Exception as e:
            logging.error(f"Error in preload thread: {e}")

        time.sleep(10)
