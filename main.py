import platform
import time
from tqdm import tqdm
import os
import sys
import atexit
import shutil
import requests

from config import TO_ASSIGN, CRF_VALUES, MACHINE_ID, MAX_WORKERS
from helpers.logging_utils import setup_logging, stop_logging, log
from helpers.call_http_url import call_http_url
from clazz.JobManager import JobManager
from includes.cleanup_working_folders import cleanup_working_folders
from includes.move_logs_to_central_output import move_logs_to_central_output
from tqdm_manager import get_tqdm_manager, BAR_TYPE_CHUNK, create_event_queue

def _restore():
    try:
        if sys.stdout and sys.stdout.isatty():
            sys.stdout.write("\033[?25h")
            sys.stdout.flush()
        if os.name == "posix" and sys.stdin and sys.stdin.isatty():
            stty = shutil.which("stty")
            if stty:
                os.system("stty echo 2>/dev/null")
    except Exception:
        pass

atexit.register(_restore)

# === Main ===
def main():
    setup_logging()
    cleanup_working_folders()
    log(f"Starting AV1 job processor on {MACHINE_ID}")

    completed_once = set()
    event_queue = create_event_queue()
    # if event_queue is not None:
    
    tqdm_manager = get_tqdm_manager()
    tqdm_manager.attach_event_queue(event_queue)
    
    job_manager = JobManager(progress_queue=event_queue)
    job_manager.start()
    # tqdm_manager.refresh_bars()
    
    try:
        while True:
            # Create bars for newly discovered chunks
            for chunk_name, total_bytes in list(job_manager.chunk_totals.items()):
                event_queue.put({
                    "op": "create",
                    "bar_type": "chunk",
                    "bar_id": chunk_name,
                    "total": total_bytes,
                    "metadata": {},
                    "unit": "B",
                    "unit_scale": True,
                    "unit_divisor": 1024
                })

            # Update progress
            for chunk_name in list(job_manager.chunk_progress.keys()):
                try:
                    chunk_progress_value = job_manager.chunk_progress[chunk_name].value
                except KeyError:
                    continue
                event_queue.put({
                    "op": "update",
                    "bar_id":chunk_name,
                    "current": chunk_progress_value
                })

                if chunk_progress_value >= job_manager.chunk_totals.get(chunk_name, 0) and chunk_name not in completed_once:
                    completed_once.add(chunk_name)
                    event_queue.put({"op": "finish", "bar_id": chunk_name})

            if job_manager.is_done():
                log("All tasks processed. Exiting main loop.")
                break

            time.sleep(1)

    except KeyboardInterrupt:
        log("Interrupted.", level="warning")
    finally:
        job_manager.shutdown()
        tqdm_manager.stop_event_loop()
        cleanup_working_folders()
        move_logs_to_central_output()
        stop_logging()
        print("Exiting main program.")

if __name__ == '__main__':
    import multiprocessing as mp
    tqdm.set_lock(mp.RLock())
    main()
