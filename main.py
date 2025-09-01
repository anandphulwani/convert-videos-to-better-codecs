"""
AV1 Video Encoding Job Processor
================================

This script operates as a distributed worker node for AV1 video encoding, processing `.mp4` files
using FFmpeg with the `libaom-av1` codec at various CRF (Constant Rate Factor) values. It is designed
for use in a multi-machine setup where each machine autonomously claims and processes jobs from
a shared directory structure.

Key Features:
-------------
- Claims `.mp4` jobs from a shared queue (`./SFTP_ROOT/jobs/to_assign`)
- Encodes videos using AV1 codec at specified CRF levels (default: 24, 60)
- Monitors and throttles CPU usage to avoid system overload
- Supports parallel encoding using multiple processes, respecting a CPU utilization threshold
- Displays real-time encoding progress via tqdm progress bars
- Implements a file-based locking system to prevent job conflicts between machines
- Automatically moves jobs through processing states: `to_assign` → `in_progress` → `done`

Directory Structure:
--------------------
- ./SFTP_ROOT/
  ├── jobs/
  │   ├── to_assign/        # Input videos to be processed
  │   ├── in_progress/      # Temporary holding for claimed jobs
  │   └── done/             # Successfully processed videos
  └── locks/                # Lock files for each machine (e.g., machineA.lock)

- ./tmp_input/processing/    # Local temporary working directory for processing
- ./tmp_output_av1_crfXX/    # Intermediate output for each CRF level
- ./ForTesting_Out/AV1_crfXX/ # Final output directory for encoded files by CRF

Environment Variables:
----------------------
- `MACHINE_ID` (optional): Unique identifier for the current machine.
  If not set, it is auto-generated using a combination of IP and MAC address.

Configuration Constants:
------------------------
- `CRF_VALUES`: List of CRF levels used for encoding.
- `MAX_CPU_UTIL`: Maximum CPU utilization fraction (e.g., 0.8 for 80%).
- `CHUNK_SIZE`: Maximum total size (in bytes) of job batch to claim at once.

Dependencies:
-------------
- Python 3.8+
- FFmpeg with `libaom-av1` and `libopus` support
- Python packages:
  - `tqdm`
  - `psutil`

Usage:
------
This script is intended to run periodically or persistently on distributed worker nodes.
Each instance automatically claims jobs, encodes them, and manages its resource usage.

To run:
    python encode_jobs_av1.py [--debug] [--throttle]

Options:
--------
--debug      Enable verbose debug logging
--throttle   Enable CPU usage monitoring and adaptive throttling

"""
import time
from tqdm import tqdm
import os
import sys
import atexit
import shutil

from config import TO_ASSIGN, CRF_VALUES, MACHINE_ID, MAX_WORKERS
from helpers.logging_utils import setup_logging, stop_logging, log
from clazz.JobManager import JobManager
from includes.cleanup_working_folders import cleanup_working_folders
from includes.move_logs_to_central_output import move_logs_to_central_output
from includes.state import pause_flag
from tqdm_manager import get_tqdm_manager, get_event_queue, BAR_TYPE

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
    created_bars = set()
    last_progress = {}
    pause_flag.clear()
    
    tqdm_manager = get_tqdm_manager()
    event_queue = get_event_queue()
    
    job_manager = JobManager()
    job_manager.start()
    
    try:
        while True:
            # Create bars for newly discovered chunks
            for chunk_name, total_bytes in list(job_manager.chunk_totals.items()):
                if chunk_name not in created_bars:
                    event_queue.put({
                        "op": "create",
                        "bar_type": BAR_TYPE.CHUNK,
                        "bar_id": chunk_name,
                        "total": total_bytes,
                        "metadata": {}
                    })
                    created_bars.add(chunk_name)

            # Update progress
            for chunk_name in list(job_manager.chunk_progress.keys()):
                try:
                    chunk_progress_value = job_manager.chunk_progress[chunk_name].value
                except KeyError:
                    continue
                if last_progress.get(chunk_name) != chunk_progress_value:
                    event_queue.put({
                        "op": "update",
                        "bar_id": chunk_name,
                        "current": chunk_progress_value
                    })
                    last_progress[chunk_name] = chunk_progress_value  # cache it

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
