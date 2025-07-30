"""
AV1 Video Encoding Job Processor
================================

This script is designed to operate as a distributed AV1 video encoding worker, processing `.mp4` files
using FFmpeg and the `libaom-av1` codec with multiple CRF (Constant Rate Factor) values. It is
intended to be used in a shared environment with multiple machines working in parallel.

Key Features:
-------------
- Claim and process `.mp4` jobs from a shared job queue (`./SFTP_ROOT/jobs/to_assign`)
- Encode videos using AV1 codec at defined CRF levels (default: 24, 60)
- Tracks CPU usage and dynamically pauses/resumes encoding based on CPU load
- Uses multiprocessing for parallel encoding, up to a configurable CPU utilization threshold
- Provides real-time progress monitoring with tqdm progress bars
- Implements a file-based locking mechanism to coordinate work between machines
- Automatically handles job state transitions: `to_assign` → `in_progress` → `done`

Directory Structure:
--------------------
- SFTP_ROOT/
  └── jobs/
      ├── to_assign/       # Input videos to be processed
      ├── in_progress/     # Videos currently being encoded
      ├── done/            # Completed jobs
  └── locks/               # Machine-specific lock files

- tmp_input/processing/    # Local temporary copies of videos for processing
- tmp_output_av1_crfXX/    # Temporary output directories for each CRF level
- ForTesting_Out/AV1_crfXX/ # Final destination for encoded outputs

Environment Variables:
----------------------
- `MACHINE_ID` (optional): Unique identifier for this machine. Defaults to "machineX".

Configuration Constants:
------------------------
- `CRF_VALUES`: List of CRF levels to encode with.
- `MAX_CPU_UTIL`: Max % of logical CPU cores to utilize (default 80%).
- `CHUNK_SIZE`: Max total size (in bytes) of files to claim in one job batch.

Dependencies:
-------------
- Python 3.8+
- FFmpeg (with `libaom-av1` and `libopus`)
- Python packages:
  - `tqdm`
  - `psutil`

Usage:
------
This script is intended to be run periodically or as a persistent process on distributed worker nodes.
It automatically claims and processes available video files, updating the job state and managing system
resources.

To run:
    python encode_jobs_av1.py

"""
import os
import shutil
import subprocess
import threading
import logging
import argparse
from multiprocessing import Manager
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import re
import time
import datetime
import psutil

# CLI argument parsing
parser = argparse.ArgumentParser(description="Distributed AV1 encoding job processor")
parser.add_argument("--debug", action="store_true", help="Enable debug logging")
parser.add_argument("--throttle", action="store_true", help="Enable CPU usage throttling")
args = parser.parse_args()

# Setup logging
LOG_FILE = '/root/av1_job_processor.log'
logging.basicConfig(
    level=logging.DEBUG if args.debug else logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# Configuration
MACHINE_ID = os.getenv("MACHINE_ID", "machineX")
SFTP_ROOT = './SFTP_ROOT'
JOBS_DIR = os.path.join(SFTP_ROOT, 'jobs')
LOCKS_DIR = os.path.join(SFTP_ROOT, 'locks')
TO_ASSIGN = os.path.join(JOBS_DIR, 'to_assign')
IN_PROGRESS = os.path.join(JOBS_DIR, 'in_progress', MACHINE_ID)
DONE_DIR = os.path.join(JOBS_DIR, 'done')

TMP_ROOT = './tmp_input'
TMP_PROCESSING = os.path.join(TMP_ROOT, 'processing')

TMP_OUTPUT_ROOT = './tmp_output_av1_crf{}'   # Format string for CRF values
FINAL_OUTPUT_ROOT = './ForTesting_Out/AV1_crf{}'  # Format string for CRF values

CRF_VALUES = [24, 60]
MAX_CPU_UTIL = 0.8
MAX_WORKERS = max(1, int(psutil.cpu_count(logical=True) * MAX_CPU_UTIL))
CHUNK_SIZE = 1 * 1024 * 1024 * 1024

pause_flag = threading.Event()

def ensure_dirs():
    dirs = [LOCKS_DIR, TO_ASSIGN, IN_PROGRESS, DONE_DIR, TMP_PROCESSING]
    for d in dirs:
        os.makedirs(d, exist_ok=True)
        logging.debug(f"Ensured directory exists: {d}")
    for crf in CRF_VALUES:
        tmp_out = TMP_OUTPUT_ROOT.format(crf)
        final_out = FINAL_OUTPUT_ROOT.format(crf)
        os.makedirs(tmp_out, exist_ok=True)
        os.makedirs(final_out, exist_ok=True)
        logging.debug(f"Ensured CRF directories: {tmp_out}, {final_out}")

def get_all_files_sorted(base_dir):
    logging.debug(f"Scanning directory: {base_dir}")
    all_files = []
    for root, _, files in os.walk(base_dir):
        for file in files:
            if file.lower().endswith('.mp4'):
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, base_dir)
                all_files.append((full_path, rel_path))
    logging.debug(f"Found {len(all_files)} .mp4 files")
    return sorted(all_files, key=lambda x: x[1])

def try_acquire_lock_loop():
    lock_path = os.path.join(LOCKS_DIR, f"{MACHINE_ID}.lock")
    while True:
        if os.path.exists(lock_path):
            mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(lock_path))
            age = datetime.datetime.now() - mod_time
            if age.total_seconds() > 900:
                logging.warning(f"⚠️ Found stale lock for {MACHINE_ID}, removing...")
                os.remove(lock_path)
        try:
            with open(lock_path, 'x'):
                logging.info(f"🔓 Lock acquired by {MACHINE_ID}")
                return
        except FileExistsError:
            logging.info(f"⏳ {MACHINE_ID} waiting for lock... retrying in 5 min")
            time.sleep(300)

def renew_lock(stop_event):
    lock_path = os.path.join(LOCKS_DIR, f"{MACHINE_ID}.lock")
    while not stop_event.is_set():
        with open(lock_path, 'w') as f:
            f.write(f"Updated at {datetime.datetime.now()}")
        logging.debug("🔄 Lock file renewed")
        stop_event.wait(600)

def cpu_watchdog():
    while True:
        usage = psutil.cpu_percent(interval=5)
        logging.debug(f"🧠 CPU usage: {usage}%")
        if usage >= 95:
            logging.warning("⚠️ High CPU usage detected. Pausing encoding...")
            pause_flag.clear()
        elif usage <= 10:
            if not pause_flag.is_set():
                logging.info("✅ CPU usage normalized. Resuming encoding...")
            pause_flag.set()

def claim_files():
    all_files = get_all_files_sorted(TO_ASSIGN)
    chunk, size = [], 0
    for full_path, rel_path in all_files:
        file_size = os.path.getsize(full_path)
        logging.debug(f"Evaluating file: {rel_path} ({file_size} bytes)")
        if size + file_size > CHUNK_SIZE and chunk:
            break
        size += file_size
        chunk.append((full_path, rel_path))

    for src, rel in chunk:
        dst = os.path.join(IN_PROGRESS, rel)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.move(src, dst)
        logging.debug(f"Moved file to in_progress: {rel}")

    return chunk

def get_duration(file_path):
    result = subprocess.run([
        'ffprobe', '-v', 'error',
        '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        file_path
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        return int(float(result.stdout.strip()))
    except Exception as e:
        logging.debug(f"ffprobe failed for {file_path}: {e}")
        return None

def format_elapsed(seconds):
    return time.strftime("%H:%M:%S", time.gmtime(seconds))

def ffmpeg_cmd_av1_crf(src, out, crf):
    cmd = [
        'ffmpeg', '-i', src,
        '-c:v', 'libaom-av1',
        '-crf', str(crf),
        '-b:v', '0',
        '-cpu-used', '2',
        '-row-mt', '1',
        '-threads', '0',
        '-c:a', 'libopus',
        '-b:a', '96k',
        out
    ]
    logging.debug(f"FFmpeg command: {' '.join(cmd)}")
    return cmd

def encode_file(src_file, rel_path, crf, bytes_encoded):
    output_dir = TMP_OUTPUT_ROOT.format(crf)
    target_dir = FINAL_OUTPUT_ROOT.format(crf)
    out_file = os.path.join(output_dir, rel_path)
    final_dst = os.path.join(target_dir, rel_path)

    if os.path.exists(final_dst):
        return f"✅ [CRF {crf}] Skipped {rel_path} (already exists)"

    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    cmd = ffmpeg_cmd_av1_crf(src_file, out_file, crf)
    duration = get_duration(src_file)
    file_size = os.path.getsize(src_file)
    start_time = time.time()

    logging.debug(f"Encoding {rel_path} [CRF {crf}]")
    pbar = tqdm(total=duration or 100, desc=f"⏳ CRF{crf}: {os.path.basename(src_file)}", unit='s', leave=False)
    process = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.DEVNULL, text=True)
    time_pattern = re.compile(r'time=(\d+):(\d+):(\d+).(\d+)')

    last_progress = 0
    while process.poll() is None:
        line = process.stderr.readline()
        if not line:
            continue

        while not pause_flag.is_set():
            time.sleep(1)

        match = time_pattern.search(line)
        if match:
            h, m, s, ms = map(int, match.groups())
            seconds = h * 3600 + m * 60 + s
            pbar.n = seconds
            pbar.refresh()
            if duration:
                percent = seconds / duration
                current_progress = int(file_size * percent)
                delta = current_progress - last_progress
                last_progress = current_progress
                bytes_encoded.value += max(0, delta)

    process.wait()
    pbar.n = duration or pbar.n
    pbar.close()

    shutil.move(out_file, final_dst)
    elapsed = time.time() - start_time
    return f"✅ [CRF {crf}] {os.path.basename(src_file)} in {format_elapsed(elapsed)}"

def update_size_pbar(pbar, shared_val, total_bytes):
    while not pbar.disable and pbar.n < total_bytes:
        pbar.n = shared_val.value
        pbar.refresh()
        time.sleep(0.5)

def main():
    logging.info(f"🚀 Starting AV1 job processor on {MACHINE_ID}")
    ensure_dirs()
    try_acquire_lock_loop()

    stop_renew = threading.Event()
    renew_thread = threading.Thread(target=renew_lock, args=(stop_renew,), daemon=True)
    renew_thread.start()

    pause_flag.set()
    if args.throttle:
        logging.info("🛡️  CPU throttling enabled.")
        threading.Thread(target=cpu_watchdog, daemon=True).start()
    else:
        logging.info("🚀 CPU throttling disabled. Encoding at full capacity.")
        pause_flag.set()

    chunk = claim_files()
    if not chunk:
        logging.info(f"🚫 No files claimed by {MACHINE_ID}")
        stop_renew.set()
        os.remove(os.path.join(LOCKS_DIR, f"{MACHINE_ID}.lock"))
        return

    stop_renew.set()
    renew_thread.join()
    os.remove(os.path.join(LOCKS_DIR, f"{MACHINE_ID}.lock"))

    for src, rel in chunk:
        dst = os.path.join(TMP_PROCESSING, rel)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copy2(os.path.join(IN_PROGRESS, rel), dst)
        logging.debug(f"Copied to processing dir: {rel}")

    task_queue = []
    total_bytes = 0
    for src, rel in get_all_files_sorted(TMP_PROCESSING):
        size = os.path.getsize(src)
        for crf in CRF_VALUES:
            task_queue.append((src, rel, crf, size))
            total_bytes += size

    manager = Manager()
    shared_bytes = manager.Value('i', 0)

    size_pbar = tqdm(total=total_bytes, unit='B', unit_scale=True, desc=f"📦 Total Progress", position=0)
    pbar_thread = threading.Thread(target=update_size_pbar, args=(size_pbar, shared_bytes, total_bytes))
    pbar_thread.start()

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(encode_file, src, rel, crf, shared_bytes)
                   for src, rel, crf, _ in task_queue]
        for future in as_completed(futures):
            result = future.result()
            tqdm.write(result)
            logging.info(result)

    size_pbar.n = total_bytes
    size_pbar.refresh()
    size_pbar.close()
    pbar_thread.join()

    for _, rel in chunk:
        done_dst = os.path.join(DONE_DIR, rel)
        os.makedirs(os.path.dirname(done_dst), exist_ok=True)
        shutil.move(os.path.join(IN_PROGRESS, rel), done_dst)
        logging.debug(f"Moved to done: {rel}")

    logging.info(f"✅ {MACHINE_ID} finished processing.")

if __name__ == "__main__":
    main()
