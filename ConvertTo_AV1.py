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
import os
import shutil
import subprocess
import threading
import logging
import argparse
from multiprocessing import Manager
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import re
import time
import datetime
import socket
import uuid
import zlib
import sys
import psutil

# CLI argument parsing
parser = argparse.ArgumentParser(description="Distributed AV1 encoding job processor")
parser.add_argument("--debug", action="store_true", help="Enable debug logging")
parser.add_argument("--throttle", action="store_true", help="Enable CPU usage throttling")
args = parser.parse_args()

# Setup logging
class ErrorWarningFilter(logging.Filter):
    def filter(self, record):
        return record.levelno >= logging.WARNING

timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
LOG_FILE = f'./av1_job_{timestamp}.log'
ERROR_LOG_FILE = f'./errors_av1_job_{timestamp}.log'

# General log file handler
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setLevel(logging.DEBUG if args.debug else logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))

# Error/Warning file handler
error_handler = logging.FileHandler(ERROR_LOG_FILE)
error_handler.setLevel(logging.WARNING)
error_handler.addFilter(ErrorWarningFilter())
error_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))

# Console output handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG if args.debug else logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))

# Add handlers to root logger
logging.getLogger().setLevel(logging.DEBUG if args.debug else logging.INFO)
logging.getLogger().handlers = [file_handler, error_handler, console_handler]

def generate_machine_id():
    # Get IP address
    try:
        ip = socket.gethostbyname(socket.gethostname())
        ip_parts = ip.split('.')
        if len(ip_parts) >= 2:
            last_two = ip_parts[-2] + ip_parts[-1]  # e.g., 123.45 → 12345
        else:
            print("Failed to parse IP address correctly.")
            sys.exit(1)
    except Exception as e:
        print(f"Failed to get IP address: {e}")
        sys.exit(1)

    # Get MAC address and compute CRC32
    try:
        mac = uuid.getnode()
        if (mac >> 40) % 2:
            raise ValueError("Invalid MAC address retrieved (locally administered bit set).")
        mac_bytes = mac.to_bytes(6, byteorder='big')
        crc32_mac = format(zlib.crc32(mac_bytes) & 0xFFFFFFFF, '08x')
    except Exception as e:
        print(f"Failed to get MAC address: {e}")
        sys.exit(1)

    return f"{last_two}-{crc32_mac}"

# Configuration
MACHINE_ID = os.getenv("MACHINE_ID", generate_machine_id())

if not MACHINE_ID.strip():
    logging.critical("MACHINE_ID is empty. Exiting.")
    sys.exit(1)

SFTP_ROOT = './SFTP_ROOT'
JOBS_DIR = os.path.join(SFTP_ROOT, 'jobs')
LOCKS_DIR = os.path.join(SFTP_ROOT, 'locks')
TO_ASSIGN = os.path.join(JOBS_DIR, 'to_assign')
IN_PROGRESS = os.path.join(JOBS_DIR, 'in_progress', MACHINE_ID)
DONE_DIR = os.path.join(JOBS_DIR, 'done')
FAILED_DIR = os.path.join(JOBS_DIR, 'failed')
LOGS_DIR = os.path.join(JOBS_DIR, 'logs', MACHINE_ID)

TMP_ROOT = './tmp_input'
TMP_PROCESSING = os.path.join(TMP_ROOT, 'processing')

TMP_OUTPUT_ROOT = './tmp_output_av1_crf{}'   # Format string for CRF values
FINAL_OUTPUT_ROOT = './ForTesting_Out/AV1_crf{}'  # Format string for CRF values

CRF_VALUES = [24, 60]
MAX_CPU_UTIL = 0.8
MAX_WORKERS = 10 if not args.throttle else max(1, int(psutil.cpu_count(logical=True) * MAX_CPU_UTIL))
CHUNK_SIZE = 1 * 1024 * 1024 * 1024

pause_flag = threading.Event()

def copy_with_progress(src, dst, desc="Copying"):
    total_size = os.path.getsize(src)
    os.makedirs(os.path.dirname(dst), exist_ok=True)

    with open(src, 'rb') as fsrc, open(dst, 'wb') as fdst:
        with tqdm(total=total_size, unit='B', unit_scale=True, desc=desc, leave=False) as pbar:
            while True:
                buf = fsrc.read(1024 * 1024)  # 1 MB
                if not buf:
                    break
                fdst.write(buf)
                pbar.update(len(buf))

    shutil.copystat(src, dst)  # Preserve metadata like modification time

def move_with_progress(src, dst, desc="Moving"):
    copy_with_progress(src, dst, desc)
    os.remove(src)

def ensure_dirs():
    dirs = [LOCKS_DIR, TO_ASSIGN, IN_PROGRESS, DONE_DIR, FAILED_DIR, LOGS_DIR, TMP_PROCESSING]
    for d in dirs:
        os.makedirs(d, exist_ok=True)
        logging.debug(f"Ensured directory exists: {d}")
    for crf in CRF_VALUES:
        tmp_out = TMP_OUTPUT_ROOT.format(crf)
        final_out = FINAL_OUTPUT_ROOT.format(crf)
        # Clear tmp output dir
        if os.path.exists(tmp_out):
            shutil.rmtree(tmp_out)
            logging.info(f"Cleared temp output directory: {tmp_out}")
        os.makedirs(tmp_out, exist_ok=True)
        os.makedirs(final_out, exist_ok=True)
        logging.debug(f"Ensured CRF directories: {tmp_out}, {final_out}")

def save_logs_to_central_output():
    try:
        shutil.copy(LOG_FILE, LOGS_DIR)
        shutil.copy(ERROR_LOG_FILE, LOGS_DIR)
        logging.info(f"Copied logs to {LOGS_DIR}")
    except Exception as e:
        logging.error(f"Failed to copy logs to {LOGS_DIR}: {e}")

def get_all_files_sorted(base_dir):
    logging.debug(f"Scanning directory: {base_dir}")
    all_files = []

    with tqdm(desc="Scanning for .mp4 files", unit=" file", leave=False) as pbar:
        for root, _, files in os.walk(base_dir):
            for file in files:
                if file.lower().endswith('.mp4'):
                    full_path = os.path.join(root, file)
                    rel_path = os.path.relpath(full_path, base_dir)
                    all_files.append((full_path, rel_path))
                    pbar.update(1)

    logging.debug(f"Found {len(all_files)} .mp4 files")
    return sorted(all_files, key=lambda x: x[1])

def try_acquire_lock_loop():
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
    lock_path = os.path.join(LOCKS_DIR, f"{MACHINE_ID}.lock")
    while not stop_event.is_set():
        try:
            with open(lock_path, 'w') as f:
                f.write(f"Updated at {datetime.datetime.now()}")
            logging.debug("Lock file renewed")
        except Exception as e:
            logging.error(f"Failed to renew lock file: {e}")
        stop_event.wait(600)

def cpu_watchdog():
    while True:
        usage = psutil.cpu_percent(interval=5)
        logging.debug(f"CPU usage: {usage}%")
        if usage >= 95:
            logging.warning("High CPU usage detected. Pausing encoding...")
            pause_flag.clear()
        elif usage <= 10:
            if not pause_flag.is_set():
                logging.info("CPU usage normalized. Resuming encoding...")
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
        move_with_progress(src, dst, desc=f"Moving {os.path.basename(src)}")
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
        return f"[CRF {crf}] Skipped {rel_path} (already exists)"

    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    cmd = ffmpeg_cmd_av1_crf(src_file, out_file, crf)
    duration = get_duration(src_file)
    if duration is None:
        logging.warning(f"Duration not found for {rel_path}, skipping file.")
        return f"[CRF {crf}] Skipped {rel_path} (duration not found)"
    file_size = os.path.getsize(src_file)
    start_time = time.time()

    logging.debug(f"Encoding {rel_path} [CRF {crf}]")
    pbar = tqdm(total=duration or 100, desc=f"CRF{crf}: {os.path.basename(src_file)}", unit='s', leave=False)
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

    if process.returncode != 0 or not os.path.exists(out_file):
        logging.error(f"FFmpeg failed for {rel_path} [CRF {crf}]")
        return f"[CRF {crf}] Failed {rel_path}"

    move_with_progress(out_file, final_dst, desc=f"Moving {os.path.basename(out_file)}")
    elapsed = time.time() - start_time
    return f"[CRF {crf}] {os.path.basename(src_file)} in {format_elapsed(elapsed)}"

def update_size_pbar(pbar, shared_val, total_bytes):
    while not pbar.disable and pbar.n < total_bytes:
        pbar.n = shared_val.value
        pbar.refresh()
        time.sleep(0.5)

def has_files(base_dir):
    return any(file.lower().endswith('.mp4') for _, _, files in os.walk(base_dir) for file in files)

def main():
    logging.info(f"Starting AV1 job processor on {MACHINE_ID}")
    ensure_dirs()

    while True:
        processing_files_exist = has_files(TMP_PROCESSING)

        if not processing_files_exist:
            try_acquire_lock_loop()
            stop_renew = threading.Event()
            renew_thread = threading.Thread(target=renew_lock, args=(stop_renew,), daemon=True)
            renew_thread.start()

            pause_flag.set()
            if args.throttle:
                logging.info("CPU throttling enabled.")
                threading.Thread(target=cpu_watchdog, daemon=True).start()
            else:
                logging.info("CPU throttling disabled. Encoding at full capacity.")
                pause_flag.set()

            chunk = claim_files()
            if not chunk:
                logging.info(f"No files claimed by {MACHINE_ID}")
                stop_renew.set()
                os.remove(os.path.join(LOCKS_DIR, f"{MACHINE_ID}.lock"))
                break

            stop_renew.set()
            renew_thread.join()
            os.remove(os.path.join(LOCKS_DIR, f"{MACHINE_ID}.lock"))

            for src, rel in chunk:
                dst = os.path.join(TMP_PROCESSING, rel)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                copy_with_progress(os.path.join(IN_PROGRESS, rel), dst, desc=f"Copying {os.path.basename(rel)}")
                logging.debug(f"Copied to processing dir: {rel}")
        else:
            logging.info("Resuming from existing TMP_PROCESSING files...")
            chunk = []  # no new files claimed; this avoids moving from IN_PROGRESS later

        task_queue = []
        total_bytes = 0
        for src, rel in get_all_files_sorted(TMP_PROCESSING):
            size = os.path.getsize(src)
            for crf in CRF_VALUES:
                task_queue.append((src, rel, crf, size))
                total_bytes += size

        manager = Manager()
        shared_bytes = manager.Value('i', 0)

        size_pbar = tqdm(total=total_bytes, unit='B', unit_scale=True, desc=f"Total Progress", position=0)
        pbar_thread = threading.Thread(target=update_size_pbar, args=(size_pbar, shared_bytes, total_bytes))
        pbar_thread.start()

        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(encode_file, src, rel, crf, shared_bytes)
                    for src, rel, crf, _ in task_queue]
            for future in as_completed(futures):
                try:
                    result = future.result()
                    tqdm.write(result)
                    logging.info(result)
                    results.append(result)
                except Exception as e:
                    logging.error(f"Encoding task failed: {e}")
                    with open("failed_encodes.log", "a") as f:
                        f.write(f"{e}\n")
                    results.append(f"Failed: {e}")

        size_pbar.n = total_bytes
        size_pbar.refresh()
        size_pbar.close()
        pbar_thread.join()

        # Determine which files succeeded entirely
        success_map = {}
        for result in results:
            match = re.search(r"\[CRF (\d+)] (.+?)(?: in| Failed)", result)
            if match:
                crf = int(match.group(1))
                file = match.group(2)
                rel_path = next((rel for _, rel, _, _ in task_queue if os.path.basename(rel) == file), None)
                if rel_path:
                    success = "Failed" not in result
                    if rel_path not in success_map:
                        success_map[rel_path] = []
                    success_map[rel_path].append(success)

        for rel in success_map:
            all_success = all(success_map[rel])
            target_dir = DONE_DIR if all_success else FAILED_DIR
            src_path = os.path.join(IN_PROGRESS, rel)
            dst_path = os.path.join(target_dir, rel)
            os.makedirs(os.path.dirname(dst_path), exist_ok=True)
            try:
                move_with_progress(src_path, dst_path, desc=f"Moving {os.path.basename(rel)}")
                logging.debug(f"Moved to {'done' if all_success else 'failed'}: {rel}")
            except Exception as e:
                logging.error(f"Failed to move file {rel} to final dir: {e}")

        save_logs_to_central_output()
        logging.info(f"{MACHINE_ID} finished processing batch; looping back to check for more jobs.")
        time.sleep(300)

if __name__ == "__main__":
    main()
