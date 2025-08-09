import time
from tqdm import tqdm
import os
import sys
import atexit
import shutil

from config import TO_ASSIGN, CRF_VALUES, MACHINE_ID
from helpers.logging_utils import setup_logging, log
from clazz.JobManager import JobManager
from includes.ffmpeg import ffmpeg_get_duration
from includes.cleanup_working_folders import cleanup_working_folders
from includes.move_logs_to_central_output import move_logs_to_central_output

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

    job_manager = JobManager()
    job_manager.start()

    total_seconds = 0
    for root, _, files in os.walk(TO_ASSIGN):
        for file in files:
            if file.lower().endswith(".mp4"):
                full_path = os.path.join(root, file)
                duration = ffmpeg_get_duration(full_path)
                if duration:
                    total_seconds += duration * len(CRF_VALUES)

    size_pbar = tqdm(total=total_seconds, unit='s', desc="Total Progress", smoothing=1, unit_scale=False, unit_divisor=1, position=0)

    try:
        start_time = time.time()
        while True:
            current_sec = job_manager.video_seconds_encoded.value
            size_pbar.update(current_sec - size_pbar.n)
            if time.time() - start_time >= 120:
                break
            time.sleep(5)

        job_manager.shutdown()
        log('Pausing now.')
        time.sleep(40)
        log('Resuming now.')
        job_manager = JobManager()
        job_manager.start()

        while True:
            current_sec = job_manager.video_seconds_encoded.value
            size_pbar.update(current_sec - size_pbar.n)
            if job_manager.is_done():
                log("All tasks processed. Exiting main loop.")
                break
            time.sleep(5)

    except KeyboardInterrupt:
        log("Interrupted.", level="warning")
    finally:
        try:
            size_pbar.close()
            print()
        except Exception:
            pass
        try:
            tqdm.write("Clean up complete.")
        except Exception:
            pass
        job_manager.shutdown()
        cleanup_working_folders()
        move_logs_to_central_output()
        print("Exiting main program.")

if __name__ == '__main__':
    import multiprocessing as mp
    tqdm.set_lock(mp.RLock())
    main()
