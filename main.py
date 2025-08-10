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

    chunk_bars = {}
    next_position = 1

    job_manager = JobManager()
    job_manager.start()

    try:
        while True:
            # current_sec = job_manager.video_seconds_encoded.value
            # size_pbar.update(current_sec - size_pbar.n)
            # Create bars for newly discovered chunks
            for chunk_name, total_bytes in list(job_manager.chunk_totals.items()):
                if chunk_name not in chunk_bars:
                    chunk_bars[chunk_name] = tqdm(
                        total=total_bytes,
                        unit='B',
                        unit_scale=True,
                        desc=f"{chunk_name[:5].capitalize()} {chunk_name[5:]}",
                        smoothing=0.3, # 1
                        position=next_position,
                        leave=False
                    )
                    next_position += 1

            # Update all visible chunk bars
            for chunk_name, bar in list(chunk_bars.items()):
                try:
                    done = job_manager.chunk_progress[chunk_name].value
                except KeyError:
                    continue
                bar.update(done - bar.n)

            if job_manager.is_done():
                log("All tasks processed. Exiting main loop.")
                break
            time.sleep(5)

    except KeyboardInterrupt:
        log("Interrupted.", level="warning")
    finally:
        # Close all bars cleanly
        for bar in chunk_bars.values():
            try:
                bar.close()
            except Exception:
                pass
        tqdm.write("\n\n\n")
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
