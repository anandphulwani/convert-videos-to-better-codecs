import time
from tqdm import tqdm
import os
import sys
import atexit
import shutil
import requests

from config import TO_ASSIGN, CRF_VALUES, MACHINE_ID, MAX_WORKERS
from helpers.logging_utils import setup_logging, log
from clazz.JobManager import JobManager
from includes.ffmpeg import ffmpeg_get_duration
from includes.cleanup_working_folders import cleanup_working_folders
from includes.move_logs_to_central_output import move_logs_to_central_output
from helpers.format_elapsed import format_elapsed

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



def call_http_url(message, timeout=10):
    try:        
        requests.post("https://ntfy.sh/anand_alerts", data=message.encode(encoding='utf-8'), timeout=timeout)
    except requests.exceptions.RequestException as e:
        print(f"Error calling URL: {e}")
        return None


# === Main ===
def main():
    setup_logging()
    cleanup_working_folders()
    log(f"Starting AV1 job processor on {MACHINE_ID}")

    chunk_bars = {}
    chunk_start = {}
    completed_once = set()
    # Worker lanes: 0..MAX_WORKERS-1
    # I/O lane: MAX_WORKERS
    # Chunk bars start below that:
    next_position = MAX_WORKERS + 1

    job_manager = JobManager()
    job_manager.start()

    try:
        while True:
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
                        # leave=True,
                        leave=False,
                        dynamic_ncols=True
                    )
                    chunk_bars[chunk_name].refresh()
                    chunk_start[chunk_name] = time.time()   # <-- track start time
                    next_position += 1

            # --- when updating bars (still inside the loop) ---
            for chunk_name, bar in list(chunk_bars.items()):
                try:
                    done = job_manager.chunk_progress[chunk_name].value
                except KeyError:
                    continue

                # ETA = (elapsed / processed) * total - elapsed
                elapsed = time.time() - chunk_start[chunk_name]
                if bar.total and done > 0 and chunk_name not in completed_once:
                    est_total_time = elapsed * (bar.total / float(done))
                    remaining = max(0.0, est_total_time - elapsed)
                    bar.set_postfix_str(f"ETA {format_elapsed(remaining)}")

                # when finished, just leave a compact summary line (no extra bars)
                if bar.total and done >= bar.total and chunk_name not in completed_once:
                    try:
                        bar.set_postfix(None, refresh=False)
                    except TypeError:
                        bar.set_postfix({})
                    bar.set_postfix_str("ETA --:--")
                    bar.bar_format = None
                    bar.refresh()
                    
                    completed_once.add(chunk_name)
                    call_http_url(f"Adding {chunk_name} to complete_once, done: {done}, bar.total: {bar.total}.")

                log(f"chunk_name: {chunk_name}, updating it with: {done - bar.n}")
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
