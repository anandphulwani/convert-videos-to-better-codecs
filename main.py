import threading
import time
import logging
from multiprocessing import Manager
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import os

from config import (
    args, CRF_VALUES, MAX_WORKERS, IN_PROGRESS, 
    LOCKS_DIR, DONE_DIR, FAILED_DIR, TMP_PROCESSING,
    TMP_OUTPUT_ROOT
)
from logging_utils import setup_logging, move_logs_to_central_output
from preload_next_chunk import preload_next_chunk
from file_ops import ensure_dirs, cleanup_working_folders
from locks_ops import try_acquire_lock_loop, renew_lock
from cpu_watchdog import cpu_watchdog
from encode_file import encode_file
from helpers.update_size_pbar import update_size_pbar
from helpers.copy_and_move_with_progress import move_with_progress
from helpers.has_files import has_files
from state import task_queue, next_chunk_ready, preload_lock, pause_flag

def main():
    setup_logging()  # Setup initial log files
    from config import MACHINE_ID  # import here to log value once logging is ready
    logging.info(f"Starting AV1 job processor on {MACHINE_ID}")
    ensure_dirs()

    preload_stop_event = threading.Event()
    preload_thread = threading.Thread(target=preload_next_chunk, args=(preload_stop_event,), daemon=True)
    preload_thread.start()

    first_iteration = True

    while True:
        (first_iteration := False) if first_iteration else setup_logging()

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

            # Wait for preload thread to get a chunk ready
            logging.info("Waiting for next preloaded chunk...")
            next_chunk_ready.wait()
            with preload_lock:
                chunk = task_queue.get()
                next_chunk_ready.clear()

            if not chunk:
                logging.info(f"No files claimed by {MACHINE_ID}")
                stop_renew.set()
                os.remove(os.path.join(LOCKS_DIR, f"{MACHINE_ID}.lock"))
                move_logs_to_central_output()
                break

            stop_renew.set()
            renew_thread.join()
            os.remove(os.path.join(LOCKS_DIR, f"{MACHINE_ID}.lock"))

        else:
            logging.info("Resuming from existing TMP_PROCESSING files...")
            chunk = []  # no new files claimed; this avoids moving from IN_PROGRESS later

        task_info = []
        total_bytes = 0
        for src, rel in chunk:
            size = os.path.getsize(src)
            for crf in CRF_VALUES:
                task_info.append((src, rel, crf, size))
                total_bytes += size

        manager = Manager()
        shared_bytes = manager.Value('i', 0)

        stop_event = threading.Event()
        size_pbar = tqdm(total=total_bytes, unit='B', unit_scale=True, desc="Total Progress", position=0)
        pbar_thread = threading.Thread(target=update_size_pbar, args=(size_pbar, shared_bytes, total_bytes, stop_event))
        pbar_thread.start()

        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(encode_file, src, rel, crf, shared_bytes)
                    for src, rel, crf, _ in task_info]
            for future in as_completed(futures):
                try:
                    result = future.result()  # result is now [src_file, crf, status, message]
                    src_file, crf, status, message = result
                    tqdm.write(f"[CRF {crf}] {status.upper()}: {message}")
                    logging.info(f"[CRF {crf}] {status.upper()}: {message}")
                    results.append(result)
                except Exception as e:
                    logging.error(f"Encoding task failed: {e}")
                    with open("failed_encodes.log", "a") as f:
                        f.write(f"{e}\n")
                    results.append([None, None, "failed", str(e)])

        size_pbar.n = total_bytes
        size_pbar.refresh()
        size_pbar.close()

        shared_bytes.value = total_bytes
        # stop_event.set()  # Tells the thread to stop
        pbar_thread.join()

        # Determine which files succeeded entirely
        success_map = {}
        for src_file, crf, status, message in results:
            if src_file is None:
                continue
            rel_path = next((rel for src, rel, crf_val, _ in task_info if src == src_file and crf_val == crf), None)
            if rel_path:
                success_map.setdefault(rel_path, []).append(status == "success")

        for rel in success_map:
            all_success = all(success_map[rel])
            target_dir = DONE_DIR if all_success else FAILED_DIR
            src_path = os.path.join(IN_PROGRESS, rel)
            dst_path = os.path.join(target_dir, rel)
            os.makedirs(os.path.dirname(dst_path), exist_ok=True)

            # If failed, remove any corresponding tmp_output files
            if not all_success:
                for crf in CRF_VALUES:
                    tmp_output_file = os.path.join(TMP_OUTPUT_ROOT.format(crf), rel)
                    if os.path.exists(tmp_output_file):
                        try:
                            os.remove(tmp_output_file)
                            logging.info(f"Removed temp output file for failed encode: {tmp_output_file}")
                        except Exception as e:
                            logging.warning(f"Failed to remove temp output file {tmp_output_file}: {e}")

            try:
                move_with_progress(src_path, dst_path, desc=f"Moving {os.path.basename(rel)}")
                logging.debug(f"Moved to {'done' if all_success else 'failed'}: {rel}")
            except Exception as e:
                logging.error(f"Failed to move file {rel} to final dir: {e}")

        cleanup_working_folders()
        logging.info(f"{MACHINE_ID} finished processing batch")
        move_logs_to_central_output()
        is_keyboard_interrupt = False
        try:
            sleep_time = 300  # total seconds
            for _ in tqdm(range(sleep_time), desc="Sleeping", unit="s"):
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nInterrupted by user. Exiting cleanly.")
            is_keyboard_interrupt = True
        finally:
            tqdm.write("Done or interrupted. Cleaning up...")
            if is_keyboard_interrupt:
                preload_stop_event.set()
                break

if __name__ == "__main__":
    main()
