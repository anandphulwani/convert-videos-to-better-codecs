import os
import pathlib
import shutil
import threading
from multiprocessing import Manager
import time
from queue import Empty, Queue
from dataclasses import dataclass
import time

from config import ( 
    IN_PROGRESS, DONE_DIR, FAILED_DIR, 
    TMP_INPUT, TMP_OUTPUT_ROOT, FINAL_OUTPUT_ROOT, CRF_VALUES,
    TMP_PROCESSING, TMP_FAILED_ROOT, TMP_SKIPPED_ROOT,
    MAX_WORKERS, CHUNK_SIZE
)
from helpers.get_topmost_dir import get_topmost_dir
from helpers.remove_topmost_dir import remove_topmost_dir
from helpers.logging_utils import log
from helpers.copy_and_move_with_progress import copy_with_progress, move_with_progress
from includes.claim_files import claim_files
from includes.encode_file import encode_file
from includes.move_done_if_all_crf_outputs_exist import move_done_if_all_crf_outputs_exist
from includes.remove_empty_dirs_in_path import remove_empty_dirs_in_path

@dataclass
class EncodingTask:
    src_path: str
    rel_path: str
    crf: int

_file_moving_lock = threading.RLock()

class JobManager:
    def __init__(self):
        self.preload_done = threading.Event()
        self.max_workers = MAX_WORKERS
        self.crf_values = CRF_VALUES
        self.chunk_size = CHUNK_SIZE
        self.tmp_input_dir = TMP_INPUT
        self.in_progress_dir = IN_PROGRESS
        self.task_queue = Queue()
        self.active_jobs = 0
        self.active_jobs_lock = threading.Lock()
        self.stop_event = threading.Event()
        self.manager = Manager()
        self.bytes_encoded = self.manager.Value('i', 0)
        self.video_seconds_encoded = self.manager.Value('i', 0)
        self.threads = []

    def start(self):
        self._start_preloader()
        self._start_workers()

    def _start_preloader(self):
        t = threading.Thread(target=self._preload_loop, daemon=True)
        t.start()
        self.threads.append(t)

    def _start_workers(self):
        for _ in range(self.max_workers):
            t = threading.Thread(target=self._worker_loop, daemon=True)
            t.start()
            self.threads.append(t)

    def _preload_loop(self):
        log("Preloader started")
        while (not self.stop_event.is_set()) and (not self.preload_done.is_set()):
            with self.active_jobs_lock:
                log(f"Active jobs: {self.active_jobs}, Max workers: {self.max_workers}", level="debug")
                if self.active_jobs <= max(1, self.max_workers // 2):
                    chunk = claim_files()
                    if chunk:
                        for src, dst, rel in chunk:
                            local_dst = os.path.join(self.tmp_input_dir, rel)
                            copy_with_progress(src, local_dst, desc=f"Preloading {os.path.basename(rel)}")
                            move_with_progress(src, dst, desc=f"Preloading {os.path.basename(rel)}")
                            for crf in self.crf_values:
                                self.task_queue.put(EncodingTask(local_dst, rel, crf))
                    else:
                        log("No more files to claim. Marking preload as done.")
                        self.preload_done.set()
            time.sleep(5)

    def _worker_loop(self):
        while not self.stop_event.is_set():
            try:
                task = self.task_queue.get(timeout=5)
            except Empty:
                continue

            self._increment_jobs()
            try:
                # result = encode_file(task.src_path, task.rel_path, crf, self.bytes_encoded)
                result = encode_file(task.src_path, task.rel_path, task.crf, self.bytes_encoded, self.video_seconds_encoded)

                tmp_processing_dir = TMP_PROCESSING.format(task.crf)
                tmp_output_dir = TMP_OUTPUT_ROOT.format(task.crf)
                tmp_failed_dir = TMP_FAILED_ROOT.format(task.crf)
                tmp_skipped_dir = TMP_SKIPPED_ROOT.format(task.crf)

                # final_output_dir = FINAL_OUTPUT_ROOT.format(task.crf)
                tmp_processing_file = os.path.join(tmp_processing_dir, task.rel_path)
                tmp_output_file = os.path.join(tmp_output_dir, task.rel_path)
                tmp_failed_file = os.path.join(tmp_failed_dir, task.rel_path)
                tmp_skipped_file = os.path.join(tmp_skipped_dir, task.rel_path)
                # final_output_file = os.path.join(final_output_dir, remove_topmost_dir(task.rel_path))

                with _file_moving_lock:
                    if result[2] == "failed" or result[2] == "skipped-notsupported":
                        if os.path.exists(tmp_processing_file):
                            os.remove(tmp_processing_file)
                        os.makedirs(os.path.dirname(tmp_failed_file), exist_ok=True)
                        pathlib.Path(tmp_failed_file).touch(exist_ok=True)
                    elif result[2] == "skipped-alreadyexists-main":
                        os.makedirs(os.path.dirname(tmp_skipped_file), exist_ok=True)
                        pathlib.Path(tmp_skipped_file).touch(exist_ok=True)
                    elif result[2] == "skipped-alreadyexists-tmp":
                        pass
                    elif result[2] == "success":
                        move_with_progress(tmp_processing_file, tmp_output_file, desc=f"Moving {os.path.basename(tmp_processing_file)}")
                    else:
                        log(f"Unknown supported result type", level="error")
                        return
                    remove_empty_dirs_in_path(tmp_processing_file, [os.path.dirname(TMP_PROCESSING)])

                    # Check if all CRF outputs exist, and if so, delete source file
                    all_crf_outputs_exist = True
                    for crf_check in CRF_VALUES:
                        output_path = os.path.join(TMP_OUTPUT_ROOT.format(crf_check), task.rel_path)
                        failed_path = os.path.join(TMP_FAILED_ROOT.format(crf_check), task.rel_path)
                        skipped_path = os.path.join(TMP_SKIPPED_ROOT.format(crf_check), task.rel_path)
                        # log(f"output_path: {output_path}, Condition: {os.path.exists(output_path)}")
                        # log(f"failed_path: {failed_path}, Condition: {os.path.exists(failed_path)}")
                        # log(f"Group Condition: {not (os.path.exists(output_path) or os.path.exists(failed_path))}")
                        if not (os.path.exists(output_path) or os.path.exists(failed_path) or os.path.exists(skipped_path)):
                            # log(f"Setting `all_crf_outputs_exist` to `False`.")
                            all_crf_outputs_exist = False
                            break

                    if all_crf_outputs_exist:
                        try:
                            # log(f"Removing source path: {task.src_path}")
                            os.remove(task.src_path)
                            remove_empty_dirs_in_path(os.path.dirname(task.src_path), [TMP_INPUT])
                        except Exception as e:
                            log(f"Failed to delete source file {task.src_path}: {e}", level="error")

                    # Check if `tmp_input\chunkXX` is removed, then we can 
                    # transfer all items which failed in `tmp_failed\av1_crf{}`, transfer from `IN_PROGRESS` to `FAILED`
                    # transfer `tmp_output\av1_crf{}` directories to the `FINAL_OUTPUT_ROOT_av1_crf{}`
                    if not os.path.exists(os.path.join(TMP_INPUT, get_topmost_dir(task.rel_path))):
                        log(f"{get_topmost_dir(task.rel_path)} folder is removed.")

                        # Removing all skipped files as they are no longer required.
                        for crf in CRF_VALUES:
                            skipped_path = os.path.join(TMP_SKIPPED_ROOT.format(crf), get_topmost_dir(task.rel_path))
                            if os.path.exists(skipped_path):
                                shutil.rmtree(skipped_path)
                            remove_empty_dirs_in_path(skipped_path, [os.path.dirname(os.path.dirname(TMP_SKIPPED_ROOT))])

                        log("We are going to process the failure now.")
                        # transfer all items which failed in `tmp_failed\av1_crf{}`, transfer from `IN_PROGRESS` to `FAILED`
                        for crf in CRF_VALUES:
                            failed_root = TMP_FAILED_ROOT.format(crf)
                            for root, _, files in os.walk(failed_root):
                                for file in files:
                                    failed_file_path = os.path.join(root, file)

                                    # Relative path inside crf folder: chunkXX/filename.mp4
                                    with_chunk_rel_path = os.path.relpath(failed_file_path, failed_root)
                                    rel_path = remove_topmost_dir(with_chunk_rel_path)

                                    in_progress_path = os.path.join(IN_PROGRESS, rel_path)
                                    failed_path = os.path.join(FAILED_DIR, rel_path)

                                    # Attempt to move only once, from in_progress to failed_dir
                                    try:
                                        if os.path.exists(in_progress_path):
                                            move_with_progress(in_progress_path, failed_path)
                                            log(f"Moved failed task from in_progress to failed: {rel_path}")
                                        else:
                                            log(f"In-progress file for failed task not found: {in_progress_path}", level="warning")
                                    except Exception as e:
                                        log(f"Error moving failed task {rel_path}: {e}", level="error")

                                    # Now, delete this file from all TMP_FAILED_ROOT_crf
                                    for crf_delete in CRF_VALUES:
                                        delete_path = os.path.join(TMP_FAILED_ROOT.format(crf_delete), with_chunk_rel_path)
                                        if os.path.exists(delete_path):
                                            try:
                                                os.remove(delete_path)
                                                log(f"Deleted failed file from tmp_failed (crf={crf_delete}): {delete_path}")
                                            except Exception as e:
                                                log(f"Error deleting failed file from tmp_failed (crf={crf_delete}): {e}", level="error")
                        
                        # --- Step 1: Collect all file sets per CRF ---
                        all_crf_file_sets = []
                        for crf_check in CRF_VALUES:
                            chunk_crf_path = os.path.join(TMP_OUTPUT_ROOT.format(crf_check), get_topmost_dir(task.rel_path))
                            if os.path.exists(chunk_crf_path):
                                crf_files = set()
                                for dirpath, _, filenames in os.walk(chunk_crf_path):
                                    for f in filenames:
                                        rel_path = os.path.relpath(os.path.join(dirpath, f), chunk_crf_path)
                                        crf_files.add(rel_path)
                                all_crf_file_sets.append(crf_files)

                        # --- Step 2: Find common files ---
                        common_files = set.intersection(*all_crf_file_sets) if all_crf_file_sets else set()

                        # --- Step 3: transfer `tmp_output\av1_crf{}` directories to the `FINAL_OUTPUT_ROOT_av1_crf{}`
                        for crf_check in CRF_VALUES:
                            chunk_crf_path = os.path.join(TMP_OUTPUT_ROOT.format(crf_check), get_topmost_dir(task.rel_path))
                            final_output_path = FINAL_OUTPUT_ROOT.format(crf_check)
                            if os.path.exists(chunk_crf_path):
                                move_with_progress(chunk_crf_path, final_output_path, True, True)

                        # --- Step 4: Move common files from IN_PROGRESS to DONE ---
                        for rel_file in common_files:
                            src_file = os.path.join(IN_PROGRESS, rel_file)
                            dst_file = os.path.join(DONE_DIR, rel_file)
                            if os.path.exists(src_file):
                                move_with_progress(src_file, dst_file)
                        
                        move_done_if_all_crf_outputs_exist()

                log(f"[CRF {task.crf}] {result[2].upper()}: {result[3]}")
            finally:
                self._decrement_jobs()

    def _increment_jobs(self):
        with self.active_jobs_lock:
            self.active_jobs += 1

    def _decrement_jobs(self):
        with self.active_jobs_lock:
            self.active_jobs -= 1

    def shutdown(self):
        self.stop_event.set()
        for t in self.threads:
            t.join(timeout=5)

    def get_encoded_bytes(self):
        return self.bytes_encoded.value

    def is_done(self):
        return self.preload_done.is_set() and self.task_queue.empty() and self.active_jobs == 0

