import os
import re
import pathlib
import signal
import shutil
import threading
import time
from multiprocessing import Manager, Process, JoinableQueue, RLock, Event, Value
from queue import Empty
from dataclasses import dataclass

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
from helpers.remove_path import remove_path
from includes.claim_files import claim_files
from includes.encode_file import encode_file
from includes.move_logs_to_central_output import move_logs_to_central_output
from includes.move_done_if_all_crf_outputs_exist import move_done_if_all_crf_outputs_exist
from includes.remove_empty_dirs_in_path import remove_empty_dirs_in_path

@dataclass
class EncodingTask:
    src_path: str
    rel_path: str
    crf: int
    chunk: str

class JobManager:
    def __init__(self):
        self.preload_done = threading.Event()
        self.max_workers = MAX_WORKERS
        self.crf_values = CRF_VALUES
        self.chunk_size = CHUNK_SIZE
        self.tmp_input_dir = TMP_INPUT
        self.in_progress_dir = IN_PROGRESS

        # IMPORTANT: JoinableQueue for proper completion semantics
        self.task_queue = JoinableQueue()

        self.manager = Manager()
        self.light_threads = []
        self.processes = [] 
        self.process_registry = self.manager.dict()  # pid -> subprocess pid
        self.active_jobs = Value('i', 0)

        # Deterministic task accounting (don’t trust Queue.empty())
        self.total_tasks = Value('i', 0)
        self.completed_tasks = Value('i', 0)

        self.stop_event = Event()
        self.file_moving_lock = RLock()
        self.bytes_encoded = self.manager.Value('q', 0)

        # per-chunk totals & live progress (bytes)
        self.chunk_totals = self.manager.dict()     # {chunk_name: total_bytes}
        self.chunk_progress = self.manager.dict()   # {chunk_name: Value('q')}

    def start(self):
        # Queue any pre-existing inputs first so workers won’t starve on startup.
        self._preload_existing_input_chunks()
        self._start_workers()
        self._start_preloader()
        # Small head-start for the preloader is fine but not required.

    def _start_preloader(self):
        t = threading.Thread(target=self._preload_loop, daemon=True)
        t.start()
        self.light_threads.append(t)

    def _start_workers(self):
        # One dedicated UI "slot" per worker
        for slot_idx in range(1, self.max_workers + 1):
            p = Process(target=self._worker_loop, args=(slot_idx,), daemon=True)
            p.start()
            self.processes.append(p)

    def _preload_loop(self):
        log("Preloader started")
        while (not self.stop_event.is_set()) and (not self.preload_done.is_set()):
            with self.active_jobs.get_lock():
                if self.active_jobs.value <= max(1, self.max_workers // 2):
                    chunk = claim_files()
                    if chunk:
                        # infer chunk name from first file’s rel path
                        _, _, first_rel = chunk[0]
                        chunk_name = get_topmost_dir(first_rel)

                        # 1) SIZE THE CHUNK (fast: file sizes only) — no tqdm
                        total_input_bytes = 0
                        for src, _, _ in chunk:
                            try:
                                total_input_bytes += os.path.getsize(src) * len(self.crf_values)
                            except FileNotFoundError:
                                pass

                        self.chunk_totals[chunk_name] = total_input_bytes
                        self.chunk_progress[chunk_name] = self.manager.Value('q', 0)

                        for src, dst, rel in chunk:
                            local_dst = os.path.join(self.tmp_input_dir, rel)
                            copy_with_progress(src, local_dst, desc=f"Preloading {os.path.basename(rel)}")
                            move_with_progress(src, dst, desc=f"Preloading {os.path.basename(rel)}")
                            for crf in self.crf_values:
                                self._enqueue_task(EncodingTask(local_dst, rel, crf, chunk_name))
                    else:
                        log("No more files to claim. Marking preload as done.")
                        self.preload_done.set()
                        break
            time.sleep(0.5)

    def _enqueue_task(self, task: EncodingTask):
        self.task_queue.put(task)
        with self.total_tasks.get_lock():
            self.total_tasks.value += 1

    def _worker_loop(self, slot_idx: int):
        while not self.stop_event.is_set():
            try:
                task = self.task_queue.get(timeout=1)
            except Empty:
                continue

            # Sentinel -> clean exit
            if task is None:
                self.task_queue.task_done()
                break

            self._increment_jobs()
            try:
                result = encode_file(
                    task.src_path,
                    task.rel_path,
                    task.crf,
                    slot_idx,
                    self.bytes_encoded,
                    process_registry=self.process_registry,
                    chunk_progress=self.chunk_progress,
                    chunk_key=task.chunk,
                )
                self._handle_encoding_result(task, result)
            except Exception as e:
                log(f"Worker loop encountered an error: {e}", level="error")
            finally:
                with self.completed_tasks.get_lock():
                    self.completed_tasks.value += 1
                self._decrement_jobs()
                self.task_queue.task_done()

    def _handle_encoding_result(self, task, result):
        crf = task.crf
        paths = self._construct_paths(task)
        status = result[2]

        # If we skipped before any encoding happened, advance the chunk by the file size for this CRF,
        # so the bar remains accurate.
        if status in ("skipped-alreadyexists-main", "skipped-alreadyexists-tmp", "skipped-notsupported"):
            try:
                sz = os.path.getsize(task.src_path)
            except FileNotFoundError:
                sz = 0
            try:
                self.chunk_progress[task.chunk].value += sz
            except KeyError:
                pass

        with self.file_moving_lock:
            self._process_result_status(status, paths)
            self._maybe_cleanup_and_finalize(task, paths)

        log(f"[CRF {crf}] {status.upper()}: {result[3]}")

    def _construct_paths(self, task):
        rel = task.rel_path
        crf = task.crf
        return {
            "processing": os.path.join(TMP_PROCESSING.format(crf), rel),
            "output": os.path.join(TMP_OUTPUT_ROOT.format(crf), rel),
            "failed": os.path.join(TMP_FAILED_ROOT.format(crf), rel),
            "skipped": os.path.join(TMP_SKIPPED_ROOT.format(crf), rel),
        }

    def _process_result_status(self, status, paths):
        if status in ("failed", "skipped-notsupported"):
            remove_path(paths["processing"])
            self._touch_file(paths["failed"])
        elif status == "skipped-alreadyexists-main":
            self._touch_file(paths["skipped"])
        elif status == "skipped-alreadyexists-tmp":
            pass
        elif status == "success":
            move_with_progress(paths["processing"], paths["output"], desc=f"Moving {os.path.basename(paths['processing'])}")
        else:
            log(f"Unknown supported result type: {status}", level="error")

    def _touch_file(self, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        pathlib.Path(path).touch(exist_ok=True)

    def _preload_existing_input_chunks(self):
        log("Scanning TMP_INPUT for pre-existing chunks...")
        task_count = 0

        for dirpath, _, filenames in os.walk(self.tmp_input_dir):
            for filename in filenames:
                if not filename.lower().endswith(".mp4"):
                    continue

                src_path = os.path.join(dirpath, filename)
                rel_path = os.path.relpath(src_path, self.tmp_input_dir)

                chunk_name = get_topmost_dir(rel_path)
                for crf in self.crf_values:
                    self._enqueue_task(EncodingTask(src_path, rel_path, crf, chunk_name))
                    task_count += 1

        log(f"Queued {task_count} tasks from existing TMP_INPUT chunk folders.")

    def _maybe_cleanup_and_finalize(self, task, paths):
        if not self._all_crf_outputs_exist(task.rel_path):
            return

        chunk_folder = get_topmost_dir(task.rel_path)
        chunk_path = os.path.join(TMP_INPUT, chunk_folder)

        remove_path(task.src_path)
        remove_empty_dirs_in_path(task.src_path, [chunk_path])

        if not os.listdir(chunk_path):
            self._finalize_chunk(chunk_folder)

    def _all_crf_outputs_exist(self, rel_path):
        for crf in CRF_VALUES:
            paths = [
                os.path.join(TMP_OUTPUT_ROOT.format(crf), rel_path),
                os.path.join(TMP_FAILED_ROOT.format(crf), rel_path),
                os.path.join(TMP_SKIPPED_ROOT.format(crf), rel_path),
            ]
            if not any(os.path.exists(p) for p in paths):
                return False
        return True

    def _finalize_chunk(self, chunk_folder):
        self._touch_file(os.path.join(TMP_INPUT, f"{chunk_folder}.done"))
        os.rmdir(os.path.join(TMP_INPUT, chunk_folder))
        self._remove_skipped_files(chunk_folder)
        self._remove_processing_folder(chunk_folder)
        self._transfer_failed_tasks()
        self._move_outputs_and_mark_done(chunk_folder)
        move_logs_to_central_output()

    def _remove_skipped_files(self, chunk_folder):
        for crf in CRF_VALUES:
            path = os.path.join(TMP_SKIPPED_ROOT.format(crf), chunk_folder)
            remove_path(path)
            remove_empty_dirs_in_path(path, [os.path.dirname(os.path.dirname(TMP_SKIPPED_ROOT))])

    def _transfer_failed_tasks(self):
        for crf in CRF_VALUES:
            failed_root = TMP_FAILED_ROOT.format(crf)
            for root, _, files in os.walk(failed_root):
                for file in files:
                    failed_file_path = os.path.join(root, file)
                    with_chunk_rel_path = os.path.relpath(failed_file_path, failed_root)
                    rel_path = remove_topmost_dir(with_chunk_rel_path)
                    in_progress_path = os.path.join(IN_PROGRESS, rel_path)
                    failed_path = os.path.join(FAILED_DIR, rel_path)

                    try:
                        if os.path.exists(in_progress_path):
                            move_with_progress(in_progress_path, failed_path)
                            log(f"Moved failed task from in_progress to failed: {rel_path}")
                        else:
                            log(f"In-progress file for failed task not found: {in_progress_path}", level="warning")
                    except Exception as e:
                        log(f"Error moving failed task {rel_path}: {e}", level="error")

                    for crf_failed in CRF_VALUES:
                        delete_path = os.path.join(TMP_FAILED_ROOT.format(crf_failed), with_chunk_rel_path)
                        if os.path.exists(delete_path):
                            remove_path(delete_path)
                            log(f"Deleted failed file from tmp_failed (crf={crf_failed}): {delete_path}")

    def _move_outputs_and_mark_done(self, chunk_folder):
        all_crf_file_sets = []
        for crf in CRF_VALUES:
            chunk_crf_path = os.path.join(TMP_OUTPUT_ROOT.format(crf), chunk_folder)
            if os.path.exists(chunk_crf_path):
                crf_files = set()
                for dirpath, _, filenames in os.walk(chunk_crf_path):
                    for f in filenames:
                        rel_path = os.path.relpath(os.path.join(dirpath, f), chunk_crf_path)
                        crf_files.add(rel_path)
                all_crf_file_sets.append(crf_files)

        common_files = set.intersection(*all_crf_file_sets) if all_crf_file_sets else set()

        for crf in CRF_VALUES:
            chunk_crf_path = os.path.join(TMP_OUTPUT_ROOT.format(crf), chunk_folder)
            final_output_path = FINAL_OUTPUT_ROOT.format(crf)
            if os.path.exists(chunk_crf_path):
                move_with_progress(chunk_crf_path, final_output_path, False, True)
            remove_empty_dirs_in_path(chunk_crf_path, [os.path.dirname(os.path.dirname(TMP_OUTPUT_ROOT))])

        for rel_file in common_files:
            src_file = os.path.join(IN_PROGRESS, rel_file)
            dst_file = os.path.join(DONE_DIR, rel_file)
            if os.path.exists(src_file):
                move_with_progress(src_file, dst_file)

        # Move the remaining files, if any
        move_done_if_all_crf_outputs_exist()

    def _increment_jobs(self):
        with self.active_jobs.get_lock():
            self.active_jobs.value += 1

    def _decrement_jobs(self):
        with self.active_jobs.get_lock():
            self.active_jobs.value -= 1

    def _remove_processing_folder(self, chunk_folder):
        for crf in CRF_VALUES:
            path = os.path.join(TMP_PROCESSING.format(crf), chunk_folder)
            remove_empty_dirs_in_path(path, [os.path.dirname(os.path.dirname(TMP_PROCESSING))])

    def shutdown(self):
        # Request stop, and make sure workers can unblock from .get()
        self.stop_event.set()
        try:
            # Push sentinels to unblock any idle workers
            for _ in range(self.max_workers):
                self.task_queue.put_nowait(None)
        except Exception:
            pass

        # Kill any external encoder processes we’ve tracked
        for pid in list(self.process_registry.values()):
            try:
                # log(f"Killing encoder process id: {pid}")
                os.kill(pid, signal.SIGKILL)
                self.process_registry.pop(pid, None)
            except Exception as e:
                log(f"Failed to kill process {pid}: {e}", level="warning")

        # Give workers a moment to exit gracefully
        # deadline = time.time() + 5
        for p in self.processes:
            p.join(timeout=3)
            if p.is_alive():
                p.terminate()
                p.join(timeout=2)
                if p.is_alive():
                    p.kill()
        self.processes.clear()

        for t in self.light_threads:
            t.join(timeout=2)
        self.light_threads.clear()


        try:
            self.manager.shutdown()
        except Exception:
            pass

    def get_encoded_bytes(self):
        return self.bytes_encoded.value

    def is_done(self):
        # Deterministic: all tasks that were enqueued have completed,
        # and the preloader has finished creating tasks.
        return (
            self.preload_done.is_set() and
            self.completed_tasks.value >= self.total_tasks.value and
            self.active_jobs.value == 0
        )
