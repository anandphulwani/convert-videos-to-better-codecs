import os
import pathlib
import signal
import threading
import time
from multiprocessing import Manager, Process, JoinableQueue, RLock, Event, Value
from queue import Empty
from dataclasses import dataclass

from config import ( 
    args,
    IN_PROGRESS, DONE_DIR, FAILED_DIR, 
    TMP_INPUT, TMP_OUTPUT_ROOT, FINAL_OUTPUT_ROOT, CRF_VALUES,
    TMP_PROCESSING, TMP_FAILED_ROOT, TMP_SKIPPED_ROOT,
    MAX_WORKERS, CHUNK_SIZE, LOCKS_DIR
)
from helpers.format_elapsed import format_elapsed
from helpers.get_topmost_dir import get_topmost_dir
from helpers.remove_topmost_dir import remove_topmost_dir
from helpers.logging_utils import log, setup_logging
from helpers.copy_and_move_with_progress import copy_with_progress, move_with_progress
from helpers.remove_path import remove_path
from includes.claim_files import claim_files
from includes.encode_file import encode_file
from includes.move_logs_to_central_output import move_logs_to_central_output
from includes.move_done_if_all_crf_outputs_exist import move_done_if_all_crf_outputs_exist
from includes.remove_empty_dirs_in_path import remove_empty_dirs_in_path
from includes.remote_transfer_locks import try_acquire_remote_transfer_lock_loop, renew_remote_transfer_lock
from includes.cleanup_working_folders import clear_tmp_processing
from includes.cpu_watchdog import cpu_watchdog
from includes.online_all_system_cores import online_all_system_cores
from includes.state import pause_flag
from helpers.logging_utils import log

@dataclass
class EncodingTask:
    src_path: str
    rel_path: str
    crf: int
    chunk: str

class JobManager:
    def __init__(self, event_queue, tqdm_manager):
        self.preload_done = threading.Event()
        self.max_workers = MAX_WORKERS
        self.crf_values = CRF_VALUES
        self.chunk_size = CHUNK_SIZE
        self.tmp_input_dir = TMP_INPUT
        self.in_progress_dir = IN_PROGRESS
        self.event_queue = event_queue
        self.tqdm_manager = tqdm_manager

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
        self.pause_event = Event()
        self.file_moving_lock = RLock()
        self.bytes_encoded = self.manager.Value('q', 0)

        # per-chunk totals & live progress (bytes)
        self.chunk_totals = self.manager.dict()     # {chunk_name: total_bytes}
        self.chunk_progress = self.manager.dict()   # {chunk_name: Value('q')}

        self.start_time = time.time()

    def start(self):
        # Queue any pre-existing inputs first so workers won’t starve on startup.
        self._preload_existing_input_chunks()
        self._start_workers()
        self._start_preloader()
        self._start_online_all_system_cores()
        self._start_cpu_watchdog() if args.throttle else None
        self._start_pause_monitor() if args.throttle else None
        # Small head-start for the preloader is fine but not required.

    def _start_online_all_system_cores(self):
        t = threading.Thread(target=online_all_system_cores, name="_start_online_all_system_cores", args=(self.stop_event,), daemon=True)
        t.start()
        self.light_threads.append(t)

    def _start_cpu_watchdog(self):
        t = threading.Thread(target=cpu_watchdog, name="_start_cpu_watchdog", args=(self.stop_event,), daemon=True)
        t.start()
        self.light_threads.append(t)

    def _start_pause_monitor(self):
        t = threading.Thread(target=self._pause_monitor_loop, name="_start_pause_monitor", args=(self.stop_event, self.event_queue), daemon=True)
        t.start()
        self.light_threads.append(t)

    def _pause_monitor_loop(self, stop_event, event_queue):
        was_paused = False
        while not stop_event.is_set():
            if pause_flag.is_set():
                if not was_paused:
                    log("Pause detected — stopping workers and clearing TMP_PROCESSING...", level="info")
                    self.pause_event.set()
                    event_queue.put({"op": "pause_tqdm_manager"})
                    self.pause()
                    move_logs_to_central_output(event_queue, True)
                    clear_tmp_processing()
                    was_paused = True
            else:
                if was_paused:
                    log("Resume detected — restarting workers...", level="info")
                    was_paused = False
                    event_queue.put({"op": "resume_tqdm_manager"})
                    self._preload_existing_input_chunks()
                    self._start_preloader()
                    self.pause_event.clear()
            time.sleep(1)

    def _start_preloader(self):
        t = threading.Thread(target=self._preload_loop, name="_start_preloader", args=(self.stop_event, self.pause_event, self.event_queue), daemon=True)
        t.start()
        self.light_threads.append(t)

    def _start_workers(self):
        # One dedicated UI "slot" per worker
        for slot_idx in range(1, self.max_workers + 1):
            p = Process(target=self._worker_loop, args=(slot_idx, self.event_queue), daemon=True)
            p.start()
            self.processes.append(p)

    def _preload_loop(self, stop_event, pause_event, event_queue):
        log("Preloader started")
        while (not stop_event.is_set()) and (not self.preload_done.is_set()):
            if pause_event.is_set():
                time.sleep(1)
                continue
            with self.active_jobs.get_lock():
                if self.active_jobs.value <= max(1, self.max_workers // 2):

                    # 1. Try to acquire the remote transfer lock
                    try_acquire_remote_transfer_lock_loop()

                    # 2. Start a thread to renew the lock
                    renew_lock_stop_event = threading.Event()
                    renew_thread = threading.Thread(
                        target=renew_remote_transfer_lock, 
                        args=(renew_lock_stop_event,), 
                        daemon=True
                    )
                    renew_thread.start()

                    try:
                        chunk = claim_files(event_queue)
                        if chunk:
                            _, _, first_rel = chunk[0]
                            chunk_name = get_topmost_dir(first_rel)

                            # Size the chunk
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
                                copy_with_progress(src, local_dst, event_queue, desc=f"Preloading (Copy) to Local: {os.path.basename(rel)}")
                                move_with_progress(src, dst, event_queue, desc=f"Preloading (Move) to InProgress: {os.path.basename(rel)}")
                                for crf in self.crf_values:
                                    self._enqueue_task(EncodingTask(local_dst, rel, crf, chunk_name))
                        else:
                            log("No more files to claim. Marking preload as done.")
                            self.preload_done.set()
                            break
                    finally:
                        # 3. Stop the renew thread and clean up the lock
                        renew_lock_stop_event.set()
                        renew_thread.join()
                        try:
                            lock_path = os.path.join(LOCKS_DIR, f"remote_fetching.lock")
                            remove_path(lock_path)
                            log("Lock file removed.", level="debug")
                        except FileNotFoundError:
                            log(f"Lock file already removed.", level="error")
            time.sleep(0.5)

    def _enqueue_task(self, task: EncodingTask):
        self.task_queue.put(task)
        with self.total_tasks.get_lock():
            self.total_tasks.value += 1

    def _worker_loop(self, slot_idx: int, event_queue):
        while not self.stop_event.is_set():
            if self.pause_event.is_set():
                time.sleep(5)
                continue

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
                    event_queue=event_queue,
                    process_registry=self.process_registry,
                    chunk_progress=self.chunk_progress,
                    chunk_key=task.chunk,
                    pause_event=self.pause_event,
                )
                self._handle_encoding_result(task, result, event_queue)
            except Exception as e:
                log(f"Worker loop encountered an error: {e}", level="error")
            finally:
                with self.completed_tasks.get_lock():
                    self.completed_tasks.value += 1
                self._decrement_jobs()
                self.task_queue.task_done()

    def _handle_encoding_result(self, task, result, event_queue):
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
            self._process_result_status(status, paths, event_queue)
            self._maybe_cleanup_and_finalize(task, paths, event_queue)

        if status != "failed-paused":
            elapsed = time.time() - self.start_time
            log(f"[CRF {crf}] {status.upper()}: {result[3]} (Time since start: {format_elapsed(elapsed)})")

    def _construct_paths(self, task):
        rel = task.rel_path
        crf = task.crf
        return {
            "processing": os.path.join(TMP_PROCESSING.format(crf), rel),
            "output": os.path.join(TMP_OUTPUT_ROOT.format(crf), rel),
            "failed": os.path.join(TMP_FAILED_ROOT.format(crf), rel),
            "skipped": os.path.join(TMP_SKIPPED_ROOT.format(crf), rel),
        }

    def _process_result_status(self, status, paths, event_queue):
        if status in ("failed", "skipped-notsupported"):
            remove_path(paths["processing"])
            self._touch_file(paths["failed"])
        elif status == "failed-paused":
            remove_path(paths["processing"])
        elif status == "skipped-alreadyexists-main":
            self._touch_file(paths["skipped"])
        elif status == "skipped-alreadyexists-tmp":
            pass
        elif status == "success":
            move_with_progress(paths["processing"], paths["output"], event_queue, desc=f"Moving {os.path.basename(paths['processing'])}")
        else:
            log(f"Unknown supported result type: {status}", level="error")

    def _touch_file(self, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        pathlib.Path(path).touch(exist_ok=True)

    def _preload_existing_input_chunks(self):
        log("Scanning TMP_INPUT for pre-existing chunks...")
        task_count = 0

        for _, dirnames, _ in os.walk(self.tmp_input_dir):
            for chunk_name in dirnames:
                if chunk_name.startswith("chunk"):
                    chunk_dir = os.path.join(self.tmp_input_dir, chunk_name)
                    log(f"Processing chunk directory: {chunk_dir}", level="debug")

                    total_input_bytes = 0
                    
                    for dirpath, _, filenames in os.walk(chunk_dir):
                        for filename in filenames:
                            if not filename.lower().endswith(".mp4"):
                                continue

                            src_path = os.path.join(dirpath, filename)
                            rel_path = os.path.relpath(src_path, self.tmp_input_dir)

                            try:
                                total_input_bytes += os.path.getsize(src_path) * len(self.crf_values)
                            except FileNotFoundError:
                                pass

                            for crf in self.crf_values:
                                self._enqueue_task(EncodingTask(src_path, rel_path, crf, chunk_name))
                                task_count += 1

                    self.chunk_totals[chunk_name] = total_input_bytes
                    self.chunk_progress[chunk_name] = self.manager.Value('q', 0)
                        

        log(f"Queued {task_count} tasks from existing TMP_INPUT chunk folders.")

    def _maybe_cleanup_and_finalize(self, task, paths, event_queue):
        if not self._all_crf_outputs_exist(task.rel_path):
            return

        chunk_folder = get_topmost_dir(task.rel_path)
        chunk_path = os.path.join(TMP_INPUT, chunk_folder)

        remove_path(task.src_path)
        remove_empty_dirs_in_path(task.src_path, [chunk_path])

        if not os.listdir(chunk_path):
            self._finalize_chunk(chunk_folder, event_queue)

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

    def _finalize_chunk(self, chunk_folder, event_queue):
        self._touch_file(os.path.join(TMP_INPUT, f"{chunk_folder}.done"))
        os.rmdir(os.path.join(TMP_INPUT, chunk_folder))
        self._remove_skipped_files(chunk_folder)
        self._remove_processing_folder(chunk_folder)
        self._transfer_failed_tasks(event_queue)
        self._move_outputs_and_mark_done(chunk_folder, event_queue)
        move_logs_to_central_output(event_queue, True)

    def _remove_skipped_files(self, chunk_folder):
        for crf in CRF_VALUES:
            path = os.path.join(TMP_SKIPPED_ROOT.format(crf), chunk_folder)
            remove_path(path)
            remove_empty_dirs_in_path(path, [os.path.dirname(os.path.dirname(TMP_SKIPPED_ROOT))])

    def _transfer_failed_tasks(self, event_queue):
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
                            move_with_progress(in_progress_path, failed_path, event_queue)
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

    def _move_outputs_and_mark_done(self, chunk_folder, event_queue):
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
                move_with_progress(chunk_crf_path, final_output_path, event_queue, False, True)
            remove_empty_dirs_in_path(chunk_crf_path, [os.path.dirname(os.path.dirname(TMP_OUTPUT_ROOT))])

        for rel_file in common_files:
            src_file = os.path.join(IN_PROGRESS, rel_file)
            dst_file = os.path.join(DONE_DIR, rel_file)
            if os.path.exists(src_file):
                move_with_progress(src_file, dst_file, event_queue)

        # Move the remaining files, if any
        move_done_if_all_crf_outputs_exist(event_queue)

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

    def _enqueue_worker_sentinels(self):
        try:
            # Push sentinels to unblock any idle workers
            for _ in range(self.max_workers):
                self.task_queue.put_nowait(None)
        except Exception:
            pass

    def _kill_tracked_processes(self):
        # Kill any external encoder processes we’ve tracked
        for key, pid in list(self.process_registry.items()):
            try:
                log(f"Killing encoder process id: {pid}", level="debug")
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                log(f"Process {pid} not found; removing from registry anyway", level="warning")
            except Exception as e:
                log(f"Failed to kill process {pid}: {e}", level="warning")
            finally:
                self.process_registry.pop(key, None)

    def _gracefully_stop_workers(self):
        for p in self.processes:
            if p.is_alive():
                p.terminate()
                p.join(timeout=2)
                if p.is_alive():
                    p.kill()
        self.processes.clear()

    def _join_light_threads(self, is_pause=False):
        for t in self.light_threads:
            if is_pause:
                if t.name == "_start_pause_monitor" or t.name == "_start_cpu_watchdog" or t.name == "_start_online_all_system_cores":
                    continue
                elif t.name == "_start_preloader":
                    continue
            t.join(timeout=0.5)
        if not is_pause:
            self.light_threads.clear()

    def pause(self):
        self._kill_tracked_processes()
        self._join_light_threads(is_pause=True)

    def shutdown(self):
        # Request stop, and make sure workers can unblock from .get()
        self.stop_event.set()
        self._enqueue_worker_sentinels()
        self._kill_tracked_processes()
        self._gracefully_stop_workers()
        self._join_light_threads()
        try:
            self.task_queue.close()
            self.task_queue.join_thread()
        except Exception as e:
            log(f"Failed to close task_queue: {e}", level="warning")
        try:
            self.manager.shutdown()
        except Exception:
            pass

    def get_pause_event(self):
        return self.pause_event
    
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
