import subprocess
import os
import platform
import subprocess
import threading
import time
import math

from includes.ffmpeg import ffmpeg_get_duration, ffmpeg_av1_crf_cmd_generator
from helpers.format_elapsed import format_elapsed
from helpers.remove_topmost_dir import remove_topmost_dir
from config import TMP_OUTPUT_ROOT, FINAL_OUTPUT_ROOT, TMP_PROCESSING
from helpers.logging_utils import log
from tqdm_manager import BAR_TYPE

def encode_file(
    src_file,
    rel_path,
    crf,
    slot_idx,
    bytes_encoded,
    event_queue,
    process_registry=None,
    chunk_progress=None,
    chunk_key=None,
    pause_event=None
):
    tmp_processing_dir = TMP_PROCESSING.format(crf)
    tmp_output_dir = TMP_OUTPUT_ROOT.format(crf)
    final_output_dir = FINAL_OUTPUT_ROOT.format(crf)
    tmp_processing_file = os.path.join(tmp_processing_dir, rel_path)
    tmp_output_file = os.path.join(tmp_output_dir, rel_path)
    final_output_file = os.path.join(final_output_dir, remove_topmost_dir(rel_path))

    if os.path.exists(final_output_file):
        return [src_file, crf, "skipped-alreadyexists-main", f"{rel_path} (already exists in the main output): {final_output_file}"]

    if os.path.exists(tmp_output_file):
        return [src_file, crf, "skipped-alreadyexists-tmp", f"{rel_path} (already exists in the temp output): {tmp_output_file}"]

    # os.makedirs(os.path.dirname(out_file), exist_ok=True)
    cmd = ffmpeg_av1_crf_cmd_generator(src_file, tmp_processing_file, crf)
    duration = ffmpeg_get_duration(src_file)

    if duration is None:
        log(f"Duration not found for {rel_path}, skipping file.", level="warning")
        return [src_file, crf, "skipped-notsupported", f"{rel_path} (duration not found)"]

    file_size = os.path.getsize(src_file)
    start_time = time.time()

    log(f"Encoding {rel_path} [CRF {crf}]", level="debug")

    os.makedirs(os.path.dirname(tmp_processing_file), exist_ok=True)

    # Create UI bar in main process via event
    if event_queue is not None:
        event_queue.put({
            "op": "create",
            "bar_type": BAR_TYPE.FILE,
            "bar_id": f"file_slot_{slot_idx:02}",
            "total": file_size,
            "metadata": {"filename": os.path.basename(src_file), "slot_no": f"{slot_idx:02}", "crf": crf}
        })

    if platform.system() == "Windows":
        # Create new process group on Windows
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )
    else:
        # Create new process group on Unix/Linux
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )

    if process_registry is not None:
        log(f"Adding to process_registry: {process.pid}", level="debug")
        process_registry[os.getpid()] = process.pid

    last_progress = 0
    error_while_decoding = False
    stdout_output = []
    stderr_output = []
    lock = threading.Lock()  # to protect shared vars above

    def stdout_reader(proc):
        nonlocal last_progress
        for line in proc.stdout:
            line = line.strip()

            with lock:
                stdout_output.append(line)

                # progress lines
                if line.startswith('out_time_ms='):
                    val = line.split('=', 1)[1].strip()
                    try:
                        out_ms = int(val)  # may be N/A early on
                    except ValueError:
                        continue  # skip this line until it's numeric

                    percent = out_ms / duration
                    current_progress = math.floor(file_size * percent)


                    delta_bytes = current_progress - last_progress
                    if delta_bytes > 0:
                        bytes_encoded.value += delta_bytes
                        if chunk_progress is not None and chunk_key is not None:
                            chunk_progress[chunk_key].value += delta_bytes
                            if event_queue is not None:
                                event_queue.put({
                                    "op": "update",
                                    "bar_id": f"file_slot_{slot_idx:02}",
                                    "current": current_progress
                                })
                        last_progress = current_progress

                elif line.startswith('progress=end'):
                    delta_bytes = file_size - last_progress
                    if delta_bytes > 0:
                        bytes_encoded.value += delta_bytes
                        if chunk_progress is not None and chunk_key is not None:
                            # per-chunk live byte progress
                            chunk_progress[chunk_key].value += delta_bytes
                        last_progress = file_size

                    if event_queue is not None:
                        event_queue.put({
                            "op": "update",
                            "bar_id": f"file_slot_{slot_idx:02}",
                            "current": file_size
                        })
                        event_queue.put({
                            "op": "finish",
                            "bar_id": f"file_slot_{slot_idx:02}"
                        })

    def stderr_reader(proc):
        nonlocal error_while_decoding
        for line in proc.stderr:
            line = line.rstrip("\n")

            with lock:
                stderr_output.append(line)
                if "error while decoding" in line.lower():
                    error_while_decoding = True

    # 2) start reader threads
    t_out = threading.Thread(target=stdout_reader, args=(process,))
    t_err = threading.Thread(target=stderr_reader, args=(process,))

    t_out.start()
    t_err.start()

    # 3) wait for ffmpeg to finish
    process.wait()

    # 4) ensure readers are done
    t_out.join()
    t_err.join()

    # remove from registry
    if process_registry is not None:
        process_registry.pop(os.getpid(), None)

    # 5) check result
    failed = (
        process.returncode != 0
        or not os.path.exists(tmp_processing_file)
        or error_while_decoding
    )

    if failed:
        if pause_event is None or not pause_event.is_set():
            log(f"{'=' * 29}  START  {'=' * 29}", level="error", log_to=["file"])
            log(f"FFmpeg failed for {rel_path} [CRF {crf}]", level="error", log_to=["file"])
            log("\n".join(stdout_output), level="error", log_to=["file"])
            log("-" * 60, level="error", log_to=["file"])
            log("\n".join(stderr_output), level="error", log_to=["file"])
            log(f"{'=' * 30}  END  {'=' * 30}", level="error", log_to=["file"])

            # print partial stderr to console
            log(f"{'=' * 29}  START  {'=' * 29}", log_to=["console"])
            log(f"FFmpeg error for {rel_path} [CRF {crf}]: error log under:", log_to=["console"])
            for line in stderr_output[-10:]:
                log(f"          {line}", log_to=["console"])
            if len(stderr_output) > 10:
                log("... (truncated)", log_to=["console"])
            log(f"{'=' * 30}  END  {'=' * 30}", log_to=["console"])
            
            # Close the UI bar if it exists
            if event_queue is not None:
                event_queue.put({"op": "finish", "bar_id": f"file_slot_{slot_idx:02}"})

            return [src_file, crf, "failed", f"FFmpeg failed for {rel_path} (see log)"]
        else:
            return [src_file, crf, "failed-paused", f"Main thread for {rel_path} is paused"]

    elapsed = time.time() - start_time
    
    return [src_file, crf, "success", f"{os.path.basename(src_file)} in {format_elapsed(elapsed)}"]
