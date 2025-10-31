import subprocess
import os
import platform
import subprocess
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
    for line in process.stdout:
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
                    # per-chunk live byte progress
                    chunk_progress[chunk_key].value += delta_bytes
                    if event_queue is not None:
                        # Send ABSOLUTE current value; manager computes delta
                        event_queue.put({
                            "op": "update",
                            "bar_id": f"file_slot_{slot_idx:02}",
                            "current": current_progress
                        })
                last_progress = current_progress

        if line.startswith('progress=end'): 
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
            if event_queue is not None:
                event_queue.put({"op": "finish", "bar_id": f"file_slot_{slot_idx:02}"})

    stdout, stderr = process.communicate()

    if process.returncode != 0 or not os.path.exists(tmp_processing_file):
        if pause_event is None or not pause_event.is_set():
            log(f"{'=' * 29}  START  {'=' * 29}", level="error", log_to=["file"])
            log(f"FFmpeg failed for {rel_path} [CRF {crf}]", level="error", log_to=["file"])
            log(stdout, level="error", log_to=["file"])
            log("-" * 60, level="error", log_to=["file"])
            log(stderr, level="error", log_to=["file"])
            log(f"{'=' * 30}  END  {'=' * 30}", level="error", log_to=["file"])

            # Print partial stderr to console
            stderr_lines = stderr.strip().splitlines()
            snippet = stderr_lines[-10:]  # Show first 10 lines
            print(f"\n{'=' * 29}  START  {'=' * 29}")
            print(f"FFmpeg error for {rel_path} [CRF {crf}]")
            print("-" * 60)
            for line in snippet:
                print(line)
            if len(stderr_lines) > 10:
                print("... (truncated)")
            print(f"{'=' * 30}  END  {'=' * 30}\n")
            
            if process_registry is not None:
                process_registry.pop(os.getpid(), None)

            # Close the UI bar if it exists
            if event_queue is not None:
                event_queue.put({"op": "finish", "bar_id": f"file_slot_{slot_idx:02}"})

            return [src_file, crf, "failed", f"FFmpeg failed for {rel_path} (see log)"]
        else:
            return [src_file, crf, "failed-paused", f"Main thread for {rel_path} is paused"]

    elapsed = time.time() - start_time

    if process_registry is not None:
        process_registry.pop(os.getpid(), None)
    
    return [src_file, crf, "success", f"{os.path.basename(src_file)} in {format_elapsed(elapsed)}"]
