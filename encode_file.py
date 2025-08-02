import subprocess
import os
import time
import logging
from tqdm import tqdm

from helpers.copy_and_move_with_progress import move_with_progress
from ffmpeg import get_duration, ffmpeg_cmd_av1_crf
from helpers.format_elapsed import format_elapsed
from config import TMP_OUTPUT_ROOT, FINAL_OUTPUT_ROOT

def encode_file(src_file, rel_path, crf, bytes_encoded):
    output_dir = TMP_OUTPUT_ROOT.format(crf)
    target_dir = FINAL_OUTPUT_ROOT.format(crf)
    out_file = os.path.join(output_dir, rel_path)
    final_dst = os.path.join(target_dir, rel_path)

    if os.path.exists(final_dst):
        return [src_file, crf, "skipped", f"{rel_path} (already exists)"]

    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    cmd = ffmpeg_cmd_av1_crf(src_file, out_file, crf)
    duration = get_duration(src_file)

    if duration is None:
        logging.warning(f"Duration not found for {rel_path}, skipping file.")
        return [src_file, crf, "skipped", f"{rel_path} (duration not found)"]

    file_size = os.path.getsize(src_file)
    start_time = time.time()

    logging.debug(f"Encoding {rel_path} [CRF {crf}]")
    pbar = tqdm(total=duration or 100, desc=f"CRF{crf}: {os.path.basename(src_file)}", unit='s', leave=False)

    process = subprocess.Popen(cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            text=True,
                            bufsize=1)

    # parse progress from stdout:
    last_progress = 0
    for line in process.stdout:
        if line.startswith('out_time_ms='):
            out_ms = int(line.split('=',1)[1].strip())
            seconds = out_ms / 1_000_000
            delta = seconds - pbar.n
            if delta > 0:
                if duration:
                    percent = seconds / duration
                    current_progress = int(file_size * percent)
                    delta_bytes = current_progress - last_progress
                    if delta_bytes > 0:
                        bytes_encoded.value += delta_bytes
                        last_progress = current_progress
                pbar.update(int(round(delta)))

    stdout, stderr = process.communicate()

    if process.returncode != 0 or not os.path.exists(out_file):
        logging.error(f"{'=' * 29}  START  {'=' * 29}")
        logging.error(f"FFmpeg failed for {rel_path} [CRF {crf}]")
        logging.error(stdout)
        logging.error("-" * 60)
        logging.error(stderr)
        logging.error(f"{'=' * 30}  END  {'=' * 30}")

        # Print partial stderr to console
        stderr_lines = stderr.strip().splitlines()
        snippet = stderr_lines[:10]  # Show first 10 lines
        print(f"\n{'=' * 29}  START  {'=' * 29}")
        print(f"FFmpeg error for {rel_path} [CRF {crf}]")
        print("-" * 60)
        for line in snippet:
            print(line)
        if len(stderr_lines) > 10:
            print("... (truncated)")
        print(f"{'=' * 30}  END  {'=' * 30}\n")
        
        return [src_file, crf, "failed", f"FFmpeg failed for {rel_path} (see log)"]

    move_with_progress(out_file, final_dst, desc=f"Moving {os.path.basename(out_file)}")
    elapsed = time.time() - start_time
    return [src_file, crf, "success", f"{os.path.basename(src_file)} in {format_elapsed(elapsed)}"]
