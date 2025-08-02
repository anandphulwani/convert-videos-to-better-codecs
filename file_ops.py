import os
import logging
import shutil
from tqdm import tqdm

from config import TO_ASSIGN, IN_PROGRESS, CHUNK_SIZE
from file_ops import get_all_files_sorted
from helpers.copy_and_move_with_progress import move_with_progress
from helpers.find_all_dirs import find_all_dirs
from helpers.remove_empty_dirs import remove_empty_dirs

from config import (
    LOCKS_DIR, TO_ASSIGN, IN_PROGRESS, DONE_DIR, FAILED_DIR, 
    LOGS_DIR, TMP_PROCESSING, TMP_OUTPUT_ROOT
    , FINAL_OUTPUT_ROOT, CRF_VALUES
)

def ensure_dirs():
    dirs = [LOCKS_DIR, TO_ASSIGN, IN_PROGRESS, DONE_DIR, FAILED_DIR, LOGS_DIR, TMP_PROCESSING]
    for d in dirs:
        os.makedirs(d, exist_ok=True)
        logging.debug(f"Ensured directory exists: {d}")
    for crf in CRF_VALUES:
        tmp_out = TMP_OUTPUT_ROOT.format(crf)
        final_out = FINAL_OUTPUT_ROOT.format(crf)
        # Clear tmp output dir
        if os.path.exists(tmp_out):
            shutil.rmtree(tmp_out)
            logging.info(f"Cleared temp output directory: {tmp_out}")
        os.makedirs(tmp_out, exist_ok=True)
        os.makedirs(final_out, exist_ok=True)
        logging.debug(f"Ensured CRF directories: {tmp_out}, {final_out}")


def get_all_files_sorted(base_dir):
    logging.debug(f"Scanning directory: {base_dir}")
    all_files = []

    with tqdm(desc="Scanning for .mp4 files", unit=" file", leave=False) as pbar:
        for root, _, files in os.walk(base_dir):
            for file in files:
                if file.lower().endswith('.mp4'):
                    full_path = os.path.join(root, file)
                    rel_path = os.path.relpath(full_path, base_dir)
                    all_files.append((full_path, rel_path))
                    pbar.update(1)

    logging.debug(f"Found {len(all_files)} .mp4 files")
    return sorted(all_files, key=lambda x: x[1])

def cleanup_working_folders():
    from config import TO_ASSIGN, IN_PROGRESS, DONE_DIR, FAILED_DIR, LOGS_DIR, TMP_PROCESSING
    logging.info("Cleaning up WORKING folders...")
    for src, rel in get_all_files_sorted(TMP_PROCESSING):
        try:
            os.remove(src)
            logging.debug(f"Removed processed file from TMP_PROCESSING: {rel}")
        except Exception as e:
            logging.error(f"Failed to delete {rel} from TMP_PROCESSING: {e}")

    # Remove empty directories with progress bar
    all_main_dirs = [TO_ASSIGN, IN_PROGRESS, os.path.dirname(IN_PROGRESS),
                     DONE_DIR, FAILED_DIR, LOGS_DIR, os.path.dirname(LOGS_DIR),
                     TMP_PROCESSING, os.path.dirname(TMP_OUTPUT_ROOT)]
    all_dirs = find_all_dirs(all_main_dirs)
    removed = remove_empty_dirs(all_dirs)
    print(f"Removed {removed} empty directories.")

def claim_files():
    all_files = get_all_files_sorted(TO_ASSIGN)
    chunk, size = [], 0
    for full_path, rel_path in all_files:
        file_size = os.path.getsize(full_path)
        if size + file_size > CHUNK_SIZE and chunk:
            break
        size += file_size
        chunk.append((full_path, rel_path))

    claimed = []
    for src, rel in chunk:
        dst = os.path.join(IN_PROGRESS, rel)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        move_with_progress(src, dst, desc=f"Moving {os.path.basename(src)}")
        logging.debug(f"Moved file to in_progress: {rel}")
        claimed.append((dst, rel))  # return path now in IN_PROGRESS

    return claimed
