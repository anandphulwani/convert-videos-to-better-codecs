import os

from helpers.logging_utils import log
from helpers.get_all_files_sorted import get_all_files_sorted
from helpers.find_all_dirs import find_all_dirs
from helpers.remove_empty_dirs import remove_empty_dirs
from helpers.remove_path import remove_path
from helpers.remove_files_of_ext import remove_files_of_ext
from config import (
    TO_ASSIGN, IN_PROGRESS, DONE_DIR, FAILED_DIR, LOGS_DIR, 
    TMP_INPUT, TMP_PROCESSING, TMP_OUTPUT_ROOT, TMP_FAILED_ROOT, TMP_SKIPPED_ROOT
)

def cleanup_working_folders():
    log("Cleaning up WORKING folders...")
    for src, rel in get_all_files_sorted(os.path.dirname(TMP_PROCESSING)):
        remove_path(src)

    remove_files_of_ext(TMP_INPUT, "done")

    # Remove empty directories with progress bar
    all_main_dirs = [TO_ASSIGN, IN_PROGRESS, DONE_DIR, FAILED_DIR, LOGS_DIR, TMP_INPUT, 
                     os.path.dirname(TMP_PROCESSING), os.path.dirname(TMP_OUTPUT_ROOT),
                     os.path.dirname(TMP_FAILED_ROOT), os.path.dirname(TMP_SKIPPED_ROOT)]
    all_dirs = find_all_dirs(all_main_dirs)
    removed = remove_empty_dirs(all_dirs)
    log(f"Removed {removed} empty directories.")
