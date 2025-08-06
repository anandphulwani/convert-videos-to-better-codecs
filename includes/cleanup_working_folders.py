import os

from helpers.logging_utils import log
from helpers.get_all_files_sorted import get_all_files_sorted
from helpers.find_all_dirs import find_all_dirs
from helpers.remove_empty_dirs import remove_empty_dirs
from config import TMP_OUTPUT_ROOT, TO_ASSIGN, IN_PROGRESS, DONE_DIR, FAILED_DIR, LOGS_DIR, TMP_INPUT

def cleanup_working_folders():
    log("Cleaning up WORKING folders...")
    for src, rel in get_all_files_sorted(TMP_INPUT):
        try:
            os.remove(src)
            log(f"Removed processed file from TMP_INPUT: {rel}", level="debug")
        except Exception as e:
            log(f"Failed to delete {rel} from TMP_INPUT: {e}", level="error")

    # Remove empty directories with progress bar
    all_main_dirs = [TO_ASSIGN, IN_PROGRESS, os.path.dirname(IN_PROGRESS), 
                     DONE_DIR, FAILED_DIR, LOGS_DIR, os.path.dirname(LOGS_DIR), 
                     TMP_INPUT, os.path.dirname(TMP_OUTPUT_ROOT)]
    all_dirs = find_all_dirs(all_main_dirs)
    removed = remove_empty_dirs(all_dirs)
    print(f"Removed {removed} empty directories.")
