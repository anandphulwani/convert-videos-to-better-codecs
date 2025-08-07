import os

from config import ALL_ROOTS
from helpers.logging_utils import log

def remove_empty_dirs_in_path(path, roots=ALL_ROOTS):
    # Expand dynamic root paths
    base = None
    for each_base in sorted(roots, key=len, reverse=True):  # longest match first
        if os.path.commonpath([path, each_base]) == each_base:
            base = each_base
    
    if not base:
        log("Provided path does not belong to any known base path.")
        return

    while os.path.abspath(path) != os.path.abspath(base):
        if not os.path.isdir(path):
            path = os.path.dirname(path)
            continue
        try:
            if not os.listdir(path):
                # log(f"00. Removing empty directory: {path}")
                os.rmdir(path)
            else:
                break  # Stop if directory is not empty
        except Exception as e:
            log(f"Error while checking/removing {path}: {e}")
            break
        path = os.path.dirname(path)
