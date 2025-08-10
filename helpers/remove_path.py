import os
import shutil

from helpers.logging_utils import log

def remove_path(path):
    """Remove a file or directory at the given path."""
    if path and os.path.exists(path):
        try:
            if os.path.isfile(path):
                os.remove(path)
            else:
                shutil.rmtree(path)
        except Exception as e:
            log(f"Failed to delete source file/dir {path}: {e}", level="error")
            return False
    return True
