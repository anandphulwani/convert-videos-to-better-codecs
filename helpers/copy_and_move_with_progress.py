import os
import shutil
from tqdm import tqdm

from multiprocessing import RLock
from helpers.remove_path import remove_path
from helpers.remove_empty_dir_upwards import remove_empty_dirs_upwards

_progress_lock = RLock()

def _calculate_total_size(path):
    """Return the total size of a file or all files within a directory."""
    if os.path.isfile(path):
        return os.path.getsize(path)
    return sum(
        os.path.getsize(os.path.join(dirpath, f))
        for dirpath, _, files in os.walk(path)
        for f in files
    )

def _copy_file_with_progress(src_file, dst_file, pbar):
    """Copy a single file with progress updates."""
    with open(src_file, 'rb') as fsrc, open(dst_file, 'wb') as fdst:
        while True:
            buf = fsrc.read(1024 * 1024)
            if not buf:
                break
            fdst.write(buf)
            pbar.update(len(buf))
    shutil.copystat(src_file, dst_file)

def copy_with_progress(src, dst, desc="Copying"):
    """
    Copy a file or directory from src to dst with progress reporting.

    Shows a tqdm progress bar for total bytes copied.
    """
    with _progress_lock:
        total_size = _calculate_total_size(src)

        # Ensure destination directory exists
        if os.path.isfile(src):
            os.makedirs(os.path.dirname(dst), exist_ok=True)
        else:
            os.makedirs(dst, exist_ok=True)

        with tqdm(total=total_size, unit='B', unit_scale=True, desc=desc, leave=False, position=0) as pbar:
            if os.path.isfile(src):
                _copy_file_with_progress(src, dst, pbar)
            else:
                for dirpath, _, files in os.walk(src):
                    rel_path = os.path.relpath(dirpath, src)
                    target_dir = os.path.join(dst, rel_path)
                    os.makedirs(target_dir, exist_ok=True)

                    for file in files:
                        src_file = os.path.join(dirpath, file)
                        dst_file = os.path.join(target_dir, file)
                        _copy_file_with_progress(src_file, dst_file, pbar)

                shutil.copystat(src, dst)  # Copy metadata of the root folder

def move_with_progress(src, dst, remove_empty_source=False, move_contents_not_dir_itself=False, desc="Moving"):
    """
    Move a file or directory from src to dst with progress reporting.

    If move_contents_not_dir_itself is True and src is a directory, only the contents of the directory
    are moved into dst, not the src directory itself.

    If remove_empty_source is True, any empty parent directories left behind are removed after the move.
    """
    # Normalize the flag to avoid incorrect use if src is not a directory
    if not (move_contents_not_dir_itself and os.path.isdir(src)):
        move_contents_not_dir_itself = False

    with _progress_lock:
        # Move the contents of src directory into dst if flag is set
        if move_contents_not_dir_itself:
            os.makedirs(dst, exist_ok=True)
            for item in os.listdir(src):
                item_src = os.path.join(src, item)
                item_dst = os.path.join(dst, item)

                copy_with_progress(item_src, item_dst, desc=f"{desc}: {item}")
                remove_path(item_src)
        else:
            # Move the entire src (file or directory) into dst
            copy_with_progress(src, dst, desc)
            remove_path(src)

        # Optionally remove any empty directories left behind
        if remove_empty_source:
            if move_contents_not_dir_itself:
                remove_empty_dirs_upwards(src)
            else:
                remove_empty_dirs_upwards(os.path.dirname(src))