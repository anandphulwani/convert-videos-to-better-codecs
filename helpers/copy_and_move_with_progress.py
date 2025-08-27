import os
import shutil
from multiprocessing import RLock

from helpers.remove_path import remove_path
from helpers.remove_empty_dir_upwards import remove_empty_dirs_upwards
from tqdm_manager import get_event_queue, get_random_value_for_id, BAR_TYPE

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

def _copy_file(src_file, dst_file, bar_id, event_queue, copied_bytes_holder):
    """Copy a single file."""
    os.makedirs(os.path.dirname(dst_file), exist_ok=True)
    with open(src_file, 'rb') as fsrc, open(dst_file, 'wb') as fdst:
        while True:
            buf = fsrc.read(1024 * 1024)
            if not buf:
                break
            fdst.write(buf)
            copied_bytes_holder[0] += len(buf)
            event_queue.put({
                "op": "update",
                "bar_id": bar_id,
                "current": copied_bytes_holder[0]
            })
    shutil.copystat(src_file, dst_file)

def copy_with_progress(src, dst, desc="Copying"):
    """
    Copy a file or directory from src to dst with progress reporting.

    Shows a tqdm progress bar for total bytes copied.
    """
    with _progress_lock:
        total_size = _calculate_total_size(src)
        copied_bytes_holder = [0]
        event_queue=get_event_queue()
        bar_id = get_random_value_for_id()
        is_copying_anything = False

        if os.path.isfile(src):
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            if not is_copying_anything:
                event_queue.put({
                    "op": "create",
                    "bar_type": BAR_TYPE.OTHER,
                    "bar_id": bar_id,
                    "total": total_size,
                    "metadata": {"name": f"{desc}: ", "unit": "B", "unit_scale": True, "unit_divisor": 1024}
                })

            _copy_file(src, dst, bar_id, event_queue, copied_bytes_holder)
            is_copying_anything = True
        else:
            os.makedirs(dst, exist_ok=True)
            for dirpath, _, files in os.walk(src):
                rel_path = os.path.relpath(dirpath, src)
                target_dir = os.path.join(dst, rel_path)
                os.makedirs(target_dir, exist_ok=True)

                for file in files:
                    if not is_copying_anything:
                        event_queue.put({
                            "op": "create",
                            "bar_type": BAR_TYPE.OTHER,
                            "bar_id": bar_id,
                            "total": total_size,
                            "metadata": {"name": f"{desc}: ", "unit": "B", "unit_scale": True, "unit_divisor": 1024}
                        })

                    src_file = os.path.join(dirpath, file)
                    dst_file = os.path.join(target_dir, file)
                    _copy_file(src_file, dst_file, bar_id, event_queue, copied_bytes_holder)
                    is_copying_anything = True

            shutil.copystat(src, dst)
        if is_copying_anything:
            event_queue.put({"op": "finish", "bar_id": bar_id})

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
