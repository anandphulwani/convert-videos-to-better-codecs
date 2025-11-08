import os
from helpers.logging_utils import log
from tqdm_manager import get_random_value_for_id, BAR_TYPE

def get_all_files_sorted(base_dir, event_queue, limit_size=0):
    log(f"Scanning directory: {base_dir}", level="debug")
    all_files = []

    bar_id = get_random_value_for_id()
    running_size = 0
    limit_reached = False
    index = 1
    for root, dirs, files in os.walk(base_dir):
        if limit_reached:
            break

        dirs.sort()
        files.sort()

        for file in files:
            if file.lower().endswith('.mp4'):
                if index == 1:
                    event_queue.put({
                        "op": "create",
                        "bar_type": BAR_TYPE.OTHER,
                        "bar_id": bar_id,
                        "total": None,
                        "metadata": {"name": f"Scanning for .mp4 files"}
                    })
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, base_dir)
                file_size = os.path.getsize(full_path)
                log(f"Evaluating file: {rel_path} ({file_size} bytes)", level="debug")

                # If we have a limit and adding this would exceed it, stop early (but only if we already have at least one)
                if limit_size > 0 and all_files and running_size + file_size > limit_size:
                    log(f"Found {len(all_files)} .mp4 files (early stop at ~{running_size} bytes)", level="debug")
                    limit_reached = True
                    break

                running_size += file_size

                all_files.append((full_path, rel_path))
                event_queue.put({
                    "op": "update",
                    "bar_id": bar_id,
                    "current": index
                })
                index += 1
    if index > 1:
        event_queue.put({"op": "finish", "bar_id": bar_id})
    log(f"Found {len(all_files)} .mp4 files", level="info")
    return sorted(all_files, key=lambda x: x[1])
