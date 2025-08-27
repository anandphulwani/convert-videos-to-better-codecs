import os
from helpers.logging_utils import log
from tqdm_manager import get_event_queue, get_random_value_for_id, BAR_TYPE

def get_all_files_sorted(base_dir):
    log(f"Scanning directory: {base_dir}", level="debug")
    all_files = []

    event_queue=get_event_queue()
    bar_id = get_random_value_for_id()
    index = 1
    for root, _, files in os.walk(base_dir):
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
                all_files.append((full_path, rel_path))
                event_queue.put({
                    "op": "update",
                    "bar_id": bar_id,
                    "current": index
                })
                index += 1
    if index > 1:
        event_queue.put({"op": "finish", "bar_id": bar_id})
    log(f"Found {len(all_files)} .mp4 files", level="debug")
    return sorted(all_files, key=lambda x: x[1])
