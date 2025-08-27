import os
from helpers.remove_path import remove_path
from tqdm_manager import get_event_queue, get_random_value_for_id, BAR_TYPE

def remove_empty_dirs(dirs):
    removed_count = 0
    event_queue=get_event_queue()
    bar_id = get_random_value_for_id()
    event_queue.put({
        "op": "create",
        "bar_type": BAR_TYPE.OTHER,
        "bar_id": bar_id,
        "total": None,
        "metadata": {"name": f"Removing empty directories"}
    })
    for index, dirpath in enumerate(dirs, start=1):
        if os.path.isdir(dirpath) and not os.listdir(dirpath):
            event_queue.put({
                "op": "update",
                "bar_id": bar_id,
                "current": index
            })
            remove_path(dirpath)
            removed_count += 1
    event_queue.put({"op": "finish", "bar_id": bar_id})
    return removed_count
