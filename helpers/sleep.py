import time
from tqdm_manager import get_event_queue, get_random_value_for_id, BAR_TYPE

def sleep(sleep_time):
    event_queue=get_event_queue()
    bar_id = get_random_value_for_id()
    event_queue.put({
        "op": "create",
        "bar_type": BAR_TYPE.OTHER,
        "bar_id": bar_id,
        "total": sleep_time,
        "metadata": {"name": f"Sleeping {sleep_time}s: "}
    })
    for index in range(sleep_time):
        time.sleep(1)
        event_queue.put({
            "op": "update",
            "bar_id": bar_id,
            "current": index
        })
    event_queue.put({"op": "finish", "bar_id": bar_id})