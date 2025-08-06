import time 

def update_size_pbar(pbar, shared_val, total_bytes, stop_event):
    while not stop_event.is_set():
        current = shared_val.value
        delta = current - pbar.n
        if delta > 0:
            pbar.update(delta)
        if current >= total_bytes:
            break
        time.sleep(0.5)
