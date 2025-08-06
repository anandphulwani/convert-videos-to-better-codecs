import threading
from queue import Queue

# Additional thread-safe constructs for preloading
# preload_lock = threading.Lock()
# task_queue = Queue()
# next_chunk_ready = threading.Event()

pause_flag = threading.Event()
