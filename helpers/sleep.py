import time
from tqdm import tqdm

def sleep(sleep_time):
    for _ in tqdm(range(sleep_time), desc="Sleeping", unit="s"):
        time.sleep(1)
