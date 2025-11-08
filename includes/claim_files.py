import os

from config import TO_ASSIGN, IN_PROGRESS, TMP_INPUT, CHUNK_SIZE
from helpers.get_all_files_sorted import get_all_files_sorted
from helpers.get_next_chunk_dir import get_next_chunk_dir
from helpers.logging_utils import log

def claim_files(event_queue):
    chunk = get_all_files_sorted(TO_ASSIGN, event_queue, CHUNK_SIZE)
    if not chunk:
        return []
    
    chunk_dir = get_next_chunk_dir(TMP_INPUT)
    claimed = []
    for src, rel in chunk:
        dst = os.path.join(IN_PROGRESS, rel)
        log(f"Added file to claiming: {rel}", level="debug")
        claimed.append((src, dst, os.path.join(chunk_dir, rel)))

    return claimed
