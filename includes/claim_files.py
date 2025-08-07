import os

from config import TO_ASSIGN, IN_PROGRESS, TMP_INPUT, CHUNK_SIZE
from helpers.get_all_files_sorted import get_all_files_sorted
from helpers.get_next_chunk_dir import get_next_chunk_dir
from helpers.logging_utils import log

def claim_files():
    all_files = get_all_files_sorted(TO_ASSIGN)
    chunk, size = [], 0
    for full_path, rel_path in all_files:
        file_size = os.path.getsize(full_path)
        log(f"Evaluating file: {rel_path} ({file_size} bytes)", level="debug")
        if size + file_size > CHUNK_SIZE and chunk:
            break
        size += file_size
        chunk.append((full_path, rel_path))

    if not chunk:
        return []
    
    chunk_dir = get_next_chunk_dir(TMP_INPUT)
    # os.makedirs(chunk_dir, exist_ok=True)

    claimed = []

    # print(f"\n{'=' * 29}  START  {'=' * 29}")
    # print(f"Claiming another set of files ...")
    for src, rel in chunk:
        dst = os.path.join(IN_PROGRESS, rel)
        log(f"Added file to claiming: {rel}", level="debug")
        claimed.append((src, dst, os.path.join(chunk_dir, rel)))

    # print(f"{'=' * 30}  END  {'=' * 30}\n")
    return claimed
