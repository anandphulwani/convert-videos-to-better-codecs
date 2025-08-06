import os
from tqdm import tqdm

def remove_empty_dirs(dirs):
    removed_count = 0
    for dirpath in tqdm(dirs, desc="Removing empty directories"):
        if os.path.isdir(dirpath) and not os.listdir(dirpath):
            try:
                os.rmdir(dirpath)
                removed_count += 1
            except Exception as e:
                tqdm.write(f"Could not remove {dirpath}: {e}")
    return removed_count
