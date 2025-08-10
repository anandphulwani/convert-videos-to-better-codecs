import os
from tqdm import tqdm

from helpers.remove_path import remove_path

def remove_empty_dirs(dirs):
    removed_count = 0
    for dirpath in tqdm(dirs, desc="Removing empty directories"):
        if os.path.isdir(dirpath) and not os.listdir(dirpath):
            remove_path(dirpath)
            removed_count += 1
    return removed_count
