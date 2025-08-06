import os
from tqdm import tqdm

from helpers.logging_utils import log

def get_all_files_sorted(base_dir):
    log(f"Scanning directory: {base_dir}", level="debug")
    all_files = []

    with tqdm(desc="Scanning for .mp4 files", unit=" file", leave=False) as pbar:
        for root, _, files in os.walk(base_dir):
            for file in files:
                if file.lower().endswith('.mp4'):
                    full_path = os.path.join(root, file)
                    rel_path = os.path.relpath(full_path, base_dir)
                    all_files.append((full_path, rel_path))
                    pbar.update(1)

    log(f"Found {len(all_files)} .mp4 files", level="debug")
    return sorted(all_files, key=lambda x: x[1])
