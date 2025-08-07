import os
import shutil
import threading
from tqdm import tqdm

_progress_lock = threading.RLock()

def copy_with_progress(src, dst, desc="Copying"):
    with _progress_lock:
        # Step 1: Compute total size
        if os.path.isfile(src):
            total_size = os.path.getsize(src)
        else:
            total_size = sum(
                os.path.getsize(os.path.join(dirpath, f))
                for dirpath, _, files in os.walk(src)
                for f in files
            )

        # Step 2: Create target directory if needed
        if os.path.isfile(src):
            os.makedirs(os.path.dirname(dst), exist_ok=True)
        else:
            os.makedirs(dst, exist_ok=True)

        # Step 3: Start copy with progress
        with tqdm(total=total_size, unit='B', unit_scale=True, desc=desc, leave=False) as pbar:
            if os.path.isfile(src):
                with open(src, 'rb') as fsrc, open(dst, 'wb') as fdst:
                    while True:
                        buf = fsrc.read(1024 * 1024)
                        if not buf:
                            break
                        fdst.write(buf)
                        pbar.update(len(buf))
                shutil.copystat(src, dst)
            else:
                for dirpath, _, files in os.walk(src):
                    rel_path = os.path.relpath(dirpath, src)
                    target_dir = os.path.join(dst, rel_path)
                    os.makedirs(target_dir, exist_ok=True)
                    for file in files:
                        src_file = os.path.join(dirpath, file)
                        dst_file = os.path.join(target_dir, file)
                        with open(src_file, 'rb') as fsrc, open(dst_file, 'wb') as fdst:
                            while True:
                                buf = fsrc.read(1024 * 1024)
                                if not buf:
                                    break
                                fdst.write(buf)
                                pbar.update(len(buf))
                        shutil.copystat(src_file, dst_file)
                shutil.copystat(src, dst)  # Copy metadata of the root folder

def move_with_progress(src, dst, remove_empty_source=True, move_contents_not_dir_itself=False, desc="Moving"):
    with _progress_lock:
        if move_contents_not_dir_itself and os.path.isdir(src):
            os.makedirs(dst, exist_ok=True)
            for item in os.listdir(src):
                item_src = os.path.join(src, item)
                item_dst = os.path.join(dst, item)
                copy_with_progress(item_src, item_dst, desc=f"{desc}: {item}")
                if os.path.isfile(item_src):
                    os.remove(item_src)
                else:
                    shutil.rmtree(item_src)
            if remove_empty_source:
                while os.path.isdir(src) and not os.listdir(src):
                    os.rmdir(src)
                    src = os.path.dirname(src)
        else:
            copy_with_progress(src, dst, desc)
            if os.path.isfile(src):
                os.remove(src)
            else:
                shutil.rmtree(src)
            if remove_empty_source:
                src_dir = os.path.dirname(src)
                while os.path.isdir(src_dir) and not os.listdir(src_dir):
                    os.rmdir(src_dir)
                    src_dir = os.path.dirname(src_dir)
