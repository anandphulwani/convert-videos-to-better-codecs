import os
import shutil
from tqdm import tqdm

def copy_with_progress(src, dst, desc="Copying"):
    total_size = os.path.getsize(src)
    os.makedirs(os.path.dirname(dst), exist_ok=True)

    with open(src, 'rb') as fsrc, open(dst, 'wb') as fdst:
        with tqdm(total=total_size, unit='B', unit_scale=True, desc=desc, leave=False) as pbar:
            while True:
                buf = fsrc.read(1024 * 1024)  # 1 MB
                if not buf:
                    break
                fdst.write(buf)
                pbar.update(len(buf))

    shutil.copystat(src, dst)  # Preserve metadata like modification time

def move_with_progress(src, dst, desc="Moving"):
    copy_with_progress(src, dst, desc)
    os.remove(src)
