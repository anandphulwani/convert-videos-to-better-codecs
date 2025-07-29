import os
import shutil
import subprocess
import threading
from multiprocessing import Manager
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import re
import time

# Config
source_dir = r'F:\ForTesting'
target_base = r'F:\ForTesting_Out'
tmp_input = r'F:\tmp_input'
CHUNK_SIZE = 1 * 1024 * 1024 * 1024  # 1 GB per chunk
CRF_VALUES = [24, 60]
MAX_WORKERS = 8

# Utilities
def get_duration(file_path):
    result = subprocess.run([
        'ffprobe', '-v', 'error',
        '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        file_path
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        return int(float(result.stdout.strip()))
    except:
        return None

def format_elapsed(seconds):
    return time.strftime("%H:%M:%S", time.gmtime(seconds))

def ffmpeg_with_progress(cmd, total_duration, file_label):
    pbar = tqdm(total=total_duration, unit='s', desc=f"⏳ {file_label}", leave=False)
    process = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.DEVNULL, text=True)
    time_pattern = re.compile(r'time=(\d+):(\d+):(\d+).(\d+)')
    for line in process.stderr:
        match = time_pattern.search(line)
        if match:
            h, m, s, ms = map(int, match.groups())
            seconds = h * 3600 + m * 60 + s
            pbar.n = seconds
            pbar.refresh()
    process.wait()
    pbar.n = total_duration
    pbar.refresh()
    pbar.close()

def get_all_files(base_dir):
    all_files = []
    for root, _, files in os.walk(base_dir):
        for file in files:
            if file.lower().endswith('.mp4'):
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, base_dir)
                all_files.append((full_path, rel_path))
    return sorted(all_files, key=lambda x: x[1])

def prepare_chunks(files):
    chunks = []
    chunk = []
    size = 0
    for full_path, rel_path in files:
        file_size = os.path.getsize(full_path)
        if size + file_size > CHUNK_SIZE and chunk:
            chunks.append(chunk)
            chunk = []
            size = 0
        size += file_size
        chunk.append((full_path, rel_path))
    if chunk:
        chunks.append(chunk)
    return chunks

def clear_tmp_all():
    shutil.rmtree(tmp_input, ignore_errors=True)
    os.makedirs(tmp_input, exist_ok=True)
    for crf in CRF_VALUES:
        shutil.rmtree(f'F:\\tmp_output_av1_crf{crf}', ignore_errors=True)

def copy_to_tmp(chunk):
    for src, rel in tqdm(chunk, desc="📄 Copying to temp", unit="file"):
        dst = os.path.join(tmp_input, rel)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copy2(src, dst)

def ffmpeg_cmd_av1_crf(src, out, crf):
    return [
        'ffmpeg', '-i', src,
        '-c:v', 'libaom-av1',
        '-crf', str(crf),
        '-b:v', '0',
        '-cpu-used', '2',
        '-row-mt', '1',
        '-threads', '0',
        '-c:a', 'libopus',
        '-b:a', '96k',
        out
    ]

def encode_file(src_file, rel_path, crf, bytes_encoded):
    output_dir = f'F:\\tmp_output_av1_crf{crf}'
    target_dir = os.path.join(target_base, f'AV1_crf{crf}')
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(target_dir, exist_ok=True)

    out_file = os.path.join(output_dir, rel_path)
    final_dst = os.path.join(target_dir, rel_path)

    if os.path.exists(final_dst):
        return f"✅ [CRF {crf}] Skipped {rel_path} (already exists)"

    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    cmd = ffmpeg_cmd_av1_crf(src_file, out_file, crf)
    duration = get_duration(src_file)
    file_size = os.path.getsize(src_file)
    start_time = time.time()

    pbar = tqdm(total=duration or 100, desc=f"⏳ CRF{crf}: {os.path.basename(src_file)}", unit='s', leave=False)
    process = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.DEVNULL, text=True)
    time_pattern = re.compile(r'time=(\d+):(\d+):(\d+).(\d+)')

    last_progress = 0
    for line in process.stderr:
        match = time_pattern.search(line)
        if match:
            h, m, s, ms = map(int, match.groups())
            seconds = h * 3600 + m * 60 + s
            pbar.n = seconds
            pbar.refresh()

            # Estimate percent and update bytes
            if duration:
                percent = seconds / duration
                current_progress = int(file_size * percent)
                delta = current_progress - last_progress
                last_progress = current_progress
                bytes_encoded.value += max(0, delta)

    process.wait()
    pbar.n = duration or pbar.n
    pbar.close()

    shutil.move(out_file, final_dst)
    elapsed = time.time() - start_time
    return f"✅ [CRF {crf}] {os.path.basename(src_file)} in {format_elapsed(elapsed)}"

def update_size_pbar(pbar, shared_val, total_bytes):
    while not pbar.disable and pbar.n < total_bytes:
        pbar.n = shared_val.value
        pbar.refresh()
        time.sleep(0.5)

def main():
    all_files = get_all_files(source_dir)
    chunks = prepare_chunks(all_files)

    print(f"🔁 Found {len(chunks)} chunks to process.")
    for i, chunk in enumerate(chunks, 1):
        print(f"\n▶️ Processing chunk {i}/{len(chunks)}")
        clear_tmp_all()
        copy_to_tmp(chunk)

        task_queue = []
        total_bytes = 0
        for src, rel in chunk:
            size = os.path.getsize(src)
            for crf in CRF_VALUES:
                task_queue.append((src, rel, crf, size))
                total_bytes += size

        manager = Manager()
        shared_bytes = manager.Value('i', 0)

        size_pbar = tqdm(total=total_bytes, unit='B', unit_scale=True, desc=f"📦 Total Progress", position=0)
        pbar_thread = threading.Thread(target=update_size_pbar, args=(size_pbar, shared_bytes, total_bytes))
        pbar_thread.start()

        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(encode_file, src, rel, crf, shared_bytes)
                       for src, rel, crf, _ in task_queue]
            for future in as_completed(futures):
                result = future.result()
                tqdm.write(result)

        size_pbar.n = total_bytes
        size_pbar.refresh()
        size_pbar.close()
        pbar_thread.join()

        clear_tmp_all()
        print(f"✅ Finished chunk {i}/{len(chunks)}")

    print("\n🎉 All video processing complete.")

if __name__ == "__main__":
    main()
