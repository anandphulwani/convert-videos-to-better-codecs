import os
import shutil
import subprocess
from multiprocessing import Process
from tqdm import tqdm
import re
import time

# Config
source_dir = r'F:\ForTesting'
target_base = r'F:\ForTesting_Out'
tmp_input = 'F:\\tmp_input'
CHUNK_SIZE = 1 * 1024 * 1024 * 1024  # 1 GB per chunk

# Utility functions
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
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, base_dir)
            all_files.append((full_path, rel_path))
    return sorted(all_files, key=lambda x: x[1])

def prepare_chunks(files):
    chunks = []
    chunk = []
    size = 0
    for full_path, rel_path in files:
        if full_path.lower().endswith('.mp4'):
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

def clear_tmp():
    shutil.rmtree(tmp_input, ignore_errors=True)
    os.makedirs(tmp_input, exist_ok=True)

def copy_to_tmp(chunk):
    for src, rel in tqdm(chunk, desc="📄 Copying to temp", unit="file"):
        dst = os.path.join(tmp_input, rel)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copy2(src, dst)

class CodecConfig:
    def __init__(self, crf_value):
        self.crf = crf_value
        self.name = f'AV1_crf{crf_value}'
        self.output_dir = f'F:\\tmp_output_av1_crf{crf_value}'
        self.target_dir = os.path.join(target_base, self.name)

def ffmpeg_cmd_av1_crf(src, out, crf):
    return [
        'ffmpeg', '-i', src, # '-t', '60',
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

def encode_worker(config: CodecConfig):
    os.makedirs(config.output_dir, exist_ok=True)
    os.makedirs(config.target_dir, exist_ok=True)

    files_to_process = []
    for root, _, files in os.walk(tmp_input):
        for file in files:
            if file.lower().endswith('.mp4'):
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, tmp_input)
                files_to_process.append((full_path, rel_path))

    total_bytes = sum(os.path.getsize(f) for f, _ in files_to_process)
    size_pbar = tqdm(total=total_bytes, unit='B', unit_scale=True,
                     desc=f"📦 {config.name} Total", position=0)

    for src_file, rel_path in files_to_process:
        out_file = os.path.join(config.output_dir, rel_path)
        final_dst = os.path.join(config.target_dir, rel_path)

        os.makedirs(os.path.dirname(out_file), exist_ok=True)
        if os.path.exists(final_dst):
            continue

        cmd = ffmpeg_cmd_av1_crf(src_file, out_file, config.crf)
        duration = get_duration(src_file)

        start_time = time.time()
        if duration:
            ffmpeg_with_progress(cmd, duration, f"{config.name}: {os.path.basename(src_file)}")
        else:
            subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        elapsed = time.time() - start_time
        file_size = os.path.getsize(src_file)
        size_pbar.update(file_size)

        tqdm.write(f"✅ [{config.name}] {os.path.basename(src_file)} in {format_elapsed(elapsed)}")

        os.makedirs(os.path.dirname(final_dst), exist_ok=True)
        shutil.move(out_file, final_dst)

    size_pbar.close()

def main():
    all_files = get_all_files(source_dir)
    chunks = prepare_chunks(all_files)

    crf_values = [24, 60]
    codec_configs = [CodecConfig(crf) for crf in crf_values]

    print(f"🔁 Found {len(chunks)} chunks to process.")
    for i, chunk in enumerate(chunks, 1):
        print(f"\n▶️ Processing chunk {i}/{len(chunks)}")
        clear_tmp()
        copy_to_tmp(chunk)

        processes = []
        for config in codec_configs:
            p = Process(target=encode_worker, args=(config,))
            p.start()
            processes.append(p)

        for p in processes:
            p.join()

        clear_tmp()
        print(f"✅ Finished chunk {i}/{len(chunks)}")

    print("\n🎉 All video processing complete.")

if __name__ == "__main__":
    main()
