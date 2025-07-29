import os
import shutil
import subprocess
import time

source_dir = r'P:\\Source_Videos'
target_dir_H265 = r'P:\\Source_Videos_H265'
target_dir_AV1 = r'P:\\Source_Videos_AV1'

tmp_input = 'F:\\tmp_input'
tmp_output = 'F:\\tmp_output'
CHUNK_SIZE = 0.05 * 1024 * 1024 * 1024  # 29 GB

def get_bitrate(file_path):
    result = subprocess.run([
        'ffprobe', '-v', 'error',
        '-select_streams', 'v:0',
        '-show_entries', 'stream=bit_rate',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        file_path
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    bitrate = result.stdout.strip()
    if bitrate == 'N/A' or not bitrate:
        return None
    return str(int(int(bitrate) / 1000)) + 'k'

def get_all_files(base_dir):
    all_files = []
    for root, _, files in os.walk(base_dir):
        for file in files:
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, base_dir)
            all_files.append((full_path, rel_path))
    return sorted(all_files, key=lambda x: x[1])

def already_processed(rel_path):
    h265_path = os.path.join(target_dir_H265, rel_path)
    av1_path = os.path.join(target_dir_AV1, rel_path)
    return os.path.exists(h265_path) and os.path.exists(av1_path)

def prepare_chunks(files):
    chunks = []
    chunk = []
    size = 0

    for full_path, rel_path in files:
        if already_processed(rel_path):
            continue
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

def clear_temp_dirs():
    shutil.rmtree(tmp_input, ignore_errors=True)
    shutil.rmtree(tmp_output, ignore_errors=True)
    os.makedirs(tmp_input, exist_ok=True)
    os.makedirs(tmp_output, exist_ok=True)

def copy_to_tmp(chunk):
    for src, rel in chunk:
        dst = os.path.join(tmp_input, rel)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copy2(src, dst)

def process_stage(codec, target_dir):
    for root, _, files in os.walk(tmp_input):
        for file in files:
            rel_path = os.path.relpath(os.path.join(root, file), tmp_input)
            src_file = os.path.join(root, file)
            out_file = os.path.join(tmp_output, rel_path)
            os.makedirs(os.path.dirname(out_file), exist_ok=True)

            if file.lower().endswith('.mp4'):
                if os.path.exists(os.path.join(target_dir, rel_path)):
                    print(f"[{codec}] Skipping {rel_path} (already exists)")
                    continue
                bitrate = get_bitrate(src_file)
                if not bitrate:
                    print(f"Could not determine bitrate for {src_file}, skipping.")
                    continue

                cmd = [
                    'ffmpeg', '-i', src_file,
                    '-c:v', 'libx265' if codec == 'H265' else 'libaom-av1',
                    '-b:v', bitrate,
                    '-preset', 'slow',
                    '-c:a', 'aac' if codec == 'H265' else 'libopus',
                    '-b:a', '64k',
                    out_file
                ]
                subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            else:
                shutil.copy2(src_file, out_file)

    # Move all from tmp_output to target_dir
    for root, _, files in os.walk(tmp_output):
        for file in files:
            rel_path = os.path.relpath(os.path.join(root, file), tmp_output)
            final_dst = os.path.join(target_dir, rel_path)
            if not os.path.exists(final_dst):
                os.makedirs(os.path.dirname(final_dst), exist_ok=True)
                shutil.move(os.path.join(root, file), final_dst)

def main():
    print("🔁 Scanning source...")
    all_files = get_all_files(source_dir)
    chunks = prepare_chunks(all_files)
    print(f"📦 Total chunks to process: {len(chunks)}")

    for i, chunk in enumerate(chunks):
        print(f"\n🚀 Processing chunk {i+1}/{len(chunks)}...")
        clear_temp_dirs()
        copy_to_tmp(chunk)

        print("🎞️  H.265 stage...")
        process_stage('H265', target_dir_H265)

        print("🎞️  AV1 stage...")
        process_stage('AV1', target_dir_AV1)

        clear_temp_dirs()

    print("\n✅ All processing complete.")

if __name__ == "__main__":
    main()
