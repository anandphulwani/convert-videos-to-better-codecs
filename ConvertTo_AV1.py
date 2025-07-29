import os
import subprocess

source_dir = 'W:\Source_Videos'
target_dir = 'W:\Source_Videos_AV1'

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
    return str(int(int(bitrate) / 1000)) + 'k'  # Convert to kbps

for root, dirs, files in os.walk(source_dir):
    dirs.sort()   # Sort directories in-place
    files.sort()  # Sort files in-place
    for file in files:
        if file.endswith('.mp4'):
            source_file = os.path.join(root, file)
            relative_path = os.path.relpath(root, source_dir)
            target_folder = os.path.join(target_dir, relative_path)
            os.makedirs(target_folder, exist_ok=True)

            target_file = os.path.join(target_folder, file)

            bitrate = get_bitrate(source_file)
            if bitrate:
                cmd = [
                    'ffmpeg',
                    '-i', source_file,
                    '-c:v', 'libaom-av1',
                    '-b:v', bitrate,
                    '-preset', 'slow',
                    '-c:a', 'libopus',
                    '-b:a', '64k',
                    # '-c:v', 'libx265',
                    # '-b:v', bitrate,
                    # '-preset', 'slow',
                    # '-c:a', 'aac',
                    # '-b:a', '64k',
                    target_file
                ]
                subprocess.run(cmd)
            else:
                print(f"Could not determine bitrate for {source_file}, skipping.")
