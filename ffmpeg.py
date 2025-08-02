import subprocess
import logging

def get_duration(file_path):
    result = subprocess.run([
        'ffprobe', '-v', 'error',
        '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        file_path
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        return int(float(result.stdout.strip()))
    except Exception as e:
        logging.debug(f"ffprobe failed for {file_path}: {e}")
        return None


def ffmpeg_cmd_av1_crf(src, out, crf):
    # use -nostats to suppress the carriage-return stats,
    # and -progress pipe:1 to print newline-terminated progress to stdout
    cmd = [
        'ffmpeg', '-y', '-i', src,
        '-c:v', 'libaom-av1',
        '-crf', str(crf),
        '-b:v', '0',
        '-cpu-used', '2',
        '-row-mt', '1',
        '-threads', '0',
        '-c:a', 'libopus',
        '-b:a', '96k',
        '-nostats',
        '-progress', 'pipe:1',
        out
    ]
    logging.debug(f"FFmpeg command: {' '.join(cmd)}")
    return cmd
