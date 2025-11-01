import subprocess
from helpers.logging_utils import log

def ffmpeg_get_duration(file_path):
    result = subprocess.run([
        'ffprobe', '-v', 'error',
        '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        file_path
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        duration_sec = float(result.stdout.strip())
        duration_ms = int(duration_sec * 1000 * 1000)
        return duration_ms
    except Exception as e:
        log(f"ffprobe failed for {file_path}: {e}", level="debug")
        return None


def ffmpeg_av1_crf_cmd_generator(src, out, crf, error_type_for_retry=None):
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

    if error_type_for_retry == "twopass_stats_buf_ctx_error":
        idx = cmd.index('-nostats') + 1
        cmd[idx:idx] = ['-lag-in-frames', '0']

    log(f"FFmpeg command: {' '.join(cmd)}", level="debug")
    return cmd