import time
import glob
import os
import platform
from helpers.logging_utils import log
from config import (CPU_ONLINE_INTERVAL_SECONDS)

def online_all_system_cores(stop_event):
    while not stop_event.is_set():
        log("[Online-Core] Starting new bringing CPU online round...", level="debug")
        if platform.system() == "Linux":
            for cpu_file in glob.glob("/sys/devices/system/cpu/cpu*/online"):
                if os.path.isfile(cpu_file):
                    try:
                        with open(cpu_file, "w") as f:
                            f.write("1")
                        log(f"[Online-Core] Brought {cpu_file} online", level="debug")
                    except PermissionError:
                        log(f"[Online-Core] Permission denied: {cpu_file} (run as root)", level="debug")

        time.sleep(CPU_ONLINE_INTERVAL_SECONDS)
