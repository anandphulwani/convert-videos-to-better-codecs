import psutil
from helpers.logging_utils import log

from state import pause_flag

def cpu_watchdog():
    while True:
        usage = psutil.cpu_percent(interval=5)
        log(f"CPU usage: {usage}%", level="debug")
        if usage >= 95:
            log("High CPU usage detected. Pausing encoding...", level="warning")
            pause_flag.clear()
        elif usage <= 10:
            if not pause_flag.is_set():
                log("CPU usage normalized. Resuming encoding...")
            pause_flag.set()
