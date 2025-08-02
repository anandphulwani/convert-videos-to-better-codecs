import psutil
import logging
import time

from state import pause_flag

def cpu_watchdog():
    while True:
        usage = psutil.cpu_percent(interval=5)
        logging.debug(f"CPU usage: {usage}%")
        if usage >= 95:
            logging.warning("High CPU usage detected. Pausing encoding...")
            pause_flag.clear()
        elif usage <= 10:
            if not pause_flag.is_set():
                logging.info("CPU usage normalized. Resuming encoding...")
            pause_flag.set()
