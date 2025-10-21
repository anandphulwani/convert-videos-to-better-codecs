import random
import psutil
import time
import math
from collections import deque
from helpers.logging_utils import log
from includes.state import pause_flag
from config import (PROCESSES_NICE_RANGE_TO_COLLECT, CPU_PERCENT_BENCHMARK_TO_PAUSE, 
                    CPU_VALUES_TO_COLLECT_IN_ONE_RUN, CPU_RUNS_TO_BENCHMARK_USAGE)

cpu_usage_history = deque(maxlen=CPU_RUNS_TO_BENCHMARK_USAGE)

def get_filtered_cpu_usage():
    total_cpu = 0.0
    for proc in psutil.process_iter(['cpu_percent', 'nice']):
        try:
            nice = proc.info['nice']
            cpu = proc.info['cpu_percent']
            if nice >= PROCESSES_NICE_RANGE_TO_COLLECT[0] and nice <= PROCESSES_NICE_RANGE_TO_COLLECT[1]:
                total_cpu += cpu
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    logical_cpus = psutil.cpu_count()
    return total_cpu / logical_cpus

def collect_cpu_samples():
    for proc in psutil.process_iter():
        proc.cpu_percent(interval=None)
    time.sleep(1)

    samples = []
    for _ in range(CPU_VALUES_TO_COLLECT_IN_ONE_RUN):
        usage = get_filtered_cpu_usage()
        samples.append(usage)

        sleep_duration = random.uniform(0.01, 1.5) # Random sleep to get realistic data
        time.sleep(sleep_duration)

    avg_usage_single_run = sum(samples) / len(samples)
    return avg_usage_single_run

def cpu_watchdog(stop_event):
    while not stop_event.is_set():
        log("[Monitor] Starting new sampling round...", level="debug")
        
        usage = collect_cpu_samples()
        cpu_usage_history.append(usage)

        avg_usage = sum(cpu_usage_history) / len(cpu_usage_history)
        log(f"[Monitor] Avg CPU usage (last {len(cpu_usage_history)}): {avg_usage:.2f}%", level="debug")

        if math.ceil(avg_usage) > CPU_PERCENT_BENCHMARK_TO_PAUSE:
            if not pause_flag.is_set():
                log(f"CPU usage ({math.ceil(avg_usage)}%) exceeds {CPU_PERCENT_BENCHMARK_TO_PAUSE}%. Pausing encoding...", level="debug")
                pause_flag.set()
        else:
            if pause_flag.is_set():
                log("CPU usage normalized. Resuming encoding...", level="debug")
                pause_flag.clear()

        time.sleep(10 * 60)  # Sleep for 10 minutes
