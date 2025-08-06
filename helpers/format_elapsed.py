import time

def format_elapsed(seconds):
    return time.strftime("%H:%M:%S", time.gmtime(seconds))
