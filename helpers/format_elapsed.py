import time
import math

# def format_elapsed(seconds):
#     return time.strftime("%H:%M:%S", time.gmtime(seconds))

def format_elapsed(seconds: float) -> str:
    """Format elapsed time into s, mm:ss, hh:mm:ss, or dd:hh:mm:ss."""
    if seconds < 60:
        return f"{math.ceil(seconds)}s"
    elif seconds < 3600:
        mins, secs = divmod(math.ceil(seconds), 60)
        return f"{mins:02}m {math.ceil(secs):02}s"
    elif seconds < 86400:
        hrs, rem = divmod(math.ceil(seconds), 3600)
        mins, secs = divmod(rem, 60)
        return f"{hrs}h {mins:02}m {math.ceil(secs):02}s"
    else:
        days, rem = divmod(math.ceil(seconds), 86400)
        hrs, rem = divmod(rem, 3600)
        mins, secs = divmod(rem, 60)
        return f"{days}d {hrs:02}h {mins:02}m {math.ceil(secs):02}s"
