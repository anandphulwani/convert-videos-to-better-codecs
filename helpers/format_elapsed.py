import time
import math

# def format_elapsed(seconds):
#     return time.strftime("%H:%M:%S", time.gmtime(seconds))

def format_elapsed(seconds: float) -> str:
    """Format elapsed time into s, mm:ss, or hh:mm:ss."""
    if seconds < 60:
        return f"{math.ceil(seconds)}s"
    elif seconds < 3600:
        mins, secs = divmod(math.ceil(seconds), 60)
        return f"{mins:02}m {secs:02}s"
    else:
        hrs, rem = divmod(math.ceil(seconds), 3600)
        mins, secs = divmod(rem, 60)
        return f"{hrs}h {mins:02}m {secs:02}s"
