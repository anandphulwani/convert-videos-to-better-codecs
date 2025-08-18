import sys

def clear_terminal_below_cursor(self):
    """Clears the terminal below the current cursor position."""
    with self.lock:
        sys.stdout.write("\033[J")  # ANSI code: erase below cursor
        sys.stdout.flush()
