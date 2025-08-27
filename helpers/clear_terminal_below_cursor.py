import sys

def clear_terminal_below_cursor():
    """Clears the terminal below the current cursor position."""
    sys.stdout.write("\r\033[J")
    sys.stdout.flush()
