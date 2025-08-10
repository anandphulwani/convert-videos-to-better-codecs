import os

from helpers.remove_path import remove_path

def remove_empty_dirs_upwards(path):
    """Recursively remove empty directories upward from the given path."""
    while os.path.isdir(path) and not os.listdir(path):
        remove_path(path)
        path = os.path.dirname(path)
