import os

def get_topmost_dir(path):
    return os.sep.join(path.split(os.sep)[:1])
