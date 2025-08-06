import os

def remove_topmost_dir(path):
    return os.sep.join(path.split(os.sep)[1:])
