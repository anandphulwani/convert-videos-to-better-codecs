import os

def find_all_dirs(root_dirs):
    all_dirs = []
    for root in root_dirs:
        for dirpath, dirnames, _ in os.walk(root, topdown=False):
            all_dirs.append(dirpath)
    return 
