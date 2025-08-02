import os

def has_files(base_dir):
    return any(file.lower().endswith('.mp4') for _, _, files in os.walk(base_dir) for file in files)
