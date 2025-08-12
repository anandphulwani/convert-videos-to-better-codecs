import os

def remove_files_of_ext(base_dir, extension):
    if not os.path.isdir(base_dir):
        return

    for filename in os.listdir(base_dir):
        if filename.endswith(f".{extension}"):
            full_path = os.path.join(base_dir, filename)
            try:
                os.remove(full_path)
            except OSError as e:
                print(f"Failed to remove {full_path}: {e}")