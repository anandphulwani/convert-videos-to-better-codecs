import os

def get_next_chunk_dir(base_path):
    os.makedirs(base_path, exist_ok=True)

    # Gather all entries (files and dirs) that start with 'chunk' and end with either nothing (dirs) or '.done' (files)
    relevant = [
        entry for entry in os.listdir(base_path)
        if entry.startswith("chunk") and (
            os.path.isdir(os.path.join(base_path, entry)) or entry.endswith(".done")
        )
    ]

    # Extract numeric suffixes after "chunk", whether dir or file (like chunk01 or chunk01.done)
    numbers = sorted([
        int(entry.replace("chunk", "").replace(".done", ""))
        for entry in relevant
        if entry.replace("chunk", "").replace(".done", "").isdigit()
    ])
    next_num = (numbers[-1] + 1) if numbers else 1
    return f"chunk{next_num:02d}"
