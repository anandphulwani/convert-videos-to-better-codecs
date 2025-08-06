import os

def get_next_chunk_dir(base_path):
    os.makedirs(base_path, exist_ok=True)
    existing = [d for d in os.listdir(base_path) if d.startswith("chunk")]
    numbers = sorted([
        int(d.replace("chunk", "")) for d in existing if d.replace("chunk", "").isdigit()
    ])
    next_num = (numbers[-1] + 1) if numbers else 1
    return f"chunk{next_num:02d}"
