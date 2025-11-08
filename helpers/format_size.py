def format_size(n_bytes: int) -> str:
    """Format size from bytes -> KB, MB, or GB with mixed units."""
    KB = 1024
    MB = KB * 1024
    GB = MB * 1024

    if n_bytes < MB:
        return f"{n_bytes / KB:.1f} KB"
    elif n_bytes < GB:
        return f"{n_bytes / MB:.1f} MB"
    else:
        gb = int(n_bytes // GB)
        mb = (n_bytes % GB) / MB
        return f"{gb} GB {mb:.0f} MB"
