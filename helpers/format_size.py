def format_size(n_bytes: int) -> str:
    """Format size from bytes -> KB or MB."""
    KB = 1024
    MB = KB * 1024
    if n_bytes < MB:
        return f"{n_bytes / KB:.1f} KB"
    else:
        return f"{n_bytes / MB:.1f} MB"
