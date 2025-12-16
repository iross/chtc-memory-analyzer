"""ASCII histogram generation"""

from typing import List


def create_histogram(values: List[float], bins: int = 10, width: int = 50) -> str:
    """Create a simple ASCII histogram"""
    if not values:
        return "No data"

    min_val = min(values)
    max_val = max(values)

    if min_val == max_val:
        return f"All values equal: {min_val:.2f}"

    # Create bins
    bin_width = (max_val - min_val) / bins
    bin_counts = [0] * bins

    for val in values:
        bin_idx = min(int((val - min_val) / bin_width), bins - 1)
        bin_counts[bin_idx] += 1

    # Find max count for scaling
    max_count = max(bin_counts)

    # Build histogram
    histogram = []
    for i, count in enumerate(bin_counts):
        bin_start = min_val + i * bin_width
        bin_end = bin_start + bin_width
        bar_length = int((count / max_count) * width) if max_count > 0 else 0
        bar = "█" * bar_length
        histogram.append(f"  {bin_start:8.2f} - {bin_end:8.2f} | {bar} ({count})")

    return "\n".join(histogram)
