"""Statistical utilities"""

from typing import Dict, List, Union

import pandas as pd


def calculate_stats(values: Union[List[float], pd.Series]) -> Dict[str, float]:
    """
    Calculate statistical measures for a list or Series of values.

    Args:
        values: List of floats or pandas Series

    Returns:
        Dict with keys: mean, median, stdev, min, max, count
    """
    # Convert to pandas Series if list
    if isinstance(values, list):
        if not values:
            return {"mean": 0, "median": 0, "stdev": 0, "min": 0, "max": 0, "count": 0}
        series = pd.Series(values)
    else:
        series = values

    if series.empty:
        return {"mean": 0, "median": 0, "stdev": 0, "min": 0, "max": 0, "count": 0}

    return {
        "mean": series.mean(),
        "median": series.median(),
        "stdev": series.std(),
        "min": series.min(),
        "max": series.max(),
        "count": len(series),
    }
