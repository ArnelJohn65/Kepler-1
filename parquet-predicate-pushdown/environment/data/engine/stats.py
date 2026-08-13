"""
Statistics writer for row groups.

Bug: the max-value accumulator loop uses range(len(values) - 1) instead of
range(len(values)), so it never considers the last element. This means the
recorded maximum is off by one element — often wrong for boundary predicates.
"""
from typing import Any, Dict, List, Optional


def compute_stats(values: List[Any]) -> Optional[Dict[str, Any]]:
    """Return min/max statistics for a list of values, excluding NULLs.

    Returns None if all values are NULL (no statistics available).
    """
    non_null = [v for v in values if v is not None]
    if not non_null:
        return None

    min_val = non_null[0]
    max_val = non_null[0]

    # BUG: range(len(non_null) - 1) skips the last element.
    # The fix is range(len(non_null)) or range(1, len(non_null)).
    for i in range(len(non_null) - 1):
        v = non_null[i]
        if v < min_val:
            min_val = v
        if v > max_val:
            max_val = v

    return {"min": min_val, "max": max_val, "null_count": len(values) - len(non_null)}
