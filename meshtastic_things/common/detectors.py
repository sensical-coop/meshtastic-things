"""Detector decision functions for pattern-detection.
"""

# Used when a channel has no reading interval
DEFAULT_FLAT_WINDOW = 5
MIN_FLAT_WINDOW = 3
MAX_FLAT_WINDOW = 240


def stuck_value_triggered(recent_values: list[float], window: int = 5) -> bool:
    """Triggers when a sensor reports the same value N times in a row."""
    if len(recent_values) < window:
        return False
    return len(set(recent_values[-window:])) == 1


def flat_window_for(interval_seconds: float | None, hours: float = 3.0) -> int:
    if not interval_seconds:
        return DEFAULT_FLAT_WINDOW
    return max(MIN_FLAT_WINDOW, min(MAX_FLAT_WINDOW, int(hours * 3600 / interval_seconds)))


def range_check_triggered(value: float, min_value: float | None, max_value: float | None) -> bool:
    """Fires when a value falls outside a configured [min, max] range."""
    if min_value is not None and value < min_value:
        return True
    if max_value is not None and value > max_value:
        return True
    return False


DETECTORS = {
    "stuck_value": stuck_value_triggered,
    "range_check": range_check_triggered,
}
