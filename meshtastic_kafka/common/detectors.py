"""Detector decision functions for pattern-detection.
"""


def stuck_value_triggered(recent_values: list[float], window: int = 5) -> bool:
    """Triggers when a sensor reports the same value N times in a row."""
    if len(recent_values) < window:
        return False
    return len(set(recent_values[-window:])) == 1


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
