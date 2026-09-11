import structlog

log = structlog.get_logger()


def identity(value: float, params: dict) -> float:
    """Default calibration - used when a field has no calibration configured."""
    return value


def offset_scale(value: float, params: dict) -> float:
    """Linear calibration: value * scale + offset."""
    return value * params.get("scale", 1.0) + params.get("offset", 0.0)


def lookup_table(value: float, params: dict) -> float:
    """Non-linear calibration curve, linearly interpolated between points.

    params = {"points": [[raw0, calibrated0], [raw1, calibrated1], ...]}
    """
    points = params.get("points") or []
    if len(points) < 1:
        raise ValueError("lookup_table needs at least one [raw, calibrated] point")

    table = sorted((float(raw), float(calibrated)) for raw, calibrated in points)
    if len(table) == 1 or value <= table[0][0]:
        return table[0][1]
    if value >= table[-1][0]:
        return table[-1][1]

    for (raw_low, cal_low), (raw_high, cal_high) in zip(table, table[1:]):
        if raw_low <= value <= raw_high:
            if raw_high == raw_low:  # duplicate x, nothing to interpolate across
                return cal_low
            ratio = (value - raw_low) / (raw_high - raw_low)
            return cal_low + ratio * (cal_high - cal_low)
    # Unreachable: the clamps above cover everything outside the table.
    return table[-1][1]


CALIBRATION_FUNCTIONS = {
    "identity": identity,
    "offset_scale": offset_scale,
    "lookup_table": lookup_table,
}


def apply_calibration(function_name: str, value: float, params: dict | None) -> float:
    """Look up and apply a calibration function by name."""
    func = CALIBRATION_FUNCTIONS.get(function_name, identity)
    try:
        return func(value, params or {})
    except Exception as e:
        log.warning("Calibration failed, passing value through unchanged", function=function_name, error=str(e))
        return value
