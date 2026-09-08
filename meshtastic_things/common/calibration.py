import structlog

log = structlog.get_logger()


def identity(value: float, params: dict) -> float:
    """Default calibration - used when a field has no calibration configured."""
    return value


def offset_scale(value: float, params: dict) -> float:
    """Linear calibration: value * scale + offset."""
    return value * params.get("scale", 1.0) + params.get("offset", 0.0)


def lookup_table(value: float, params: dict) -> float:
    """Non-linear calibration curve.

    Not implemented .
    """
    raise NotImplementedError("lookup_table calibration is a stub - not implemented yet")


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
