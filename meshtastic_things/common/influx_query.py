"""Validation helpers for anything that interpolates values into Influx.
"""
import re
import uuid

_DURATION_RE = re.compile(r"^-?\d+(ms|s|m|h|d|w|mo|y)$")
_IDENT_RE = re.compile(r"^[A-Za-z0-9_:.\-]+$")
ALLOWED_AGG = {"mean", "max", "min", "last", "first", "sum", "count"}


def validate_ident(value: str, name: str) -> str:
    """Measurement/field names: alphanumerics plus _ : . - only."""
    if not _IDENT_RE.match(value or ""):
        raise ValueError(f"Invalid {name}: {value!r}")
    return value


def validate_mesh_id(value: str) -> str:
    """Fails on anything that isn't a real UUID."""
    try:
        return str(uuid.UUID(str(value)))
    except (ValueError, AttributeError, TypeError):
        raise ValueError(f"Invalid mesh_id: {value!r}")


def validate_range_bound(value: str, name: str) -> str:
    if value == "now()" or _DURATION_RE.match(value or ""):
        return value
    raise ValueError(f"Invalid {name}: {value!r} (expected a relative duration like -1h, or now())")


def validate_duration(value: str, name: str) -> str:
    if not _DURATION_RE.match(value or ""):
        raise ValueError(f"Invalid {name}: {value!r}")
    return value


def validate_agg(value: str) -> str:
    if value not in ALLOWED_AGG:
        raise ValueError(f"Invalid agg: {value!r} (allowed: {sorted(ALLOWED_AGG)})")
    return value


def split_channel(channel: str) -> tuple[str, str]:
    """"<measurement>.<field>" -> ("<measurement>", "<field>").

    Validate each part of the channel form used by blueprint step inputs and outputs
    (see PostprocessingStep). Both halves are validated.
    """
    measurement, separator, field = (channel or "").partition(".")
    if not separator or not measurement or not field:
        raise ValueError(f"Invalid channel {channel!r} - expected '<measurement>.<field>'")
    return validate_ident(measurement, "measurement"), validate_ident(field, "field")


def duration_seconds(value: str) -> int:
    """Relative duration in positive seconds
    Example:
    "-7d"
    -> 604800.
    Used to turn a window into a sample count."""
    validate_duration(value, "duration")
    units = {"ms": 0.001, "s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800, "mo": 2592000, "y": 31536000}
    match = re.match(r"^-?(\d+)(ms|s|m|h|d|w|mo|y)$", value)
    return int(abs(int(match.group(1)) * units[match.group(2)]))
