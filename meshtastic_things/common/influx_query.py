"""Validation helpers for anything that interpolates values into Influx.
"""
import re
import uuid
from datetime import datetime, timezone

_DURATION_RE = re.compile(r"^-?\d+(ms|s|m|h|d|w|mo|y)$")
_IDENT_RE = re.compile(r"^[A-Za-z0-9_:.\-]+$")
# A calendar date, optionally with a time and a UTC offset.
_ABSOLUTE_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}"
    r"(?:[T ]\d{2}:\d{2}(?::\d{2})?(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)?$"
)
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


def _absolute_bound(value: str) -> str | None:
    """Canonical UTC timestamp for a date or datetime, or None if it is neither.

    The result is rebuilt from the parsed value rather than passed through, so
    nothing of the caller's text reaches the query.
    """
    if not _ABSOLUTE_RE.match(value):
        return None
    normalized = f"{value[:-1]}+00:00" if value.endswith("Z") else value
    try:
        moment = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if moment.tzinfo is None:
        # A bare date or a time without an offset is read as UTC.
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def validate_range_bound(value: str, name: str) -> str:
    """Accept a relative duration, `now()`, a date, or a datetime.

    Dates and datetimes are returned as a UTC timestamp. A date is taken as
    midnight, and a datetime without an offset is read as UTC.
    """
    value = (value or "").strip()
    if value == "now()" or _DURATION_RE.match(value):
        return value
    absolute = _absolute_bound(value)
    if absolute is not None:
        return absolute
    raise ValueError(
        f"Invalid {name}: {value!r} (expected a relative duration like -1h, now(), "
        f"a date like 2026-09-01, or a timestamp like 2026-09-01T13:45:00Z)"
    )


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
