"""Environment-variable parsing tools.

These tools treat:

ENV_VAR=

As not set to avoid security pitfalls.
"""
import os


def env_bool(name: str, default: bool = False) -> bool:
    """True for 1/true/yes/on (any case). Empty or unset -> `default`."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    return raw.lower() in {"1", "true", "yes", "on"}


def env_list(name: str, default: list[str] | None = None) -> list[str]:
    """Comma-separated list (dropping blanks). Empty or unset -> `default`."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return list(default or [])
    return [item.strip() for item in raw.split(",") if item.strip()]


def env_str(name: str, default: str = "") -> str:
    """Empty or unset -> `default`."""
    return os.environ.get(name, "").strip() or default


def env_int(name: str, default: int) -> int:
    """Empty or unset -> `default`. A non-numeric value is an error."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    return int(raw)
