import os

TRUE_VALUES = {"1", "true", "yes", "on"}


def get_bool_env(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in TRUE_VALUES
