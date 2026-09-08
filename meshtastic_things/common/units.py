"""Units, and the quantity kind for each.
"""

# The vocabulary keyapi's catalog uses.
PHYSICAL_PROPERTY = "physical_property"
RATIO = "ratio"
COUNT = "count"
CONCENTRATION = "concentration"
OTHER = "other"

# Every unit the catalog currently knows, plus the ones the algorithms in
# common/algorithms produce. Extend as new units appear.
QUANTITY_KIND_BY_UNIT = {
    "%": RATIO,
    "ratio": RATIO,
    "1/100": OTHER,
    "count": COUNT,
    "#/0.1L": COUNT,
    "bytes": OTHER,
    "ppb": CONCENTRATION,
    "ppm": CONCENTRATION,
    "ug/m3": CONCENTRATION,
    "A": PHYSICAL_PROPERTY,
    "V": PHYSICAL_PROPERTY,
    "MOhm": PHYSICAL_PROPERTY,
    "bpm": PHYSICAL_PROPERTY,
    "dBm": PHYSICAL_PROPERTY,
    "deg": PHYSICAL_PROPERTY,
    "degC": PHYSICAL_PROPERTY,
    "g/m3": PHYSICAL_PROPERTY,
    "hPa": PHYSICAL_PROPERTY,
    "kg": PHYSICAL_PROPERTY,
    "lux": PHYSICAL_PROPERTY,
    "m/s": PHYSICAL_PROPERTY,
    "mm": PHYSICAL_PROPERTY,
    "s": PHYSICAL_PROPERTY,
    "uR/h": PHYSICAL_PROPERTY,
    "um": PHYSICAL_PROPERTY,
}


def quantity_kind_for(unit: str | None) -> str | None:
    """
    The quantity kind a unit implies, or None for an unknown/absent unit.
    """
    if not unit:
        return None
    return QUANTITY_KIND_BY_UNIT.get(unit)


# Data-quality channels calculated in quality_worker
QUALITY_CHANNELS = {
    "completeness_ratio": ("ratio", "Observed readings / expected readings over the window, capped at 1.0"),
    "observed": ("count", "Readings actually present in the window"),
    "expected": ("count", "Readings the configured reading interval implies for the window"),
    "largest_gap_seconds": ("s", "Longest interval between consecutive readings in the window"),
}
