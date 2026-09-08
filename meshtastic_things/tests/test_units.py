"""common/units.py - the unit -> quantity_kind rule, and the one definition of
the quality channels that keyapi and quality_worker both read."""
import pytest

from common.units import (
    CONCENTRATION,
    COUNT,
    PHYSICAL_PROPERTY,
    QUALITY_CHANNELS,
    QUANTITY_KIND_BY_UNIT,
    RATIO,
    quantity_kind_for,
)


@pytest.mark.parametrize(
    "unit,expected",
    [
        ("degC", PHYSICAL_PROPERTY),
        ("g/m3", PHYSICAL_PROPERTY),
        ("s", PHYSICAL_PROPERTY),
        ("%", RATIO),
        ("ratio", RATIO),
        ("count", COUNT),
        ("ppb", CONCENTRATION),
        ("ug/m3", CONCENTRATION),
    ],
)
def test_known_units_classify(unit, expected):
    assert quantity_kind_for(unit) == expected


def test_unknown_unit_is_none_not_other():
    """Test for 'We don't know' vs 'we know it's uncategorised'"""
    # Only the second should be in the catalog
    assert quantity_kind_for("furlongs/fortnight") is None
    assert quantity_kind_for(None) is None
    assert quantity_kind_for("") is None


def test_every_quality_channel_has_a_classifiable_unit():
    # Important so that an unclassifiable channel never ends
    # with a null quantity_kind.
    for field_name, (unit, description) in QUALITY_CHANNELS.items():
        assert quantity_kind_for(unit) is not None, f"{field_name} has unclassifiable unit {unit!r}"
        assert description, f"{field_name} has no description"


def test_algorithm_output_units_are_all_in_the_table():
    """Names an wrongly decorated algorithm rather than breaking the whole collection."""
    from common.algorithms import INHERIT, discover

    for spec in discover().values():
        for output, unit in spec.output_units.items():
            if unit.startswith(INHERIT):
                continue
            assert unit in QUANTITY_KIND_BY_UNIT, f"{spec.name}.{output} uses unknown unit {unit!r}"
