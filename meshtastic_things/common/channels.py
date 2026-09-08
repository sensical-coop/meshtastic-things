"""Channel naming convention: "<measurement>.<field>".

A *channel* is one addressable timeseries.
InfluxDB's pairs (measurement, field) into single strings.
This vocabulary is shared by (all need to be the same):

  - blueprint inputs/outputs (keyapi's PostprocessingStep)
  - quality thresholds (mesh.quality_config.v1's key)
  - the streams per-channel keying (flink/pattern_detection_job.py)
  - the batches (quality_worker/influx_io.py)

Naming must be the same as writer/point_builders.py, which is what defines
the InfluxDB measurement name. Each telemetry sub-metric dict becomes its own
measurement, and everything else uses its payload kind.
"""

# payload_kinds
# Example: Blueprint outputs all end up in one `derived`
# measurement (attributed by the point's blueprint_id tag).
# A payload_kind "derived:<blueprint>" would yield "<blueprint>.dew_point"
# and never match the real "derived.dew_point" series.
DERIVED = "derived"
QUALITY = "quality"
TELEMETRY = "telemetry"

# channel_origins
# Derived from the payload_kind namespace, but exposed on the API
ORIGIN_MEASURED = "measured"
ORIGIN_DERIVED = "derived"
ORIGIN_QUALITY = "quality"
ORIGIN_OTHER = "other"

def channel_name(payload_kind: str, field_name: str) -> str:
    """Gets the channel name for a payload_kind and field name

    Example:
        channel_name("telemetry:environment_metrics", "temperature")
        -> "environment_metrics.temperature".

    It doesn't affect DERIVED or QUALITY, as they are unchanged
    """
    measurement = (payload_kind or "unknown").split(":", 1)[-1]
    return f"{measurement}.{field_name}"


def namespace_of(payload_kind: str) -> str:
    """The part before the colon, or the whole thing when if ""."""
    return (payload_kind or "").split(":", 1)[0]


def origin_of(payload_kind: str) -> str:
    """Which of the channel types this is."""
    return {
        TELEMETRY: ORIGIN_MEASURED,
        DERIVED: ORIGIN_DERIVED,
        QUALITY: ORIGIN_QUALITY,
    }.get(namespace_of(payload_kind), ORIGIN_OTHER)


def origin_of_measurement(measurement: str) -> str:
    """Get origin of an InfluxDB measurement.

    Only DERIVED and QUALITY are recognised. Telemetry variants are
    dynamic, so anything else is a measured reading.
    This also gives the right answer for fields carrying no catalog
    row at all (packet_id, timestamp_quality)."""
    return origin_of(measurement) if measurement in (DERIVED, QUALITY) else ORIGIN_MEASURED


def channels_in(payload_kind: str, payload: dict):
    """Yield ("<measurement>.<field>", value) for every numeric value of a
    decoded payload, matching build_telemetry_points' measurement choice."""
    fallback = (payload_kind or "unknown").split(":", 1)[-1]
    for key, value in (payload or {}).items():
        if key == "time":
            continue
        if isinstance(value, dict):
            for field, leaf in numeric_leaves(value):
                yield f"{key}.{field}", leaf
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            yield f"{fallback}.{key}", value


def numeric_leaves(d: dict):
    """Recursive numeric-leaf walk.
    bools are excluded."""
    for key, value in d.items():
        if isinstance(value, dict):
            yield from numeric_leaves(value)
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            yield key, value
