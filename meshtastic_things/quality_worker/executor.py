"""Blueprint execution and completeness analysis

The executor's job is to take a stored blueprint (an ordered list of steps,
each naming an algorithm and binding its inputs to channel names) into derived
series. Two important details:

- Alignment: An algorithm like ec_sensor_ppb needs two ADC channels and
  a temperature reading for the same moment. Channels are sampled
  independently, so they're aligned on timestamp before any
  algorithm sees them.
- Chaining: A step's outputs are added to the namespace under
  their channel names, so a later step can consume them exactly like a raw
  channel - which is how "ADC -> ppb -> ug/m3" is expressed as two steps.
"""
from datetime import timedelta

import structlog

from common.algorithms import POINTWISE, get as get_algorithm

log = structlog.get_logger()

# Two samples land in the same aligned row if they're within this fraction of
# the alignment interval. Sensors in one device share a clock but not an exact
# sampling instant, so exact-timestamp joins would drop nearly everything.
DEFAULT_ALIGNMENT_TOLERANCE = 0.5


def align_series(named_series: dict[str, list[tuple]], interval_seconds: float) -> list[dict]:
    """Join independently sampled channels onto shared timestamps.

    Uses the first channel as the time base and, for each of its timestamps,
    takes the nearest sample from every other channel within
    `interval_seconds * DEFAULT_ALIGNMENT_TOLERANCE`. Rows where any channel
    has no sample in range are dropped.

    Returns [{"time": dt, "<channel>": value, ...}, ...].
    """
    if not named_series:
        return []
    channels = list(named_series)
    base_channel = channels[0]
    base = named_series[base_channel]
    if not base:
        return []
    if len(channels) == 1:
        return [{"time": t, base_channel: v} for t, v in base]

    tolerance = timedelta(seconds=interval_seconds * DEFAULT_ALIGNMENT_TOLERANCE)
    # Cursor per channel
    cursors = {channel: 0 for channel in channels[1:]}
    rows = []
    for timestamp, base_value in base:
        row = {"time": timestamp, base_channel: base_value}
        complete = True
        for channel in channels[1:]:
            series = named_series[channel]
            index = cursors[channel]
            while index + 1 < len(series) and abs(series[index + 1][0] - timestamp) <= abs(
                series[index][0] - timestamp
            ):
                index += 1
            cursors[channel] = index
            if not series or abs(series[index][0] - timestamp) > tolerance:
                complete = False
                break
            row[channel] = series[index][1]
        if complete:
            rows.append(row)
    return rows


def run_steps(steps: list[dict], rows: list[dict]) -> dict[str, list[tuple]]:
    """Execute an ordered blueprint over aligned rows.

    Returns {derived_channel: [(time, value), ...]} for every channel any step
    declared as an output. Rows are mutated in place as steps run.

    A raising algorithm drops that one row rather than failing the run.
    """
    outputs: dict[str, list[tuple]] = {}
    failures: dict[str, int] = {}

    for step in sorted(steps, key=lambda s: s["order"]):
        spec = get_algorithm(step["function_name"])
        if spec is None:
            log.warning("unknown_algorithm_skipped", function_name=step["function_name"], order=step["order"])
            continue
        if spec.mode != POINTWISE:
            # Windowed algorithms need the whole series
            log.warning("windowed_algorithm_not_supported_yet", function_name=spec.name, order=step["order"])
            continue

        input_bindings = step.get("inputs") or {}
        params = step.get("params") or {}
        output_bindings = step.get("outputs") or {}

        for row in rows:
            try:
                inputs = {arg: row[channel] for arg, channel in input_bindings.items()}
            except KeyError:
                continue  # an upstream step produced nothing for this row
            try:
                result = spec.call(inputs, params)
            except Exception as e:
                failures[spec.name] = failures.get(spec.name, 0) + 1
                if failures[spec.name] == 1:
                    log.warning("algorithm_failed", function_name=spec.name, error=str(e))
                continue
            for output_name, channel in output_bindings.items():
                if output_name not in result:
                    continue
                row[channel] = result[output_name]
                outputs.setdefault(channel, []).append((row["time"], result[output_name]))

    for name, count in failures.items():
        log.warning("algorithm_failed_total", function_name=name, rows_skipped=count)
    return outputs


def resolve_output_units(steps: list[dict], channel_units: dict[str, str | None]) -> dict[str, str | None]:
    """{derived channel: unit} for every channel these steps produce.

    A derived channel has no protobuf field to get a unit from, so it needs
    to be declared on the algorithm itself.
    """
    known = dict(channel_units)
    resolved: dict[str, str | None] = {}

    for step in sorted(steps, key=lambda s: s["order"]):
        spec = get_algorithm(step["function_name"])
        if spec is None:
            continue
        input_units = {arg: known.get(channel) for arg, channel in (step.get("inputs") or {}).items()}
        for output_name, channel in (step.get("outputs") or {}).items():
            unit = spec.unit_for(output_name, input_units)
            resolved[channel] = unit
            if unit:
                known[channel] = unit
    return resolved


def analyse_completeness(
    series: list[tuple], window_seconds: int, reading_interval_seconds: int
) -> dict:
    """Returns observed/expected counts, and the largest
    gap in seconds.
    """
    expected = max(1, int(window_seconds // max(1, reading_interval_seconds)))
    observed = len(series)
    largest_gap = 0.0
    if len(series) >= 2:
        for (earlier, _), (later, _) in zip(series, series[1:]):
            largest_gap = max(largest_gap, (later - earlier).total_seconds())
    return {
        "observed": observed,
        "expected": expected,
        # Capped at 1.0: more samples than expected (faster than expected)
        "ratio": min(1.0, observed / expected) if expected else 0.0,
        "largest_gap_seconds": largest_gap,
    }
