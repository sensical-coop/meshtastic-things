from datetime import datetime, timezone

import structlog
from influxdb_client import Point

log = structlog.get_logger()

MIN_FEASIBLE_TIME = 1577836800  # 2020-01-01 in epoch (s)

# payload_kind values that are intentionally not persisted (logged at debug though).
SKIP_PAYLOAD_KINDS = {
    "portnum:1",  # TEXT_MESSAGE_APP - free text, not a timeseries metric
    "portnum:5",  # ROUTING_APP - mesh routing/ack control traffic
}


def _convert_date(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _resolve_timestamp(event: dict) -> tuple[int, str]:
    timestamp = event["rx_time"]
    quality = "rx_time"
    payload_time = (event.get("payload") or {}).get("time")
    if payload_time and payload_time > MIN_FEASIBLE_TIME:
        timestamp = payload_time
        quality = "payload"
    return timestamp, quality


def _base_point_dict(event: dict, measurement: str) -> dict:
    timestamp, quality = _resolve_timestamp(event)
    tags = {"node_id": event["node_id"]}
    if event.get("mesh_id"):
        # node_id alone is not unique
        tags["mesh_id"] = event["mesh_id"]
    if event.get("channel_id"):
        tags["channel_id"] = event["channel_id"]
    if event.get("gateway_id"):
        tags["gateway_id"] = event["gateway_id"]
    return {
        "measurement": measurement,
        "tags": tags,
        "fields": {
            "packet_id": event["packet_id"],
            "timestamp_quality": quality,
        },
        "time": _convert_date(timestamp),
    }


def build_telemetry_points(event: dict) -> list[Point]:
    """One Point per populated Telemetry sub-metric dict (device_metrics, environment_metrics,
    etc.). Handles the (rare in practice) case of multiple sub-metrics present at once instead
    of silently picking whichever dict-valued key was seen last, like the code this replaces."""
    payload = event.get("payload") or {}
    points = []
    for key, value in payload.items():
        if key == "time" or not isinstance(value, dict):
            continue
        point_dict = _base_point_dict(event, key)
        for field, field_value in value.items():
            try:
                point_dict["fields"][field] = float(field_value)
            except (TypeError, ValueError):
                continue
        points.append(Point.from_dict(point_dict))
    return points


def build_position_points(event: dict) -> list[Point]:
    payload = event.get("payload") or {}
    point_dict = _base_point_dict(event, "position")
    if "latitude_i" in payload:
        point_dict["fields"]["latitude"] = payload["latitude_i"] / 1e7
    if "longitude_i" in payload:
        point_dict["fields"]["longitude"] = payload["longitude_i"] / 1e7
    if "altitude" in payload:
        try:
            point_dict["fields"]["altitude"] = float(payload["altitude"])
        except (TypeError, ValueError):
            pass
    if len(point_dict["fields"]) <= 2:  # nothing beyond packet_id/timestamp_quality
        return []
    return [Point.from_dict(point_dict)]


def build_nodeinfo_points(event: dict) -> list[Point]:
    payload = event.get("payload") or {}
    point_dict = _base_point_dict(event, "nodeinfo")
    for tag_field in ("long_name", "short_name", "hw_model", "role"):
        if payload.get(tag_field):
            point_dict["tags"][tag_field] = str(payload[tag_field])
    if payload.get("id"):
        point_dict["fields"]["user_id"] = str(payload["id"])
    return [Point.from_dict(point_dict)]


def build_generic_points(event: dict) -> list[Point]:
    """Fallback for any payload_kind without a dedicated builder above, so a
    brand-new portnum gets *something* persisted without requiring a code change first."""
    payload = event.get("payload") or {}
    portnum = event.get("portnum", "unknown")
    point_dict = _base_point_dict(event, f"portnum_{portnum}")
    for key, value in payload.items():
        if isinstance(value, bool) or isinstance(value, (int, float)):
            point_dict["fields"][key] = value
    if len(point_dict["fields"]) <= 2:  # nothing beyond packet_id/timestamp_quality
        return []
    return [Point.from_dict(point_dict)]


def build_alarm_points(event: dict) -> list[Point]:
    """Quality alarm events have a different shape from decoded messages - no
    packet_id/rx_time/payload_kind - so they don't go through _base_point_dict
    like the builders above. Example:

        {mesh_id, device_id, channel, detector, source, value, message, triggered_at}

    """
    device_id = event.get("device_id", event.get("node_id"))
    channel = event.get("channel", event.get("field"))

    tags = {"detector": event["detector"]}

    if device_id is not None:
        tags["node_id"] = str(device_id)
    if event.get("mesh_id"):
        # node_id alone is not unique
        tags["mesh_id"] = event["mesh_id"]
    if channel:
        # Absent for device alarms.
        tags["channel"] = channel
    if event.get("source"):
        # "stream" or "batch" detection.
        tags["source"] = event["source"]

    fields = {"message": str(event.get("message", ""))}
    value = event.get("value")
    if value is not None:
        fields["value"] = float(value)

    point_dict = {
        "measurement": "alarms",
        "tags": tags,
        "fields": fields,
        "time": event["triggered_at"],
    }
    return [Point.from_dict(point_dict)]


def get_points(event: dict) -> list[Point]:
    if "detector" in event:  # alarm event, not a decoded telemetry message
        return build_alarm_points(event)

    payload_kind = event.get("payload_kind", "") or ""
    if payload_kind in SKIP_PAYLOAD_KINDS:
        log.debug("Skipping payload kind", payload_kind=payload_kind)
        return []
    if payload_kind.startswith("telemetry"):
        return build_telemetry_points(event)
    if payload_kind == "position":
        return build_position_points(event)
    if payload_kind == "nodeinfo":
        return build_nodeinfo_points(event)
    return build_generic_points(event)
