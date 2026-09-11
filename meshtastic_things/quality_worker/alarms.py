"""Alarms"""

import json
import os
from datetime import datetime, timezone

import structlog
from kafka import KafkaProducer

log = structlog.get_logger()

SOURCE_BATCH = "batch"
SOURCE_STREAM = "stream"


def build_alarm(
    mesh_id: str,
    device_id: int,
    channel: str,
    detector: str,
    message: str,
    value: float | None = None,
    source: str = SOURCE_BATCH,
) -> dict:

    return {
        "mesh_id": str(mesh_id),
        "device_id": int(device_id),
        "channel": channel,
        "detector": detector,
        "source": source,
        "value": value,
        "message": message,
        "triggered_at": datetime.now(timezone.utc).isoformat(),
    }


class QualityPublisher:
    """Two topics are published from here:
    - alarms
    - the sensor-discovery events that register a device's computed channels."""

    def __init__(self, bootstrap_servers: str, alarms_topic: str, discovery_topic: str):
        self._alarms_topic = alarms_topic
        self._discovery_topic = discovery_topic
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
        )

    def publish(self, alarm: dict) -> None:
        key = f"{alarm['mesh_id']}:{alarm['device_id']}:{alarm['channel']}:{alarm['detector']}"
        self._producer.send(self._alarms_topic, key=key, value=alarm).get(timeout=10)
        log.info("published_alarm", key=key, detector=alarm["detector"])

    def publish_discovery(
        self,
        mesh_id: str,
        device_id: int,
        payload_kind: str,
        fields: list[str],
        field_units: dict[str, str | None] | None = None,
    ) -> None:
        """Register computed channels. Reuses the existing mesh.sensor_discovery.v1
        `field_units` is optional
        """
        if not fields:
            return
        event = {
            "mesh_id": str(mesh_id),
            "device_id": int(device_id),
            "payload_kind": payload_kind,
            "fields": sorted(fields),
            "op": "upsert",
        }
        units = {name: unit for name, unit in (field_units or {}).items() if unit}
        if units:
            event["field_units"] = units
        self._producer.send(self._discovery_topic, key=f"{mesh_id}:{device_id}", value=event).get(timeout=10)
        log.info("published_discovery", payload_kind=payload_kind, fields=len(fields))


_publisher: QualityPublisher | None = None


def get_alarm_publisher() -> QualityPublisher:
    global _publisher
    if _publisher is None:
        _publisher = QualityPublisher(
            bootstrap_servers=os.environ["KAFKA_BOOTSTRAP"],
            alarms_topic=os.environ.get("PATTERN_DETECTION__ALARMS_TOPIC", "mesh.telemetry.alarms.v1"),
            discovery_topic=os.environ.get("KEYAPI__SENSOR_DISCOVERY_KAFKA_TOPIC", "mesh.sensor_discovery.v1"),
        )
    return _publisher
