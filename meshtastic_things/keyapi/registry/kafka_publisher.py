import json
import os
from datetime import datetime, timezone

import structlog
from kafka import KafkaProducer

# Definition of the "<measurement>.<field>"
from common.channels import channel_name

from .models import Device, Mesh

log = structlog.get_logger()


def gateway_kafka_key(device: Device) -> str:
    """Meshtastic's node number in string form, e.g. 0xaabbccdd -> "!aabbccdd".
    This matches ServiceEnvelope.gateway_id"""
    return f"!{device.device_id:08x}"


def node_rejection_kafka_key(device: Device) -> str:
    """Compound mesh_id:device_id key - matches decode_job.py's
    NODE_REJECTED_STATE key."""
    return f"{device.mesh_id}:{device.device_id}"


def device_intervals_kafka_key(device: Device) -> str:
    return f"{device.mesh_id}:{device.device_id}"


class KeyEventPublisher:
    """Publishes to mesh.keys.v1, mesh.node_rejections.v1,
    mesh.owner_auth_revocations.v1 and mesh.quality_config.v1."""

    def __init__(
        self,
        bootstrap_servers: str,
        keys_topic: str,
        node_rejections_topic: str,
        owner_auth_revocations_topic: str,
        quality_config_topic: str,
    ):
        self._keys_topic = keys_topic
        self._node_rejections_topic = node_rejections_topic
        self._owner_auth_revocations_topic = owner_auth_revocations_topic
        self._quality_config_topic = quality_config_topic
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
        )

    def publish_gateway_upsert(self, device: Device, mesh: Mesh) -> None:
        """psk_b64/channel_id come from the mesh"""
        key = gateway_kafka_key(device)
        event = {
            "kind": "gateway_key",
            "key": key,
            "gateway_id": key,
            "device_id": device.device_id,
            "mesh_id": str(device.mesh_id),
            "channel_id": mesh.channel_id,
            "psk_b64": mesh.psk_b64,
            "owner_id": str(mesh.owner_id),
            "op": "upsert",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        future = self._producer.send(self._keys_topic, key=key, value=event)
        future.get(timeout=10)
        log.info("Published gateway upsert", key=key)

    def publish_gateway_delete(self, device: Device) -> None:
        key = gateway_kafka_key(device)
        event = {
            "kind": "gateway_key",
            "key": key,
            "gateway_id": key,
            "device_id": device.device_id,
            "mesh_id": str(device.mesh_id),
            "op": "delete",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        future = self._producer.send(self._keys_topic, key=key, value=event)
        future.get(timeout=10)
        log.info("Published gateway delete", key=key)

    def publish_node_reject(self, device: Device) -> None:
        """Adds device_id to the mesh's reject-list broadcast state to
        stops attempting to decrypt this device's future packets."""
        key = node_rejection_kafka_key(device)
        event = {
            "kind": "node_rejection",
            "key": key,
            "mesh_id": str(device.mesh_id),
            "device_id": device.device_id,
            "op": "upsert",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        future = self._producer.send(self._node_rejections_topic, key=key, value=event)
        future.get(timeout=10)
        log.info("Published node rejection", key=key)

    def publish_node_unreject(self, device: Device) -> None:
        """Removes device_id from the reject-list"""
        key = node_rejection_kafka_key(device)
        event = {
            "kind": "node_rejection",
            "key": key,
            "mesh_id": str(device.mesh_id),
            "device_id": device.device_id,
            "op": "delete",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        future = self._producer.send(self._node_rejections_topic, key=key, value=event)
        future.get(timeout=10)
        log.info("Published node unreject", key=key)

    def publish_key_revoked(self, api_key_hash: str) -> None:
        """Tells queryapi to drop this key immediately"""
        event = {
            "kind": "owner_auth_revocation",
            "api_key_hash": api_key_hash,
            "op": "revoke",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        future = self._producer.send(self._owner_auth_revocations_topic, key=api_key_hash, value=event)
        future.get(timeout=10)
        log.info("Published owner auth revocation")

    def publish_variant_measurement_config(self, link) -> None:
        """Publish plausibility limits and expected reading
        interval for a variant"""
        key = channel_name(link.measurement_type.payload_kind, link.measurement_type.field_name)
        event = {
            "kind": "variant_config",
            "key": key,
            "payload_kind": link.measurement_type.payload_kind,
            "field_name": link.measurement_type.field_name,
            "default_reading_interval_seconds": link.telemetry_variant.default_reading_interval_seconds,
            "valid_min": link.valid_min,
            "valid_max": link.valid_max,
            "op": "upsert",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        future = self._producer.send(self._quality_config_topic, key=key, value=event)
        future.get(timeout=10)
        log.info("Published variant measurement config", key=key)

    def publish_device_intervals(self, device: Device) -> None:
        """Publish device's own reading/publish intervals."""
        key = device_intervals_kafka_key(device)
        cleared = device.reading_interval_seconds is None and device.publish_interval_seconds is None
        event = {
            "kind": "device_intervals",
            "key": key,
            "mesh_id": str(device.mesh_id),
            "device_id": device.device_id,
            "reading_interval_seconds": device.reading_interval_seconds,
            "publish_interval_seconds": device.publish_interval_seconds,
            "op": "delete" if cleared else "upsert",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        future = self._producer.send(self._quality_config_topic, key=key, value=event)
        future.get(timeout=10)
        log.info("Published device intervals", key=key)

    def close(self) -> None:
        self._producer.flush()
        self._producer.close()


_publisher: KeyEventPublisher | None = None


def get_publisher() -> KeyEventPublisher:
    """For gunicorn workers."""
    global _publisher
    if _publisher is None:
        _publisher = KeyEventPublisher(
            bootstrap_servers=os.environ["KAFKA_BOOTSTRAP"],
            keys_topic=os.environ.get("KEYAPI__KAFKA_TOPIC", "mesh.keys.v1"),
            node_rejections_topic=os.environ.get(
                "KEYAPI__NODE_REJECTIONS_KAFKA_TOPIC", "mesh.node_rejections.v1"
            ),
            owner_auth_revocations_topic=os.environ.get(
                "KEYAPI__OWNER_AUTH_REVOCATIONS_KAFKA_TOPIC", "mesh.owner_auth_revocations.v1"
            ),
            quality_config_topic=os.environ.get("KEYAPI__QUALITY_CONFIG_KAFKA_TOPIC", "mesh.quality_config.v1"),
        )
    return _publisher
