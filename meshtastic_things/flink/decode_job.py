import argparse
import json
import os
import traceback

import structlog
from dotenv import load_dotenv

from pyflink.common import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream import RuntimeExecutionMode, StreamExecutionEnvironment
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import KeySelector, KeyedBroadcastProcessFunction
from pyflink.datastream.state import MapStateDescriptor
from pyflink.common.serialization import SimpleStringSchema

log = structlog.get_logger()


def _udf_log(message: str) -> None:
    """Never use anything else here except print.

    PyFlink UDF methods crashes otherwise.
    """
    print(message)


def _udf_log_exception(message: str) -> None:
    print(f"{message}\n{traceback.format_exc()}")

# SimpleStringSchema decodes raw bytes with a Charset
# UTF-8 corrupts protobuf packets, so we use ISO-8859-1
RAW_BYTES_CHARSET = "ISO-8859-1"

# Keyed by gateway_id, value {"psk_b64":..., "mesh_id":...}
GATEWAY_KEY_STATE = MapStateDescriptor("gateway-keys", Types.STRING(), Types.STRING())

# Keyed by "mesh_id:device_id", presence means rejected
NODE_REJECTED_STATE = MapStateDescriptor("node-rejections", Types.STRING(), Types.STRING())


def _discovered_fields(payload_kind: str, payload: dict) -> list[str]:
    """telemetry:<variant> only, numeric leaf fields only."""
    if not payload_kind.startswith("telemetry:"):
        return []
    variant = payload_kind.split(":", 1)[1]
    sub = payload.get(variant)
    if not isinstance(sub, dict):
        return []
    return [k for k, v in sub.items() if isinstance(v, (int, float)) and not isinstance(v, bool)]


class PacketKeySelector(KeySelector):
    """Keys the main packet stream by gateway_id so it can be connected to the
    broadcast config stream"""

    def open(self, runtime_context):
        from common import codec

        self._codec = codec

    def get_key(self, value: str) -> str:
        try:
            se = self._codec.parse_envelope(value.encode(RAW_BYTES_CHARSET))
            return se.gateway_id or "unparseable"
        except Exception:
            return "unparseable"


class DecodeWithBroadcastKey(KeyedBroadcastProcessFunction):
    """Decrypt/decode a ServiceEnvelope only if the gateway is registered and the
    node isn't rejected."""

    def open(self, runtime_context):
        from common import codec

        self._codec = codec

    def process_element(self, value: str, ctx):
        gateway_state_map = ctx.get_broadcast_state(GATEWAY_KEY_STATE)
        current_key = ctx.get_current_key()
        if not gateway_state_map.contains(current_key):
            _udf_log(f"Dropping packet from unregistered gateway: gateway_id={current_key}")
            return

        raw = value.encode(RAW_BYTES_CHARSET)
        try:
            gateway_state = json.loads(gateway_state_map.get(current_key))
        except Exception:
            _udf_log_exception(f"Failed to parse gateway state for {current_key}")
            return
        key_b64 = gateway_state.get("psk_b64")
        mesh_id = gateway_state.get("mesh_id")
        key_bytes = self._codec.decode_psk(key_b64) if key_b64 else None

        try:
            node_id = getattr(self._codec.parse_service_envelope(raw), "from")
        except Exception:
            _udf_log_exception("Failed to parse packet header")
            return
        if mesh_id:
            rejected_state = ctx.get_broadcast_state(NODE_REJECTED_STATE)
            if rejected_state.contains(f"{mesh_id}:{node_id}"):
                _udf_log(f"Dropping packet from rejected node: mesh_id={mesh_id} device_id={node_id}")
                return

        try:
            decoded = self._codec.decode_message(raw, key_bytes)
        except Exception:
            _udf_log_exception("Failed to decode packet")
            return
        if decoded is None:
            return
        if mesh_id:
            # Tag with mesh_id
            decoded["mesh_id"] = mesh_id

        yield json.dumps({"kind": "decoded", "payload": decoded})
        if mesh_id:
            # Node is allowed on first sighting
            sighting = {
                "key": f"{mesh_id}:{decoded['node_id']}",
                "mesh_id": mesh_id,
                "device_id": decoded["node_id"],
                "op": "upsert",
            }
            # Opportunistic enrichment.
            payload = decoded["payload"]
            if decoded["payload_kind"] == "position":
                lat_i, lon_i = payload.get("latitude_i"), payload.get("longitude_i")
                if lat_i is not None and lon_i is not None:
                    sighting["latitude"] = lat_i / 1e7
                    sighting["longitude"] = lon_i / 1e7
            elif decoded["payload_kind"] == "nodeinfo":
                for src_field, sighting_key in (
                    ("long_name", "name"),
                    ("short_name", "short_name"),
                    ("hw_model", "hardware_type"),
                    ("role", "role"),
                ):
                    if payload.get(src_field):
                        sighting[sighting_key] = payload[src_field]
            yield json.dumps({"kind": "sighting", "payload": sighting})

            # Sensor/Measurement discovery.
            fields = _discovered_fields(decoded["payload_kind"], payload)
            if fields:
                discovery = {
                    "mesh_id": mesh_id,
                    "device_id": decoded["node_id"],
                    "payload_kind": decoded["payload_kind"],
                    "fields": fields,
                    "op": "upsert",
                }
                yield json.dumps({"kind": "discovery", "payload": discovery})

    def process_broadcast_element(self, value: str, ctx):
        try:
            event = json.loads(value)
        except Exception:
            _udf_log_exception(f"Failed to parse broadcast-config event: raw={value}")
            return
        key = event.get("key")
        if not key:
            return

        if event.get("kind") == "node_rejection":
            rejected_state = ctx.get_broadcast_state(NODE_REJECTED_STATE)
            if event.get("op") == "delete":
                rejected_state.remove(key)
            else:
                rejected_state.put(key, "1")
        else:
            gateway_state_map = ctx.get_broadcast_state(GATEWAY_KEY_STATE)
            if event.get("op") == "delete":
                gateway_state_map.remove(key)
            else:
                state_value = json.dumps({"psk_b64": event.get("psk_b64", ""), "mesh_id": event.get("mesh_id")})
                gateway_state_map.put(key, state_value)
        yield from ()  # this method only changes broadcast state


def build_pipeline(
    env,
    kafka_bootstrap: str,
    raw_topic: str,
    decoded_topic: str,
    keys_topic: str,
    devices_topic: str,
    node_rejections_topic: str,
    sensor_discovery_topic: str,
):
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_topics(raw_topic)
        .set_group_id("flink-decode-group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema(RAW_BYTES_CHARSET))
        .build()
    )
    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "raw-mesh-packets")

    keys_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_topics(keys_topic)
        .set_group_id("flink-decode-keys-group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    keys_stream = env.from_source(keys_source, WatermarkStrategy.no_watermarks(), "gateway-key-config")

    node_rejections_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_topics(node_rejections_topic)
        .set_group_id("flink-decode-node-rejections-group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    node_rejections_stream = env.from_source(
        node_rejections_source, WatermarkStrategy.no_watermarks(), "node-rejection-config"
    )

    keyed_stream = stream.key_by(PacketKeySelector(), key_type=Types.STRING())
    # Both config topics feed one broadcast connection - "kind" routes each
    # event to the right state map in process_broadcast_element.
    broadcast_stream = keys_stream.union(node_rejections_stream).broadcast(GATEWAY_KEY_STATE, NODE_REJECTED_STATE)

    combined_stream = keyed_stream.connect(broadcast_stream).process(
        DecodeWithBroadcastKey(), output_type=Types.STRING()
    )

    decoded_stream = combined_stream.filter(lambda v: json.loads(v)["kind"] == "decoded").map(
        lambda v: json.dumps(json.loads(v)["payload"]), output_type=Types.STRING()
    )
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(decoded_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    decoded_stream.sink_to(sink)

    device_sightings_stream = combined_stream.filter(lambda v: json.loads(v)["kind"] == "sighting").map(
        lambda v: json.dumps(json.loads(v)["payload"]), output_type=Types.STRING()
    )
    devices_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(devices_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    device_sightings_stream.sink_to(devices_sink)

    sensor_discovery_stream = combined_stream.filter(lambda v: json.loads(v)["kind"] == "discovery").map(
        lambda v: json.dumps(json.loads(v)["payload"]), output_type=Types.STRING()
    )
    sensor_discovery_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(sensor_discovery_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    sensor_discovery_stream.sink_to(sensor_discovery_sink)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--local", default=False, dest="local", action="store_true", help="Local development mode"
    )
    args = parser.parse_args()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    if args.local:
        log.warning("Local development mode")
        load_dotenv("../.env")
        kafka_bootstrap = os.environ["KAFKA_BOOTSTRAP_LOCAL"]
        src_dir = os.path.dirname(os.path.realpath(__file__))
        jar_files = [
            "flink-connector-kafka-3.3.0-1.19.jar",
            "flink-sql-connector-kafka-3.3.0-1.19.jar",
            "kafka-clients-3.2.3.jar",
        ]
        jar_paths = tuple(f"file://{os.path.join(src_dir, 'jars', name)}" for name in jar_files)
        env.add_jars(*jar_paths)
    else:
        kafka_bootstrap = os.environ["KAFKA_BOOTSTRAP"]

    log.info("Starting Flink decode job")

    build_pipeline(
        env,
        kafka_bootstrap=kafka_bootstrap,
        raw_topic=os.environ["MQTT_BRIDGE__KAFKA_TOPIC"],
        decoded_topic=os.environ["PROTO_DECODE__KAFKA_TOPIC"],
        keys_topic=os.environ.get("KEYAPI__KAFKA_TOPIC", "mesh.keys.v1"),
        devices_topic=os.environ.get("KEYAPI__DEVICES_KAFKA_TOPIC", "mesh.devices.v1"),
        node_rejections_topic=os.environ.get("KEYAPI__NODE_REJECTIONS_KAFKA_TOPIC", "mesh.node_rejections.v1"),
        sensor_discovery_topic=os.environ.get(
            "KEYAPI__SENSOR_DISCOVERY_KAFKA_TOPIC", "mesh.sensor_discovery.v1"
        ),
    )

    env.execute("mesh-decode-decrypt")


if __name__ == "__main__":
    main()
