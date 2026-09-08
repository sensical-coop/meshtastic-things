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

    PyFlink UDF methods crashes if we use any log.*()
    call inside a UDF method with a TypeError.
    """
    print(message)


def _udf_log_exception(message: str) -> None:
    print(f"{message}\n{traceback.format_exc()}")

# SimpleStringSchema decodes the raw byte[] through a Charset before it reaches Python.
# Meshtastic packets are protobuf, not UTF-8 text, so UTF-8 here corrupts them
RAW_BYTES_CHARSET = "ISO-8859-1"

# Tagged by gateway_id (e.g. "!aabbccdd"), instead of by the originating device -
# every device on a mesh shares one PSK, so a registered gateway is what authorizes
# decode for the whole mesh behind it.
# Value is: a JSON string {"psk_b64": ..., "mesh_id": ...}
# Populated from mesh.keys.v1 topic.
# Unregistered gateways get dropped, encrypted or not.
GATEWAY_KEY_STATE = MapStateDescriptor("gateway-keys", Types.STRING(), Types.STRING())


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
    """Decrypt and decode a raw ServiceEnvelope, but only if it came through a
    registered gateway.

    The registration check runs before decode is even attempted. Packets relayed
    by an unregistered gateway are always dropped, encrypted or not.
    """

    def open(self, runtime_context):
        from common import codec

        self._codec = codec

    def process_element(self, value: str, ctx):
        broadcast_state = ctx.get_broadcast_state(GATEWAY_KEY_STATE)
        current_key = ctx.get_current_key()
        if not broadcast_state.contains(current_key):
            _udf_log(f"Dropping packet from unregistered gateway: gateway_id={current_key}")
            return

        raw = value.encode(RAW_BYTES_CHARSET)
        try:
            gateway_state = json.loads(broadcast_state.get(current_key))
        except Exception:
            _udf_log_exception(f"Failed to parse gateway state for {current_key}")
            return
        key_b64 = gateway_state.get("psk_b64")
        mesh_id = gateway_state.get("mesh_id")
        key_bytes = self._codec.decode_psk(key_b64) if key_b64 else None

        try:
            decoded = self._codec.decode_message(raw, key_bytes)
        except Exception:
            _udf_log_exception("Failed to decode packet")
            return
        if decoded is None:
            return

        yield json.dumps({"kind": "decoded", "payload": decoded})
        if mesh_id:
            sighting = {
                "key": f"{mesh_id}:{decoded['node_id']}",
                "mesh_id": mesh_id,
                "device_id": decoded["node_id"],
                "op": "upsert",
            }
            yield json.dumps({"kind": "sighting", "payload": sighting})

    def process_broadcast_element(self, value: str, ctx):

        broadcast_state = ctx.get_broadcast_state(GATEWAY_KEY_STATE)
        try:
            event = json.loads(value)
        except Exception:
            _udf_log_exception(f"Failed to parse key-config event: raw={value}")
            return
        key = event.get("key")
        if not key:
            return
        if event.get("op") == "delete":
            broadcast_state.remove(key)
        else:
            state_value = json.dumps({"psk_b64": event.get("psk_b64", ""), "mesh_id": event.get("mesh_id")})
            broadcast_state.put(key, state_value)
        yield from ()  # this method only changes broadcast state


def build_pipeline(
    env, kafka_bootstrap: str, raw_topic: str, decoded_topic: str, keys_topic: str, devices_topic: str
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

    keyed_stream = stream.key_by(PacketKeySelector(), key_type=Types.STRING())
    broadcast_stream = keys_stream.broadcast(GATEWAY_KEY_STATE)

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
    )

    env.execute("mesh-decode-decrypt")


if __name__ == "__main__":
    main()
