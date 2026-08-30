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
# Meshtastic packets are protobuf, not UTF-8 text, so UTF-8 here would corrupt them
RAW_BYTES_CHARSET = "ISO-8859-1"

# Dict with "channel_id:device_id"
# mesh.keys.v1
# Being in this map is what authorizes decode: unregistered devices get dropped,
# encrypted or not.
DEVICE_KEY_STATE = MapStateDescriptor("device-keys", Types.STRING(), Types.STRING())

class PacketKeySelector(KeySelector):
    """Keys the main packet stream by channel_id:device_id so it can be connected to
    the broadcast config stream. get_key has to (re-)parse the envelope since
    KeySelector runs before of any other operator on this stream."""

    def open(self, runtime_context):
        from common import codec

        self._codec = codec

    def get_key(self, value: str) -> str:
        try:
            se = self._codec.parse_envelope(value.encode(RAW_BYTES_CHARSET))
            return self._codec.packet_key(se)
        except Exception:
            return "unparseable"


class DecodeWithBroadcastKey(KeyedBroadcastProcessFunction):
    """Decrypt and decode a raw ServiceEnvelope, but only for registered devices.

    The registration check runs before decode is even attempted.
    Unregistered devices always dropped, encrypted or not.
    """

    def open(self, runtime_context):
        from common import codec

        self._codec = codec

    def process_element(self, value: str, ctx):
        broadcast_state = ctx.get_broadcast_state(DEVICE_KEY_STATE)
        current_key = ctx.get_current_key()
        if not broadcast_state.contains(current_key):
            _udf_log(f"Dropping packet from unregistered device: key={current_key}")
            return

        raw = value.encode(RAW_BYTES_CHARSET)
        key_b64 = broadcast_state.get(current_key)
        key_bytes = self._codec.decode_psk(key_b64) if key_b64 else None

        try:
            decoded = self._codec.decode_message(raw, key_bytes)
        except Exception:
            _udf_log_exception("Failed to decode packet")
            return
        if decoded is not None:
            yield json.dumps(decoded)

    def process_broadcast_element(self, value: str, ctx):

        broadcast_state = ctx.get_broadcast_state(DEVICE_KEY_STATE)
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
            broadcast_state.put(key, event.get("psk_b64", ""))
        yield from ()  # this method only changes broadcast state, never emits output


def build_pipeline(env, kafka_bootstrap: str, raw_topic: str, decoded_topic: str, keys_topic: str):
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
    keys_stream = env.from_source(keys_source, WatermarkStrategy.no_watermarks(), "device-key-config")

    keyed_stream = stream.key_by(PacketKeySelector(), key_type=Types.STRING())
    broadcast_stream = keys_stream.broadcast(DEVICE_KEY_STATE)

    decoded_stream = keyed_stream.connect(broadcast_stream).process(
        DecodeWithBroadcastKey(), output_type=Types.STRING()
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
    )

    env.execute("mesh-decode-decrypt")


if __name__ == "__main__":
    main()
