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


def _udf_log_exception(message: str) -> None:

    print(f"{message}\n{traceback.format_exc()}")


# Tagged by "channel_id:device_id:payload_kind:field" -> JSON {"function_name", "params"}.
# Populated from the mesh.calibration.v1 topic.
CALIBRATION_STATE = MapStateDescriptor("calibration-config", Types.STRING(), Types.STRING())


class CalibrationKeySelector(KeySelector):
    """Keys the decoded-message stream by channel_id:device_id, matching the prefix
    used for calibration config lookups (see ApplyCalibration.process_element)."""

    def get_key(self, value: str) -> str:
        try:
            event = json.loads(value)
            return f"{event.get('channel_id', '')}:{event.get('node_id', '')}"
        except Exception:
            return "unparseable"


class ApplyCalibration(KeyedBroadcastProcessFunction):
    """Applies a configured calibration function to every numeric leaf field in a
    decoded message's payload.

    Currently unused.
    """

    def open(self, runtime_context):
        from common import calibration

        self._calibration = calibration

    def process_element(self, value: str, ctx):
        try:
            event = json.loads(value)
        except Exception:
            _udf_log_exception("Bad decoded message, skipping")
            return

        broadcast_state = ctx.get_broadcast_state(CALIBRATION_STATE)
        prefix = ctx.get_current_key()
        payload_kind = event.get("payload_kind", "")
        self._calibrate(event.get("payload") or {}, payload_kind, prefix, broadcast_state)

        yield json.dumps(event)

    def _calibrate(self, payload: dict, payload_kind: str, prefix: str, broadcast_state) -> None:
        for key, val in list(payload.items()):
            if isinstance(val, dict):
                self._calibrate(val, payload_kind, prefix, broadcast_state)
            elif isinstance(val, (int, float)) and not isinstance(val, bool):
                config_key = f"{prefix}:{payload_kind}:{key}"
                raw = broadcast_state.get(config_key)
                if raw:
                    cfg = json.loads(raw)
                    payload[key] = self._calibration.apply_calibration(
                        cfg.get("function_name", "identity"), val, cfg.get("params")
                    )

    def process_broadcast_element(self, value: str, ctx):
        broadcast_state = ctx.get_broadcast_state(CALIBRATION_STATE)
        try:
            event = json.loads(value)
        except Exception:
            _udf_log_exception("Bad calibration config event")
            return
        key = event.get("key")
        if not key:
            return
        if event.get("op") == "delete":
            broadcast_state.remove(key)
        else:
            broadcast_state.put(
                key,
                json.dumps({"function_name": event.get("function_name", "identity"), "params": event.get("params", {})}),
            )
        yield from ()  # this method only mutates broadcast state


def build_pipeline(env, kafka_bootstrap: str, decoded_topic: str, calibrated_topic: str, calibration_topic: str):
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_topics(decoded_topic)
        .set_group_id("flink-calibration-group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "decoded-messages")

    calib_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_topics(calibration_topic)
        .set_group_id("flink-calibration-config-group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    calib_stream = env.from_source(calib_source, WatermarkStrategy.no_watermarks(), "calibration-config")

    keyed_stream = stream.key_by(CalibrationKeySelector(), key_type=Types.STRING())
    broadcast_stream = calib_stream.broadcast(CALIBRATION_STATE)

    calibrated_stream = keyed_stream.connect(broadcast_stream).process(
        ApplyCalibration(), output_type=Types.STRING()
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(calibrated_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    calibrated_stream.sink_to(sink)


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

    log.info("Starting Flink calibration job")

    build_pipeline(
        env,
        kafka_bootstrap=kafka_bootstrap,
        decoded_topic=os.environ["PROTO_DECODE__KAFKA_TOPIC"],
        calibrated_topic=os.environ.get("CALIBRATED__KAFKA_TOPIC", "mesh.telemetry.calibrated.v1"),
        calibration_topic=os.environ.get("CALIBRATION__KAFKA_TOPIC", "mesh.calibration.v1"),
    )

    env.execute("mesh-calibration")


if __name__ == "__main__":
    main()
