import argparse
import datetime
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
from pyflink.datastream.functions import FlatMapFunction, KeySelector, KeyedBroadcastProcessFunction
from pyflink.datastream.state import ListStateDescriptor, MapStateDescriptor
from pyflink.common.serialization import SimpleStringSchema

log = structlog.get_logger()


def _udf_log_exception(message: str) -> None:

    print(f"{message}\n{traceback.format_exc()}")


# Keyed by "node_id:field" -> JSON {"detector_name", "params"}. Populated from the
# mesh.rules.v1 broadcast stream.
RULES_STATE = MapStateDescriptor("detector-rules", Types.STRING(), Types.STRING())
RECENT_VALUES_WINDOW = 5


class ExplodeFields(FlatMapFunction):
    """Flattens a decoded/calibrated message"""

    def flat_map(self, value: str):
        try:
            event = json.loads(value)
        except Exception:
            _udf_log_exception("Bad message, skipping")
            return
        node_id = event.get("node_id")
        rx_time = event.get("rx_time")
        for field, val in self._walk(event.get("payload") or {}):
            yield json.dumps({"node_id": node_id, "field": field, "value": val, "time": rx_time})

    def _walk(self, d: dict):
        for key, val in d.items():
            if isinstance(val, dict):
                yield from self._walk(val)
            elif isinstance(val, (int, float)) and not isinstance(val, bool):
                yield key, val


class ObservationKeySelector(KeySelector):
    def get_key(self, value: str) -> str:
        try:
            obs = json.loads(value)
            return f"{obs.get('node_id')}:{obs.get('field')}"
        except Exception:
            return "unparseable"


class DetectPatterns(KeyedBroadcastProcessFunction):
    """Runs custom detectors (see common/detectors.py) per (node_id, field) keyed
    stream.
    """

    def open(self, runtime_context):
        from common import detectors

        self._detectors = detectors
        self._recent_values = runtime_context.get_list_state(
            ListStateDescriptor("recent-values", Types.DOUBLE())
        )

    def process_element(self, value: str, ctx):
        try:
            obs = json.loads(value)
        except Exception:
            _udf_log_exception("Bad observation, skipping")
            return

        node_id = obs.get("node_id")
        field = obs.get("field")
        obs_value = obs.get("value")

        recent = list(self._recent_values.get() or [])
        recent.append(float(obs_value))
        recent = recent[-RECENT_VALUES_WINDOW:]
        self._recent_values.update(recent)

        if self._detectors.stuck_value_triggered(recent, window=RECENT_VALUES_WINDOW):
            yield self._alarm(
                node_id, field, obs_value, "stuck_value",
                f"Value stuck at {obs_value} for {RECENT_VALUES_WINDOW} consecutive readings",
            )

        broadcast_state = ctx.get_broadcast_state(RULES_STATE)
        rule_raw = broadcast_state.get(ctx.get_current_key())
        if rule_raw:
            rule = json.loads(rule_raw)
            if rule.get("detector_name") == "range_check":
                params = rule.get("params", {})
                if self._detectors.range_check_triggered(obs_value, params.get("min"), params.get("max")):
                    yield self._alarm(
                        node_id, field, obs_value, "range_check",
                        f"Value {obs_value} outside configured range [{params.get('min')}, {params.get('max')}]",
                    )

    def process_broadcast_element(self, value: str, ctx):
        broadcast_state = ctx.get_broadcast_state(RULES_STATE)
        try:
            event = json.loads(value)
        except Exception:
            _udf_log_exception("Bad rule event")
            return
        key = event.get("key")
        if not key:
            return
        if event.get("op") == "delete":
            broadcast_state.remove(key)
        else:
            broadcast_state.put(
                key,
                json.dumps({"detector_name": event.get("detector_name", "range_check"), "params": event.get("params", {})}),
            )
        yield from ()  # this method only mutates broadcast state, never emits output

    @staticmethod
    def _alarm(node_id, field, value, detector, message) -> str:
        return json.dumps(
            {
                "node_id": node_id,
                "field": field,
                "detector": detector,
                "triggered_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "value": value,
                "message": message,
            }
        )


def build_pipeline(env, kafka_bootstrap: str, input_topic: str, alarms_topic: str, rules_topic: str):
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_topics(input_topic)
        .set_group_id("flink-pattern-detection-group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "calibrated-or-decoded-messages")
    exploded = stream.flat_map(ExplodeFields(), output_type=Types.STRING())

    rules_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_topics(rules_topic)
        .set_group_id("flink-pattern-detection-rules-group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    rules_stream = env.from_source(rules_source, WatermarkStrategy.no_watermarks(), "detector-rules")

    keyed_stream = exploded.key_by(ObservationKeySelector(), key_type=Types.STRING())
    broadcast_stream = rules_stream.broadcast(RULES_STATE)

    alarms_stream = keyed_stream.connect(broadcast_stream).process(DetectPatterns(), output_type=Types.STRING())

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(alarms_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    alarms_stream.sink_to(sink)


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

    log.info("Starting Flink pattern-detection job")

    build_pipeline(
        env,
        kafka_bootstrap=kafka_bootstrap,
        input_topic=os.environ.get(
            "PATTERN_DETECTION__INPUT_TOPIC",
            os.environ.get("CALIBRATED__KAFKA_TOPIC", "mesh.telemetry.calibrated.v1"),
        ),
        alarms_topic=os.environ.get("PATTERN_DETECTION__ALARMS_TOPIC", "mesh.telemetry.alarms.v1"),
        rules_topic=os.environ.get("PATTERN_DETECTION__RULES_TOPIC", "mesh.rules.v1"),
    )

    env.execute("mesh-pattern-detection")


if __name__ == "__main__":
    main()
