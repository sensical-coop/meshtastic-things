import os
import base64
import structlog
import argparse
from dotenv import load_dotenv

# from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
# from cryptography.hazmat.backends import default_backend
# from meshtastic.protobuf import mqtt_pb2, mesh_pb2
# from meshtastic import protocols
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.functions import MapFunction #, KeyedProcessFunction
from pyflink.common.typeinfo import Types
from pyflink.common.serialization import SimpleStringSchema, SerializationSchema
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.common import WatermarkStrategy

log = structlog.get_logger()

# TODO Move to DB
KEY = "AQ=="
KEY = "" if KEY == "AQ==" else KEY

class ByteSerializationSchema(SerializationSchema):

    def serialize(self, value):
        return value

class DecodeProtobuf(MapFunction):

    def open(self, runtime_context):
        import base64
        from meshtastic.protobuf import mqtt_pb2, mesh_pb2
        from meshtastic import protocols
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
        from cryptography.hazmat.backends import default_backend

        self.base64 = base64
        self.mqtt_pb2 = mqtt_pb2
        self.mesh_pb2 = mesh_pb2
        self.protocols = protocols
        self.Cipher = Cipher
        self.algorithms = algorithms
        self.modes = modes
        self.default_backend = default_backend

        # decode key once TODO
        if KEY:
            self.key_bytes = base64.b64decode(KEY.encode("ascii"))
        else:
            self.key_bytes = None

    def decrypt_packet(self, mp):

        if not self.key_bytes:
            return None
        try:
            key_bytes = self.base64.b64decode(key.encode('ascii'))

            # Build the nonce from message ID and sender
            nonce_packet_id = getattr(mp, "id").to_bytes(8, "little")
            nonce_from_node = getattr(mp, "from").to_bytes(8, "little")
            nonce = nonce_packet_id + nonce_from_node

            # Decrypt the encrypted payload
            cipher = self.Cipher(self.algorithms.AES(key_bytes), self.modes.CTR(nonce), backend=default_backend())
            decryptor = cipher.decryptor()
            decrypted_bytes = decryptor.update(getattr(mp, "encrypted")) + decryptor.finalize()

            # Parse the decrypted bytes into a Data object
            data = self.mesh_pb2.Data()
            data.ParseFromString(decrypted_bytes)
            return data

        except Exception as e:
            return None

    def map(self, msg: bytes):

        se = self.mqtt_pb2.ServiceEnvelope()
        # se.ParseFromString(msg.payload)
        se.ParseFromString(msg)
        decoded_mp = se.packet
        log.info("Decoded MP")
        log.info(decoded_mp)

        # Try to decrypt the payload if it is encrypted
        if decoded_mp.HasField("encrypted") and not decoded_mp.HasField("decoded"):
            decoded_data = self.decrypt_packet(decoded_mp)
            if decoded_data is None:
                log.warn("Decryption failed; retaining original encrypted payload")
            else:
                decoded_mp.decoded.CopyFrom(decoded_data)

        # Attempt to process the decrypted or encrypted payload
        portNumInt = decoded_mp.decoded.portnum if decoded_mp.HasField("decoded") else None
        handler = self.protocols.get(portNumInt, None) if portNumInt else None

        pb = None
        if handler is not None and handler.protobufFactory is not None:
            pb = handler.protobufFactory()
            pb.ParseFromString(decoded_mp.decoded.payload)
        if pb:
            # Clean and update the payload
            pb_str = str(pb).replace('\n', ' ').replace('\r', ' ').strip()
            decoded_mp.decoded.payload = pb_str.encode("utf-8")

        return decoded_mp

parser = argparse.ArgumentParser()

parser.add_argument(
    "--local", default=False, dest='local', action='store_true', help="Local development mode"
)

args = parser.parse_args()

env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

if args.local:
    log.warn('Local development mode')
    load_dotenv("../.env")
    SRC_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    jar_files = ["flink-connector-kafka-3.3.0-1.19.jar", "flink-sql-connector-kafka-3.3.0-1.19.jar", "kafka-clients-3.2.3.jar"]
    jar_paths = tuple([f"file://{os.path.join(SRC_DIR, 'jars', name)}" for name in jar_files])
    log.info(jar_paths)
    env.add_jars(*jar_paths)
# env.enable_checkpointing(10000) # Enable checkpointing every 10s
else:
    env.add_jars(
        "file:///opt/flink/lib/flink-connector-kafka-3.3.0-1.19.jar",
        "file:///opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.19.jar",
    )

log.info("Starting Flink Job")

source = KafkaSource.builder() \
    .set_bootstrap_servers(os.environ['KAFKA_BOOTSTRAP']) \
    .set_topics(os.environ['MQTT_KAFKA_BRIDGE_TOPIC']) \
    .set_group_id("flink-consumer") \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

stream = env.from_source(
    source,
    WatermarkStrategy.no_watermarks(),
    "Kafka Source"
)

# Deserialize protobuf
protobuf_stream = stream.map(DecodeProtobuf(),
    output_type=Types.PICKLED_BYTE_ARRAY()
)

# ---- Kafka Sink ----
# sink = KafkaSink.builder() \
#     .set_bootstrap_servers(os.environ['KAFKA_BOOTSTRAP']) \
#     .set_record_serializer(
#         KafkaSink.record_serializer_builder()
#         .set_topic(os.environ['KAFKA_DECODED_TOPIC'])
#         .set_value_serialization_schema(SimpleStringSchema())
#         .build()
#     ) \
#     .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
#     .build()

sink = KafkaSink.builder() \
    .set_bootstrap_servers(os.environ['KAFKA_BOOTSTRAP']) \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
        .set_topic(os.environ['KAFKA_DECODED_TOPIC'])
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    ) \
    .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
    .build()

protobuf_stream.sink_to(sink)

env.execute("Kafka to Influx Throttled")