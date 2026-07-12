import json
import os
import asyncio
import sys
from datetime import datetime

from aiohttp import web
import structlog
from kafka import KafkaConsumer, KafkaProducer
from tools.healthcheck import HealthState, KafkaConsumerHealth
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from meshtastic.protobuf import mqtt_pb2, mesh_pb2
from meshtastic import protocols
from google.protobuf.json_format import MessageToDict
from tools.key import get_key

log = structlog.get_logger()

# TODO Move to DB
KEY = "AQ=="
KEY = "" if KEY == "AQ==" else KEY

class ProtoHandler(object):
    def __init__(self, kafka_bootstrap_server):
        self.kafka_bootstrap_server = kafka_bootstrap_server
        self.health_state = HealthState()
        # Consumer
        self.kafka_consumer = KafkaConsumer(
            bootstrap_servers=self.kafka_bootstrap_server,
            group_id='proto-decoding-group',
            enable_auto_commit=False,
            auto_offset_reset="earliest"

        )
        log.debug(f"Subscribing to kafka topics {[os.environ['MQTT_BRIDGE__KAFKA_TOPIC']]}")
        self.kafka_consumer.subscribe(
            topics=[os.environ['MQTT_BRIDGE__KAFKA_TOPIC']]
        )
        self.consumer_health = KafkaConsumerHealth(
            consumer=self.kafka_consumer,
            topic=os.environ['MQTT_BRIDGE__KAFKA_TOPIC'])
        # Producer
        self.kafka_producer = KafkaProducer(bootstrap_servers=self.kafka_bootstrap_server)
        self.consumer_health = KafkaConsumerHealth(
            consumer=self.kafka_consumer,
            topic=os.environ['MQTT_BRIDGE__KAFKA_TOPIC'])
        # TODO Health
        self.health_state = HealthState()

    def decode_proto(self, msg: bytes):
        se = mqtt_pb2.ServiceEnvelope()
        se.ParseFromString(msg)
        decoded_mp = se.packet
        # TODO Query DB and cache
        key = ""

        # Try to decrypt the payload if it is encrypted
        if decoded_mp.HasField("encrypted") and not decoded_mp.HasField("decoded"):
            decoded_data = self.decrypt_packet(decoded_mp, key)
            if decoded_data is None:
                log.warn("Decryption failed; retaining original encrypted payload")
            else:
                decoded_mp.decoded.CopyFrom(decoded_data)

        # Attempt to process the decrypted or encrypted payload
        portNumInt = decoded_mp.decoded.portnum if decoded_mp.HasField("decoded") else None
        handler = protocols.get(portNumInt) if portNumInt else None

        pb = None
        if handler is not None and handler.protobufFactory is not None:
            pb = handler.protobufFactory()
            pb.ParseFromString(decoded_mp.decoded.payload)
            pb_dict = MessageToDict(pb, preserving_proto_field_name=True)
            # if "time" in pb_dict:
            #     pb_dict["time"] = self.convert_date(pb_dict["time"])
        if pb:
            # Clean and update the payload
            pb_str = str(pb).replace('\n', ' ').replace('\r', ' ').strip()
            decoded_mp.decoded.payload = pb_str.encode("utf-8")

            return {
                "node_id": getattr(decoded_mp, "from"),
                "packet_id": decoded_mp.id,
                "portnum": portNumInt,
                "rx_time": decoded_mp.rx_time,
                "hop_start": decoded_mp.hop_start,
                "payload": pb_dict
            }

        return None

    def decrypt_packet(self, mp, key):

        try:
            key_bytes = base64.b64decode(key.encode('ascii'))

            # Build the nonce from message ID and sender
            nonce_packet_id = getattr(mp, "id").to_bytes(8, "little")
            nonce_from_node = getattr(mp, "from").to_bytes(8, "little")
            nonce = nonce_packet_id + nonce_from_node

            # Decrypt the encrypted payload
            cipher = Cipher(algorithms.AES(key_bytes), modes.CTR(nonce), backend=default_backend())
            decryptor = cipher.decryptor()
            decrypted_bytes = decryptor.update(getattr(mp, "encrypted")) + decryptor.finalize()

            # Parse the decrypted bytes into a Data object
            data = mesh_pb2.Data()
            data.ParseFromString(decrypted_bytes)
            return data

        except Exception as e:
            return None

    async def proto_listener(self):

        while True:
            # log.debug("Getting points")
            messages = self.kafka_consumer.poll(
                max_records=1000,
                timeout_ms=500)

            # We need to leave some time for other health checks
            await asyncio.sleep(0.5)
            if not messages:
                continue

            out_messages = []
            for topic_partition, consumer_records in messages.items():
                try:
                    log.info(f'Got the following messages for topic: {topic_partition.topic}:')
                    for record in consumer_records:
                        try:
                            log.debug("New record")
                            log.debug(f'{record.key} [{record.timestamp}]: {record.value}')

                            dproto = self.decode_proto(record.value)
                            if dproto is not None:
                                out_messages.append(self.decode_proto(record.value))
                        except Exception as e:
                            log.exception(f"Bad message skipped", exc_info=e)
                except Exception as e:
                    log.exception(f"Issue while opening records", exc_info=e)

            if out_messages:
                for message in out_messages:
                    print (message)
                    self.kafka_producer.send(os.environ['PROTO_DECODE__KAFKA_TOPIC'], key=str(message["node_id"]).encode(), value=json.dumps(message).encode())
                # TODO - Check why this sometimes fails
                self.kafka_consumer.commit()
                log.debug("Commit offset")

    async def health_handler(self, request):

        self.health_state.kafka_connected = self.producer_health.is_healthy()
        log.info(f'Health check. Kafka connected: {self.health_state.kafka_connected}')

        if self.health_state.is_healthy():
            return web.json_response({"status": "ok"})
        return web.json_response({"status": "unhealthy"}, status=503)

    async def start_health_server(self):
        app = web.Application()
        app.router.add_get("/health", self.health_handler)
        log.info(f"Starting health server on 0.0.0.0:{int(os.environ['MQTT_HEALTHCHECK_PORT'])}")

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", int(os.environ['MQTT_HEALTHCHECK_PORT']))
        await site.start()
