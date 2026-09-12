import argparse
import asyncio
import json
import os
import sys
import time

import structlog
from aiohttp import web
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer

from common import decode
from tools.healthcheck import KafkaConsumerHealth

log = structlog.get_logger()


class _ProducerHealth:
    """Metadata-only producer health check."""

    def __init__(self, producer, topic):
        self._producer = producer
        self._topic = topic
        self.last_ok = 0

    async def monitor(self):
        while True:
            try:
                self._producer.partitions_for(self._topic)
                self.last_ok = time.time()
            except Exception as e:
                log.exception("Kafka producer monitor", exc_info=e)
            await asyncio.sleep(10)

    def is_healthy(self):
        return (time.time() - self.last_ok) < 30


class DecodeConsumer(object):
    """Lite decode"""

    def __init__(self, kafka_bootstrap_server):
        self.kafka_bootstrap_server = kafka_bootstrap_server

        self.raw_topic = os.environ['MQTT_BRIDGE__KAFKA_TOPIC']
        self.raw_consumer = KafkaConsumer(
            bootstrap_servers=kafka_bootstrap_server,
            group_id='decode-consumer-group',
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        self.raw_consumer.subscribe(topics=[self.raw_topic])

        self.keys_topic = os.environ.get('KEYAPI__KAFKA_TOPIC', 'mesh.keys.v1')
        self.node_rejections_topic = os.environ.get(
            'KEYAPI__NODE_REJECTIONS_KAFKA_TOPIC', 'mesh.node_rejections.v1'
        )
        self.config_consumer = KafkaConsumer(
            bootstrap_servers=kafka_bootstrap_server,
            group_id='decode-consumer-config-group',
            enable_auto_commit=True,
            auto_offset_reset="earliest",
        )
        self.config_consumer.subscribe(topics=[self.keys_topic, self.node_rejections_topic])

        self.decoded_topic = os.environ['PROTO_DECODE__KAFKA_TOPIC']
        self.devices_topic = os.environ.get('KEYAPI__DEVICES_KAFKA_TOPIC', 'mesh.devices.v1')
        self.sensor_discovery_topic = os.environ.get(
            'KEYAPI__SENSOR_DISCOVERY_KAFKA_TOPIC', 'mesh.sensor_discovery.v1'
        )
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_server,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )

        # gateway_id -> {"psk_b64", "mesh_id"}; presence in the set means rejected.
        self.gateway_state = {}
        self.rejected_nodes = set()

        self.raw_consumer_health = KafkaConsumerHealth(self.raw_consumer, topic=self.raw_topic)
        self.config_consumer_health = KafkaConsumerHealth(self.config_consumer, topic=self.keys_topic)
        self.producer_health = _ProducerHealth(self.producer, topic=self.decoded_topic)

    async def consume_config(self):
        """Same as decode_job.py's process_broadcast_element. Keeps
        gateway_state/rejected_nodes updated from the same two kafka
        topics keyapi publishes to."""
        while True:
            messages = self.config_consumer.poll(timeout_ms=1000)
            for topic_partition, records in messages.items():
                for record in records:
                    try:
                        self._apply_config_event(topic_partition.topic, record.value)
                    except Exception as e:
                        log.exception("Bad config message skipped", exc_info=e)
            await asyncio.sleep(0.1)

    def _apply_config_event(self, topic, value):
        event = json.loads(value)
        key = event.get("key")
        if not key:
            return
        if topic == self.node_rejections_topic:
            if event.get("op") == "delete":
                self.rejected_nodes.discard(key)
            else:
                self.rejected_nodes.add(key)
        else:
            if event.get("op") == "delete":
                self.gateway_state.pop(key, None)
            else:
                self.gateway_state[key] = {"psk_b64": event.get("psk_b64", ""), "mesh_id": event.get("mesh_id")}

    async def consume_decode(self):
        while True:
            messages = self.raw_consumer.poll(
                max_records=int(os.environ.get('DECODE_CONSUMER__CONSUMER__N_MSGS', 100)),
                timeout_ms=int(os.environ.get('DECODE_CONSUMER__CONSUMER__TIMEOUT_MS', 1000)),
            )
            await asyncio.sleep(0.1)
            if not messages:
                continue

            commit_ok = True
            for topic_partition, records in messages.items():
                for record in records:
                    try:
                        events = decode.decode_packet(
                            record.value,
                            self.gateway_state,
                            self.rejected_nodes,
                            on_drop=lambda reason: log.debug("Dropping packet", reason=reason),
                        )
                    except Exception as e:
                        # Not retryable - a malformed record won't decode differently next time.
                        log.exception("Failed to decode packet", exc_info=e)
                        continue
                    if events is None:
                        continue
                    try:
                        self._publish(events)
                    except Exception as e:
                        log.exception("Failed to publish decoded events, retrying next poll", exc_info=e)
                        commit_ok = False

            if commit_ok:
                self.raw_consumer.commit()

    def _publish(self, events):
        self.producer.send(self.decoded_topic, value=events["decoded"]).get(timeout=10)
        if "sighting" in events:
            self.producer.send(self.devices_topic, value=events["sighting"]).get(timeout=10)
        if "discovery" in events:
            self.producer.send(self.sensor_discovery_topic, value=events["discovery"]).get(timeout=10)

    async def health_handler(self, request):
        healthy = (
            self.raw_consumer_health.is_healthy()
            and self.config_consumer_health.is_healthy()
            and self.producer_health.is_healthy()
        )
        if healthy:
            return web.json_response({"status": "ok"})
        return web.json_response({"status": "unhealthy"}, status=503)

    async def start_health_server(self):
        port = int(os.environ['DECODE_CONSUMER__HEALTHCHECK_PORT'])
        log.info(f"Starting health server on 0.0.0.0:{port}")
        app = web.Application()
        app.router.add_get("/health", self.health_handler)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()


async def create_tasks(consumer):
    log.info('Creating decode consumer tasks...')
    async with asyncio.TaskGroup() as tg:
        tg.create_task(consumer.consume_config())
        tg.create_task(consumer.consume_decode())
        tg.create_task(consumer.start_health_server())
        tg.create_task(consumer.raw_consumer_health.monitor())
        tg.create_task(consumer.config_consumer_health.monitor())
        tg.create_task(consumer.producer_health.monitor())

        log.info("Looping forever...")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--local", default=False, dest='local', action='store_true', help="Local development mode"
    )
    args = parser.parse_args()

    if args.local:
        load_dotenv("../.env")
        kafka_bootstrap_server = os.environ['KAFKA_BOOTSTRAP_LOCAL']
        log.warn('Local development mode')
    else:
        kafka_bootstrap_server = os.environ['KAFKA_BOOTSTRAP']

    consumer = DecodeConsumer(kafka_bootstrap_server=kafka_bootstrap_server)

    log.info("Starting decode consumer...")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(create_tasks(consumer))
    except KeyboardInterrupt:
        log.info("Program interrupted by user")
        sys.exit(0)
