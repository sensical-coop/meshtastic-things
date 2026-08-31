import asyncio
import aiomqtt
import os
import time
import structlog

log = structlog.get_logger()

class HealthState:
    def __init__(self):
        self.kafka_connected = False
        self.mqtt_connected = False
        self.influx_connected = False
        self.s3_connected = False

    def is_healthy(self):
        return all([
            self.kafka_connected,
            self.influx_connected or self.mqtt_connected or self.s3_connected
        ])

class MQTTHealth:

    def __init__(self):
        self.connected = False
        self.last_ok = 0

    def mark_connected(self):
        self.connected = True
        self.last_ok = time.time()

    def mark_disconnected(self):
        self.connected = False

    def is_healthy(self):
        return self.connected and (time.time() - self.last_ok < 30)

class KafkaConsumerHealth:

    def __init__(self, consumer, topic):
        self.consumer = consumer
        self.last_ok = 0
        self.topic = topic

    async def monitor(self):
        while True:
            try:
                self.consumer.partitions_for_topic(self.topic)
                self.last_ok = time.time()
            except Exception as e:
                log.exception('Kafka consumer monitor', exc_info=e)
                pass

            await asyncio.sleep(10)

    def is_healthy(self):
        return (time.time() - self.last_ok) < 30

class KakfaProducerHealth:

    def __init__(self, producer, topic, timeout):
        self.producer = producer
        self.topic = topic
        self.last_ok = 0
        self.timeout = timeout

    async def monitor(self):
        while True:
            try:
                future = self.producer.send(
                    self.topic,
                    b"healthcheck"
                )
                future.get(timeout=self.timeout)
                self.last_ok = time.time()
            except Exception:
                pass
            await asyncio.sleep(10)

    def is_healthy(self):
        return (time.time() - self.last_ok) < 30