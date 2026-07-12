import asyncio
import os

import aiomqtt
from aiohttp import web
import structlog
from kafka import KafkaProducer
from tools.healthcheck import HealthState, KakfaProducerHealth, MQTTHealth
from tools.key import get_key

log = structlog.get_logger()

class MQTTHandler(object):
    def __init__(self, broker, kafka_bootstrap_server):
        self.mqtt_broker = broker
        self.kafka_bootstrap_server = kafka_bootstrap_server
        self.kafka_producer = KafkaProducer(bootstrap_servers=self.kafka_bootstrap_server)
        self.health_state = HealthState()
        self.producer_health = KakfaProducerHealth(
            self.kafka_producer,
            topic=os.environ['MQTT_BRIDGE__HEALTH__KAFKA_TOPIC'],
            timeout=int(os.environ['MQTT_BRIDGE__HEALTH__TIMEOUT'])
        )
        self.mqtt_health = MQTTHealth()

    async def bridge_mqtt_kafka(self, topic, shared_sub=False):

        if shared_sub:
            topic = f'$share/kafka-bridge/{topic}'

        client = aiomqtt.Client(self.mqtt_broker, username=os.environ['MQTT_BRIDGE__USER'], password=os.environ['MQTT_BRIDGE__PASS'], clean_session=os.environ['MQTT_BRIDGE__CLEAN_SESSION'])

        while True:
            try:
                async with client:
                    self.mqtt_health.mark_connected()
                    log.info(f'Subscription: {self.mqtt_broker}, topic: {topic}')
                    await client.subscribe(topic)
                    async for message in client.messages:
                        self.mqtt_health.mark_connected()
                        log.debug(f'Got message: {message.topic}: {message.payload}')
                        self.kafka_producer.send(os.environ['MQTT_BRIDGE__KAFKA_TOPIC'], key=get_key(message.topic, message.payload), value=message.payload)
            except aiomqtt.MqttError:
                self.mqtt_health.mark_disconnected()
                log.info(f"Connection lost; Reconnecting in {int(os.environ['MQTT_BRIDGE__RECONNECT'])} seconds ...")
                await asyncio.sleep(int(os.environ['MQTT_BRIGE__RECONNECT']))
            except Exception as e:
                log.exception("An error occurred", exc_info=e)

    async def monitor_publish(self):
        while True:
            log.debug('MQTT monitor sending health ping')
            try:
                await self.publish_to_mqtt("health/ping", "ok")
            except aiomqtt.MqttError:
                self.mqtt_health.mark_disconnected()
            await asyncio.sleep(10)

    async def publish_to_mqtt(self, topic, payload):
        async with aiomqtt.Client(self.mqtt_broker) as client:
            await client.publish(topic, payload=payload)
            self.mqtt_health.mark_connected()

    async def health_handler(self, request):

        self.health_state.kafka_connected = self.producer_health.is_healthy()
        self.health_state.mqtt_connected = self.mqtt_health.is_healthy()
        log.info(f'Health check. Kafka connected: {self.health_state.kafka_connected}')
        log.info(f'Health check. MQTT connected: {self.health_state.mqtt_connected}')

        if self.health_state.is_healthy():
            return web.json_response({"status": "ok"})
        return web.json_response({"status": "unhealthy"}, status=503)

    async def start_health_server(self):
        app = web.Application()
        app.router.add_get("/health", self.health_handler)
        log.info(f"Starting health server on 0.0.0.0:{int(os.environ['MQTT_BRIDGE__HEALTH__PORT'])}")

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", int(os.environ['MQTT_BRIDGE__HEALTH__PORT']))
        await site.start()
