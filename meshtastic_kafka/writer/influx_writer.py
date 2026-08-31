import json
import os
import asyncio

from aiohttp import web
import structlog
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client import WriteOptions
from influxdb_client.client.write_api import WriteType
from kafka import KafkaConsumer
from tools.healthcheck import HealthState, KafkaConsumerHealth
from writer.point_builders import get_points

log = structlog.get_logger()

class InfluxWriter(object):
    def __init__(self, kafka_bootstrap_server, influx_url):
        self.kafka_bootstrap_server = kafka_bootstrap_server
        self.kafka_consumer = KafkaConsumer(
            bootstrap_servers=self.kafka_bootstrap_server,
            group_id='influx-writer-group',
            enable_auto_commit=False,
            auto_offset_reset="earliest"

        )
        self.influx_url = influx_url
        topics = [os.environ['PROTO_DECODE__KAFKA_TOPIC']]
        # Optional: also persist pattern-detection alarms into the same bucket,
        # if that job is deployed
        alarms_topic = os.environ.get('PATTERN_DETECTION__ALARMS_TOPIC')
        if alarms_topic:
            topics.append(alarms_topic)
        log.debug(f"Subscribing to kafka topics {topics}")
        self.kafka_consumer.subscribe(topics=topics)
        self.health_state = HealthState()
        self.consumer_health = KafkaConsumerHealth(
            consumer=self.kafka_consumer,
            topic=os.environ['PROTO_DECODE__KAFKA_TOPIC']
        )

    async def write_kafka_influx(self):

        while True:
            # log.debug("Getting points")
            messages = self.kafka_consumer.poll(
                max_records=int(os.environ['INFLUX__CONSUMER__N_MSGS']),
                timeout_ms=int(os.environ['INFLUX__CONSUMER__TIMEOUT_MS']))

            # TODO We need to leave some time for other health checks
            # And to avoid CPU to go awol
            await asyncio.sleep(1)
            if not messages:
                continue

            points = []

            for topic_partition, consumer_records in messages.items():
                try:
                    log.info(f'Got the following messages for topic: {topic_partition.topic}:')
                    for record in consumer_records:
                        try:
                            log.debug(f'{record.key} [{record.timestamp}]: {record.value}')
                            event = json.loads(record.value)
                            if event is not None:
                                points.extend(get_points(event))
                        except Exception as e:
                            log.exception(f"Bad message skipped", exc_info=e)
                except Exception as e:
                    log.exception(f"Issue while opening records", exc_info=e)

            if points:
                try:
                    async with InfluxDBClientAsync(url=self.influx_url, token=os.environ['INFLUX__TOKEN'], org=os.environ['INFLUX__ORG']) as influxdb_client:
                        influx_write_api = influxdb_client.write_api()
                        await influx_write_api.write(bucket=os.environ['INFLUX__BUCKET'], record=points)

                    self.kafka_consumer.commit()
                    log.debug("Commit offset")
                except Exception as e:
                    log.exception(f"Write failed, retrying next poll", exc_info=e)

    async def health_handler(self, request):
        self.health_state.kafka_connected = self.consumer_health.is_healthy()
        try:
            async with InfluxDBClientAsync(url=self.influx_url, token=os.environ['INFLUX__TOKEN'], org=os.environ['INFLUX__ORG']) as influxdb_client:
                self.health_state.influx_connected = await influxdb_client.ping()
        except Exception as e:
            log.warning("InfluxDB ping failed", error=str(e))
            self.health_state.influx_connected = False
        log.info(f'Health check. Kafka connected: {self.health_state.kafka_connected}, Influx connected: {self.health_state.influx_connected}')

        if self.health_state.is_healthy():
            return web.json_response({"status": "ok"})
        return web.json_response({"status": "unhealthy"}, status=503)

    async def start_health_server(self):
        try:
            log.info(f"Starting health server on 0.0.0.0:{int(os.environ['INFLUX__HEALTHCHECK_PORT'])}")
            app = web.Application()
            app.router.add_get("/health", self.health_handler)

            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, "0.0.0.0", int(os.environ['INFLUX__HEALTHCHECK_PORT']))
            await site.start()
        except Exception as e:
            log.exception('Problem with web server', exc_info=e)
