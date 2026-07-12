import json
import os
import asyncio
from datetime import datetime

from aiohttp import web
import structlog
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client import Point, WriteOptions
from influxdb_client.client.write_api import WriteType
from kafka import KafkaConsumer
from tools.healthcheck import HealthState, KafkaConsumerHealth

MIN_FEASIBLE_TIME = 1577836800 # 01-01-2020 in epoch (s)

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
        log.debug(f"Subscribing to kafka topics {[os.environ['PROTO_DECODE__KAFKA_TOPIC']]}")
        self.kafka_consumer.subscribe(
            topics=[os.environ['PROTO_DECODE__KAFKA_TOPIC']]
        )
        self.health_state = HealthState()
        self.consumer_health = KafkaConsumerHealth(
            consumer=self.kafka_consumer,
            topic=os.environ['PROTO_DECODE__KAFKA_TOPIC']
        )

    def convert_date(self, time):
        return datetime.utcfromtimestamp(time).strftime("%Y-%m-%dT%H:%M:%SZ")

    # TODO Make it variable size with message properties
    def to_point(self, event: dict) -> Point:
        print (event)

        # TODO - Check if point is not already in cache
        # TODO - Add other payload types (non-telemetry)

        # Check if time comes from device or rx
        timestamp = event['rx_time']
        timestamp_quality = 'rx_time'
        if 'payload' in event:
            if 'time' in event['payload']:
                if event['payload']['time'] > MIN_FEASIBLE_TIME:
                    timestamp = event['payload']['time']
                    timestamp_quality = 'payload'

        point = {
            "tags": {
                "node_id": event["node_id"],
            },
            "fields": {},
            "time": self.convert_date(timestamp),
        }

        try:
            # TODO there must be a better way to do this based on protobuf info
            # TODO make this error proof
            for key in event['payload']:
                if key == 'time': continue
                if type(event['payload'][key]) == dict:
                    telemetry_type = key
                    for item in event['payload'][key]:
                        point["fields"][item] = float(event['payload'][key][item])

            point["measurement"] = telemetry_type
            point["fields"]["packet_id"] = event["packet_id"]
            point["fields"]["timestamp_quality"] = timestamp_quality

        except Exception as e:
            log.exception(f"Issue while parsing payload", exc_info=e)

        else:
            return Point.from_dict(point)

        return None

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
                                point = self.to_point(event)
                                if point is not None:
                                    points.append(self.to_point(event))
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
        # TODO add influxdb healthcheck
        self.health_state.kafka_connected = self.consumer_health.is_healthy()
        # self.health_state.influx_connected = self.influx_healt.is_healthy()
        log.info(f'Health check. Kafka connected: {self.health_state.kafka_connected}')
        # log.info(f'Health check. MQTT connected: {self.health_state.mqtt_connected}')

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
