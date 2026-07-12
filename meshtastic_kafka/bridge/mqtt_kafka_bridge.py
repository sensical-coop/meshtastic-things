import asyncio
import os
import sys
import argparse
from bridge.mqtt_handler import MQTTHandler
from dotenv import load_dotenv

import structlog
log = structlog.get_logger()

async def create_tasks(mqtt_handler):

    # queue = asyncio.Queue()
    log.info('Creating bridge tasks...')
    async with asyncio.TaskGroup() as tg:
        tg.create_task(mqtt_handler.bridge_mqtt_kafka(topic=os.environ['MQTT_BRIDGE__TOPIC'], shared_sub=os.environ['MQTT_BRIDGE__SHARED_SUBS']))
        tg.create_task(mqtt_handler.monitor_publish())
        tg.create_task(mqtt_handler.start_health_server())
        tg.create_task(mqtt_handler.producer_health.monitor())

        log.info("Looping forever...")

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--local", default=False, dest='local', action='store_true', help="Local development mode"
    )

    args = parser.parse_args()

    if args.local:
        load_dotenv("../.env")
        kafka_bootstrap_server=os.environ['KAFKA_BOOTSTRAP_LOCAL']
        log.warn('Local development mode')
    else:
        log.warn("Set environment file manually")
        kafka_bootstrap_server=os.environ['KAFKA_BOOTSTRAP']

    mqtt_handler = MQTTHandler(broker=os.environ['MQTT_BRIDGE__BROKER'], kafka_bootstrap_server=kafka_bootstrap_server)

    log.info("Starting MQTT bridge...")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(create_tasks(mqtt_handler))
    except KeyboardInterrupt:
        log.info("Program interrupted by user")
        sys.exit(0)