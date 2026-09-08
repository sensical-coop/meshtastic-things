import argparse
import os
import sys
import asyncio
from datetime import datetime

import structlog
from dotenv import load_dotenv
from writer.influx_writer import InfluxWriter

log = structlog.get_logger()

async def create_tasks(influx_writer):

    # queue = asyncio.Queue()
    log.info('Creating influx writer tasks...')

    async with asyncio.TaskGroup() as tg:
        # NOTE Use this structure if we switch to multiple incoming topics
        # for topic in INCOMING_TOPICS:
        #     tg.create_task(mqtt_handler.bridge_incomming(topic=topic, queue=queue))
        tg.create_task(influx_writer.start_health_server())
        tg.create_task(influx_writer.consumer_health.monitor())
        tg.create_task(influx_writer.write_kafka_influx())

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
        influx_url = os.environ['INFLUX__URL_LOCAL']
    else:
        kafka_bootstrap_server=os.environ['KAFKA_BOOTSTRAP']
        influx_url = os.environ['INFLUX__URL']

    influx_writer = InfluxWriter(kafka_bootstrap_server=kafka_bootstrap_server,
    influx_url=influx_url)

    log.info("Starting Influx writer...")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(create_tasks(influx_writer))
    except KeyboardInterrupt:
        log.info("Program interrupted by user")
        log.info("Closing connections...")
        influx_writer.kafka_consumer.close()
        sys.exit(0)


main()
