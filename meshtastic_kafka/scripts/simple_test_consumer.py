import argparse
import json
import os
import signal
import sys
import asyncio
from datetime import datetime

import structlog
from dotenv import load_dotenv
from kafka import KafkaConsumer

log = structlog.get_logger()

class SimpleConsumer(object):

    def __init__(self, kafka_bootstrap_server):
        self.kafka_bootstrap_server = kafka_bootstrap_server
        self.kafka_consumer = KafkaConsumer(
            bootstrap_servers=self.kafka_bootstrap_server,
            group_id='simple-consumer-group',
            enable_auto_commit=False,
            auto_offset_reset="earliest"
        )

        topics = [os.environ[key] for key in dict(os.environ).keys() if "__KAFKA_TOPIC" in key]
        log.debug(f"Subscribing to kafka topics {topics}")

        self.kafka_consumer.subscribe(topics=topics)

    def consume(self):
        while True:
            messages = self.kafka_consumer.poll(
                max_records=1000,
                timeout_ms=1000)

            if not messages:
                continue

            for topic_partition, consumer_records in messages.items():
                try:
                    log.info(f'Got the following messages for topic: {topic_partition.topic}:')
                    for record in consumer_records:
                        try:
                            log.info(f'{record.key} [{record.timestamp}]: {record.value}')
                        except Exception as e:
                            log.exception(f"Bad message skipped", exc_info=e)
                except Exception as e:
                    log.exception(f"Issue while opening records", exc_info=e)

async def main():
    log.info('Creating consumer tasks...')
    async with asyncio.TaskGroup() as tg:
        tg.create_task(consumer.consume())
        log.info("Looping forever...")

if __name__ == "__main__":
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
        kafka_bootstrap_server=os.environ['KAFKA_BOOTSTRAP']

    consumer = SimpleConsumer(kafka_bootstrap_server=kafka_bootstrap_server)

    log.info("Starting simple writer...")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        log.info("Program interrupted by user")
        log.info("Closing connections...")
        consumer.kafka_consumer.close()
        sys.exit(0)
