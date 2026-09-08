import argparse
import os
import sys
import asyncio

import structlog
from dotenv import load_dotenv
from writer.s3_writer import S3Writer

log = structlog.get_logger()

async def create_tasks(s3_writer):

    log.info('Creating s3 writer tasks...')

    async with asyncio.TaskGroup() as tg:
        tg.create_task(s3_writer.start_health_server())
        tg.create_task(s3_writer.consumer_health.monitor())
        tg.create_task(s3_writer.write_kafka_s3())

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
        kafka_bootstrap_server=os.environ['KAFKA_BOOTSTRAP']

    s3_writer = S3Writer(
        kafka_bootstrap_server=kafka_bootstrap_server,
        s3_bucket=os.environ['S3_WRITER__BUCKET'],
        s3_prefix=os.environ['S3_WRITER__PREFIX'],
        endpoint_url=os.environ.get('S3_WRITER__ENDPOINT_URL'),
        access_key=os.environ.get('S3_WRITER__ACCESS_KEY'),
        secret_key=os.environ.get('S3_WRITER__SECRET_KEY'),
        region_name=os.environ.get('S3_WRITER__REGION'),
    )

    log.info("Starting S3 writer...")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(create_tasks(s3_writer))
    except KeyboardInterrupt:
        log.info("Program interrupted by user")
        log.info("Closing connections...")
        s3_writer.kafka_consumer.close()
        sys.exit(0)


main()
