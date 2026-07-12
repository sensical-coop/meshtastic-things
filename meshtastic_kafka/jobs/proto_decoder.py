import argparse
import asyncio
import base64
import os
import sys

import structlog
from dotenv import load_dotenv
from proto_handler import ProtoHandler

log = structlog.get_logger()

async def create_tasks(proto_handler):

    # queue = asyncio.Queue()
    log.info('Creating bridge tasks...')
    async with asyncio.TaskGroup() as tg:

        tg.create_task(proto_handler.proto_listener())
        # tg.create_task(proto_handler.monitor_publish())
        # tg.create_task(proto_handler.start_health_server())
        # tg.create_task(proto_handler.producer_health.monitor())

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

    proto_handler = ProtoHandler(kafka_bootstrap_server=kafka_bootstrap_server)

    log.info("Starting Proto decoder...")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(create_tasks(proto_handler))
    except KeyboardInterrupt:
        log.info("Program interrupted by user")
        sys.exit(0)


main()