"""Background worker: invalidates the cached introspection result for an
owner as soon as keyapi rotates/revokes their key.

  python manage.py consume_owner_auth_revocations
"""
import json
import os

import structlog
from django.core.cache import cache
from django.core.management.base import BaseCommand
from kafka import KafkaConsumer

log = structlog.get_logger()


class Command(BaseCommand):
    help = "Consume mesh.owner_auth_revocations.v1 and drop the matching auth cache entry"

    def handle(self, *args, **options):
        bootstrap_servers = os.environ["KAFKA_BOOTSTRAP"]
        topic = os.environ.get("KEYAPI__OWNER_AUTH_REVOCATIONS_KAFKA_TOPIC", "mesh.owner_auth_revocations.v1")
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id="queryapi-owner-auth-revocations-group",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        consumer.subscribe([topic])
        log.info("Starting owner-auth-revocations consumer", topic=topic)
        try:
            while True:
                messages = consumer.poll(timeout_ms=1000, max_records=500)
                if not messages:
                    continue
                for records in messages.values():
                    for record in records:
                        if record.value is None:
                            continue
                        try:
                            self._handle(json.loads(record.value))
                        except Exception as e:
                            log.warning("Failed to process owner auth revocation", error=str(e), raw=record.value)
                consumer.commit()
        finally:
            consumer.close()

    @staticmethod
    def _handle(event: dict) -> None:
        api_key_hash = event.get("api_key_hash")
        if not api_key_hash:
            return
        cache.delete(f"queryapi:owner:{api_key_hash}")
