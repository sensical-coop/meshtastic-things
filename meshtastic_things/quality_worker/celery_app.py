"""Celery app for the batch data quality checks

This exists for work that fundamentally needs history: blueprint
algorithms over days/months, and gap analysis that can only be seen across a
window. Per-point checks belong in Flink instead
"""
import os

from celery import Celery
from celery.schedules import crontab  # noqa: F401  (available for custom schedules)

BROKER_URL = os.environ.get("QUALITY_WORKER__REDIS_URL", "redis://redis:6379/2")
SCAN_INTERVAL_SECONDS = float(os.environ.get("QUALITY_WORKER__SCAN_INTERVAL_SECONDS", 3600))

app = Celery("quality_worker", broker=BROKER_URL, backend=BROKER_URL)

app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    result_expires=3600,
    broker_connection_retry_on_startup=True,
    beat_schedule={
        "scan-devices": {
            "task": "quality_worker.tasks.scan_devices",
            "schedule": SCAN_INTERVAL_SECONDS,
        }
    },
)

# Registers the tasks named in beat_schedule above.
from quality_worker import tasks  # noqa: E402,F401
