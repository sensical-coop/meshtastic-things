"""Background worker: emails a device's owner when a quality alarm is triggered.

  python manage.py consume_quality_alarms

Consumes mesh.telemetry.alarms.v1:
- Flink stream jobs (flat_value/plausibility/absence)
- quality_worker's batch checks (completeness).

Alarms are repeated by design (an absent device will trigger again), but
identical alarms are cooled down (in Redis)
"""
import json
import os

import structlog
from django.core.cache import cache
from django.core.management.base import BaseCommand

from kafka import KafkaConsumer

from registry.emailing import send_templated_email
from registry.models import Device

log = structlog.get_logger()

# Don't re-email the same (device, channel, detector) more often than this.
ALARM_EMAIL_COOLDOWN_SECONDS = int(os.environ.get("KEYAPI__ALARM_EMAIL_COOLDOWN_SECONDS", 3600))

DETECTOR_SUBJECTS = {
    "absence": "Device has stopped publishing",
    "completeness": "Device readings have gaps",
    "flat_value": "Sensor value is flat",
    "plausibility": "Sensor value is out of range",
}


class Command(BaseCommand):
    help = "Consume mesh.telemetry.alarms.v1 and email the device owner - see module docstring."

    def handle(self, *args, **options):
        bootstrap_servers = os.environ["KAFKA_BOOTSTRAP"]
        topic = os.environ.get("PATTERN_DETECTION__ALARMS_TOPIC", "mesh.telemetry.alarms.v1")
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id="keyapi-quality-alarms-group",
            enable_auto_commit=False,
            auto_offset_reset="latest", # avoid bursting
        )
        consumer.subscribe([topic])
        log.info("Starting quality-alarms consumer", topic=topic)
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
                            log.warning("Failed to handle quality alarm", error=str(e), raw=record.value)
                consumer.commit()
        finally:
            consumer.close()

    @staticmethod
    def _handle(event: dict) -> None:
        detector = event.get("detector")
        mesh_id, device_id = event.get("mesh_id"), event.get("device_id")
        if not detector or not mesh_id or device_id is None:
            log.debug("Skipping unroutable alarm", detector=detector)
            return

        device = (
            Device.objects.select_related("mesh__owner")
            .filter(mesh_id=mesh_id, device_id=int(device_id))
            .first()
        )
        if device is None:
            # Deleted between the alarm trigger and now?
            log.info("Alarm for unknown device, skipping", mesh_id=mesh_id, device_id=device_id)
            return
        owner = device.mesh.owner
        if not owner.email or not owner.is_active:
            log.info("Owner has no verified email, skipping", owner_id=str(owner.id))
            return

        channel = event.get("channel")
        cooldown_key = f"keyapi:alarm-email:{mesh_id}:{device_id}:{channel}:{detector}"
        if not cache.add(cooldown_key, "1", ALARM_EMAIL_COOLDOWN_SECONDS):
            log.debug("Alarm email suppressed by cooldown", key=cooldown_key)
            return

        send_templated_email(
            to_email=owner.email,
            subject=f"[Meshtastic] {DETECTOR_SUBJECTS.get(detector, 'Data quality alarm')}"
            f" - {device.label or device.name or device.device_id}",
            template_name="quality_alarm",
            context={
                "name": owner.name,
                "device_label": device.label or device.name or str(device.device_id),
                "device_id": device.device_id,
                "mesh_label": device.mesh.label or device.mesh.channel_id,
                "channel": channel,
                "detector": detector,
                "detector_summary": DETECTOR_SUBJECTS.get(detector, "Data quality alarm"),
                "message": event.get("message"),
                "value": event.get("value"),
                "source": event.get("source"),
                "triggered_at": event.get("triggered_at"),
                "cooldown_hours": round(ALARM_EMAIL_COOLDOWN_SECONDS / 3600, 1),
            },
        )
        log.info("Sent quality alarm email", detector=detector, device_id=device.device_id)
