"""Background worker: upserts TelemetryVariant/Sensor/MeasurementType/Measurement
rows per mesh.sensor_discovery.v1 event

  python manage.py consume_sensor_discovery
"""
import json
import os

import structlog
from django.core.management.base import BaseCommand
from django.utils import timezone
from kafka import KafkaConsumer

from common.units import quantity_kind_for

from registry.models import Device, Measurement, MeasurementType, Sensor, TelemetryVariant

log = structlog.get_logger()


class Command(BaseCommand):
    help = "Consume mesh.sensor_discovery.v1 and upsert Sensor/Measurement rows"

    def handle(self, *args, **options):
        bootstrap_servers = os.environ["KAFKA_BOOTSTRAP"]
        topic = os.environ.get("KEYAPI__SENSOR_DISCOVERY_KAFKA_TOPIC", "mesh.sensor_discovery.v1")
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id="keyapi-sensor-discovery-group",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        consumer.subscribe([topic])
        log.info("Starting sensor-discovery consumer", topic=topic)
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
                            event = json.loads(record.value)
                            if event.get("op") != "upsert":
                                continue
                            self._upsert(event)
                        except Exception as e:
                            log.warning("Failed to upsert sensor discovery", error=str(e), raw=record.value)
                consumer.commit()
        finally:
            consumer.close()

    @staticmethod
    def _upsert(event: dict) -> None:
        try:
            device = Device.objects.get(mesh_id=event["mesh_id"], device_id=int(event["device_id"]))
        except (Device.DoesNotExist, ValueError):
            return
        payload_kind = event["payload_kind"]
        telemetry_variant, _ = TelemetryVariant.objects.get_or_create(payload_kind=payload_kind)
        sensor, _ = Sensor.objects.get_or_create(device=device, telemetry_variant=telemetry_variant)
        sensor.last_seen = timezone.now()
        sensor.save(update_fields=["last_seen"])
        # This is optional because we either get it from the protos, or from the
        # computed channels (derived or quality)
        field_units = event.get("field_units") or {}
        for field in event["fields"]:
            measurement_type, _ = MeasurementType.objects.get_or_create(
                payload_kind=payload_kind, field_name=field
            )
            Command._apply_unit(measurement_type, field_units.get(field))
            measurement, _ = Measurement.objects.get_or_create(sensor=sensor, measurement_type=measurement_type)
            measurement.last_seen = timezone.now()
            measurement.save(update_fields=["last_seen"])

    @staticmethod
    def _apply_unit(measurement_type: MeasurementType, unit: str | None) -> None:
        """Fill in a missing unit but never overwrite one that's already there.

        This can be superseeded by a superuser or the protobuf seed.
        A producer republishing every run must not undo them.
        """
        if not unit or measurement_type.native_unit:
            return
        measurement_type.native_unit = unit
        fields = ["native_unit"]
        if not measurement_type.quantity_kind:
            # Derived from the unit rather than sent separately
            measurement_type.quantity_kind = quantity_kind_for(unit)
            fields.append("quantity_kind")
        measurement_type.save(update_fields=fields)
