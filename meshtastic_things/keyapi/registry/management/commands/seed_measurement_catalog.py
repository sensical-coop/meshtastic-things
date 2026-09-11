"""Populate the TelemetryVariant/MeasurementType/TelemetryVariantMeasurement
catalog from the Meshtastic Telemetry protobuf schema"""
from google.protobuf.descriptor import FieldDescriptor
from meshtastic.protobuf import telemetry_pb2

from django.core.management.base import BaseCommand

from common.channels import QUALITY
from common.units import QUALITY_CHANNELS, quantity_kind_for

from registry.models import MeasurementType, TelemetryVariant, TelemetryVariantMeasurement

# Non-numeric fields (e.g. host_metrics.user_string) aren't part of this catalog.
_SKIP_CPP_TYPES = (FieldDescriptor.CPPTYPE_STRING, FieldDescriptor.CPPTYPE_MESSAGE)

# The data-quality metrics quality_worker writes to InfluxDB's `quality`
# measurement.
_QUALITY_SEED = [
    (field_name, unit, quantity_kind_for(unit), description)
    for field_name, (unit, description) in QUALITY_CHANNELS.items()
]


class Command(BaseCommand):
    help = "Seed TelemetryVariant/MeasurementType/TelemetryVariantMeasurement from the Telemetry protobuf schema."

    def handle(self, *args, **options):
        variant_fields = telemetry_pb2.Telemetry.DESCRIPTOR.oneofs_by_name["variant"].fields
        telemetry_variants = measurement_types = links = 0

        for variant_field in variant_fields:
            payload_kind = f"telemetry:{variant_field.name}"
            telemetry_variant, created = TelemetryVariant.objects.get_or_create(
                payload_kind=payload_kind, defaults={"name": variant_field.message_type.name}
            )
            telemetry_variants += created

            for field in variant_field.message_type.fields:
                if field.cpp_type in _SKIP_CPP_TYPES:
                    continue
                measurement_type, created = MeasurementType.objects.get_or_create(
                    payload_kind=payload_kind, field_name=field.name
                )
                measurement_types += created
                _, created = TelemetryVariantMeasurement.objects.get_or_create(
                    telemetry_variant=telemetry_variant, measurement_type=measurement_type
                )
                links += created

        quality_variant, created = TelemetryVariant.objects.get_or_create(
            payload_kind=QUALITY,
            defaults={"name": "Data quality metrics"},
        )
        telemetry_variants += created
        for field_name, unit, quantity_kind, description in _QUALITY_SEED:
            measurement_type, created = MeasurementType.objects.get_or_create(
                payload_kind=QUALITY,
                field_name=field_name,
                defaults={
                    "native_unit": unit,
                    "quantity_kind": quantity_kind,
                    "description": description,
                },
            )
            measurement_types += created
            _, created = TelemetryVariantMeasurement.objects.get_or_create(
                telemetry_variant=quality_variant, measurement_type=measurement_type
            )
            links += created

        self.stdout.write(
            f"Created {telemetry_variants} new TelemetryVariant, {measurement_types} new MeasurementType, "
            f"{links} new TelemetryVariantMeasurement rows "
            f"({len(variant_fields)} telemetry variants + the quality namespace)."
        )
