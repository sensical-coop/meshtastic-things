import uuid

from django.contrib.auth.base_user import AbstractBaseUser, BaseUserManager
from django.contrib.auth.models import PermissionsMixin
from django.db import models


class OwnerManager(BaseUserManager):
    """Owners authenticate via bearer API key"""

    def create_user(self, email: str, name: str, password: str | None = None, **extra_fields):
        if not email:
            raise ValueError("Owners must have an email")
        owner = self.model(email=self.normalize_email(email), name=name, **extra_fields)
        if password:
            owner.set_password(password)
        else:
            owner.set_unusable_password()
        owner.save(using=self._db)
        return owner

    def create_superuser(self, email: str, name: str, password: str | None = None, **extra_fields):
        extra_fields.setdefault("is_staff", True)
        extra_fields.setdefault("is_superuser", True)
        return self.create_user(email, name, password, **extra_fields)


class Owner(AbstractBaseUser, PermissionsMixin):
    """A registered account"""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    email = models.EmailField(unique=True)
    name = models.CharField(max_length=255)
    # sha256 hex digest
    api_key_hash = models.CharField(max_length=64, unique=True, db_index=True, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    is_staff = models.BooleanField(default=False)
    is_active = models.BooleanField(default=True)
    # is_active means "email-verified"
    email_verified_at = models.DateTimeField(null=True, blank=True)
    email_verification_token_hash = models.CharField(max_length=64, unique=True, db_index=True, null=True, blank=True)
    email_verification_sent_at = models.DateTimeField(null=True, blank=True)

    objects = OwnerManager()

    USERNAME_FIELD = "email"
    REQUIRED_FIELDS = ["name"]

    class Meta:
        db_table = "owners"

    def __str__(self) -> str:
        return self.email


class Mesh(models.Model):
    """Physical Meshtastic network"""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    channel_id = models.CharField(max_length=255, db_index=True)
    # PSK is password field: write-only over the API
    psk_b64 = models.TextField()
    label = models.CharField(max_length=255, null=True, blank=True)
    owner = models.ForeignKey(Owner, on_delete=models.CASCADE, related_name="meshes")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "meshes"


def _max_three_admin_keys(value: list[str]) -> None:
    from django.core.exceptions import ValidationError

    if value is not None and len(value) > 3:
        raise ValidationError("Meshtastic firmware supports at most 3 admin keys (Config.SecurityConfig.admin_key)")


class PostprocessingBlueprint(models.Model):
    """Reusable recipe for calculating new channels."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    description = models.TextField(null=True, blank=True)
    owner = models.ForeignKey(Owner, on_delete=models.CASCADE, related_name="blueprints")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "postprocessing_blueprints"
        constraints = [
            models.UniqueConstraint(fields=["owner", "name"], name="uq_blueprint_owner_name")
        ]


class PostprocessingStep(models.Model):
    """One step for a blueprint, run in `order`"""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    blueprint = models.ForeignKey(PostprocessingBlueprint, on_delete=models.CASCADE, related_name="steps")
    order = models.PositiveIntegerField()
    function_name = models.CharField(max_length=128)
    inputs = models.JSONField(default=dict, blank=True)
    params = models.JSONField(default=dict, blank=True)
    outputs = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "postprocessing_steps"
        ordering = ["order"]
        constraints = [
            models.UniqueConstraint(fields=["blueprint", "order"], name="uq_step_blueprint_order")
        ]


class Device(models.Model):
    """A physical Meshtastic device (either gateway or node)."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    device_id = models.BigIntegerField(db_index=True)
    mesh = models.ForeignKey(Mesh, on_delete=models.CASCADE, related_name="devices")
    label = models.CharField(max_length=255, null=True, blank=True)
    # TODO - Fix, these could be just "is_gateway". If it's not gateway, it's a node
    is_gateway = models.BooleanField(default=False)
    is_node = models.BooleanField(default=False)
    is_allowed = models.BooleanField(default=True)
    # Password field: write-only
    admin_keys_b64 = models.JSONField(default=list, blank=True, validators=[_max_three_admin_keys])
    # Optional Blueprint
    postprocessing_blueprint = models.ForeignKey(
        PostprocessingBlueprint, on_delete=models.SET_NULL, null=True, blank=True, related_name="devices"
    )
    reading_interval_seconds = models.PositiveIntegerField(null=True, blank=True)
    publish_interval_seconds = models.PositiveIntegerField(null=True, blank=True)
    latitude = models.FloatField(null=True, blank=True)
    longitude = models.FloatField(null=True, blank=True)
    location_overridden = models.BooleanField(default=False)
    # Auto-filled from NodeInfo
    name = models.CharField(max_length=255, null=True, blank=True)
    short_name = models.CharField(max_length=32, null=True, blank=True)
    hardware_type = models.CharField(max_length=64, null=True, blank=True)
    role = models.CharField(max_length=32, null=True, blank=True)
    nodeinfo_overridden = models.BooleanField(default=False)
    last_seen = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "devices"
        constraints = [
            models.CheckConstraint(
                check=(models.Q(is_gateway=True, is_node=False) | models.Q(is_gateway=False, is_node=True)),
                name="device_is_gateway_xor_node",
            ),
            models.UniqueConstraint(
                fields=["device_id"], condition=models.Q(is_allowed=True), name="uq_devices_device_id_allowed"
            ),
        ]


class MeasurementType(models.Model):
    """Definition of one physical quantity"""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    payload_kind = models.CharField(max_length=64)  # e.g. "telemetry:environment_metrics"
    field_name = models.CharField(max_length=64)  # e.g. "temperature"
    display_name = models.CharField(max_length=255, null=True, blank=True)
    description = models.TextField(null=True, blank=True)  # proto comment
    native_unit = models.CharField(max_length=32, null=True, blank=True)  # "degC", "V", "ug/m3", "%"
    quantity_kind = models.CharField(
        max_length=32,
        null=True,
        blank=True,
        choices=[
            ("physical_property", "Physical property"),
            ("concentration", "Concentration"),
            ("count", "Count"),
            ("ratio", "Ratio/percent"),
            ("other", "Other"),
        ],
    )
    vocabulary_uri = models.URLField(null=True, blank=True)  # QUDT/EIONET concept URI
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "measurement_types"
        constraints = [models.UniqueConstraint(fields=["payload_kind", "field_name"], name="uq_measurement_type")]


class TelemetryVariant(models.Model):
    """Meshtastic Telemetry protobuf "variant"""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    payload_kind = models.CharField(max_length=64, unique=True)
    name = models.CharField(max_length=255, null=True, blank=True)
    model = models.CharField(max_length=255, null=True, blank=True)
    datasheet_url = models.URLField(null=True, blank=True)
    description = models.TextField(null=True, blank=True)
    default_reading_interval_seconds = models.PositiveIntegerField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "telemetry_variants"


class TelemetryVariantMeasurement(models.Model):
    """Declares that a TelemetryVariant has a given MeasurementType, and
    its the plausible value range for it - e.g.
    environment_metrics -> temperature: [-40, 85]."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    telemetry_variant = models.ForeignKey(TelemetryVariant, on_delete=models.CASCADE, related_name="measurement_links")
    measurement_type = models.ForeignKey(
        MeasurementType, on_delete=models.CASCADE, related_name="telemetry_variant_links"
    )
    valid_min = models.FloatField(null=True, blank=True)
    valid_max = models.FloatField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "telemetry_variant_measurements"
        constraints = [
            models.UniqueConstraint(
                fields=["telemetry_variant", "measurement_type"], name="uq_telemetry_variant_measurement"
            )
        ]


class Sensor(models.Model):
    # TODO Fix

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    device = models.ForeignKey(Device, on_delete=models.CASCADE, related_name="sensors")
    telemetry_variant = models.ForeignKey(TelemetryVariant, on_delete=models.CASCADE, related_name="sensors")
    first_seen = models.DateTimeField(auto_now_add=True)
    last_seen = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "sensors"
        constraints = [
            models.UniqueConstraint(fields=["device", "telemetry_variant"], name="uq_sensor_device_telemetry_variant")
        ]


class Measurement(models.Model):
    """One measurement channel a Sensor produces"""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    sensor = models.ForeignKey(Sensor, on_delete=models.CASCADE, related_name="measurements")
    measurement_type = models.ForeignKey(MeasurementType, on_delete=models.CASCADE, related_name="measurements")
    first_seen = models.DateTimeField(auto_now_add=True)
    last_seen = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "measurements"
        constraints = [
            models.UniqueConstraint(fields=["sensor", "measurement_type"], name="uq_measurement_sensor_type")
        ]
