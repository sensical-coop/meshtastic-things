from rest_framework import serializers

from common.algorithms import validate_step
from common.channels import channel_name, origin_of

from .models import (
    Device,
    Measurement,
    MeasurementType,
    Mesh,
    Owner,
    PostprocessingBlueprint,
    PostprocessingStep,
    Sensor,
    TelemetryVariant,
    TelemetryVariantMeasurement,
)


class OwnerPublicSerializer(serializers.ModelSerializer):
    """Public directory"""

    class Meta:
        model = Owner
        fields = ["id", "name", "is_superuser", "created_at"]
        read_only_fields = fields


class OwnerReadSerializer(serializers.ModelSerializer):
    """Includes email"""

    class Meta:
        model = Owner
        fields = ["id", "name", "email", "is_superuser", "is_active", "email_verified_at", "created_at"]
        read_only_fields = fields


class OwnerCreateSerializer(serializers.ModelSerializer):
    email = serializers.EmailField(validators=[])

    class Meta:
        model = Owner
        fields = ["name", "email"]


class OwnerUpdateSerializer(serializers.ModelSerializer):
    email = serializers.EmailField(validators=[], required=False)

    class Meta:
        model = Owner
        fields = ["name", "email"]
        extra_kwargs = {"name": {"required": False}}


class OwnerWithApiKeySerializer(OwnerReadSerializer):
    """In POST /owners and POST /owners/me/rotate-key"""

    api_key = serializers.CharField(read_only=True)

    class Meta(OwnerReadSerializer.Meta):
        fields = OwnerReadSerializer.Meta.fields + ["api_key"]


# Password field: write-only everywhere
# TODO - FIX, should be returned to owner (although not admin)
_PSK_HELP = "Base64-encoded channel PSK"


class MeshSerializer(serializers.ModelSerializer):

    owner_id = serializers.UUIDField(read_only=True)
    # blank psk_b64 is mesh without encryption"
    psk_b64 = serializers.CharField(write_only=True, allow_blank=True, help_text=_PSK_HELP)

    class Meta:
        model = Mesh
        fields = ["id", "channel_id", "psk_b64", "label", "owner_id", "created_at", "updated_at"]
        read_only_fields = ["id", "created_at", "updated_at"]


class MeshUpdateSerializer(serializers.ModelSerializer):
    """PUT /meshes/{id}: only psk_b64 and label are editable."""

    psk_b64 = serializers.CharField(write_only=True, required=False, allow_blank=True, help_text=_PSK_HELP)

    class Meta:
        model = Mesh
        fields = ["psk_b64", "label"]
        extra_kwargs = {"label": {"required": False}}


def _max_three_admin_keys(value):
    if value is not None and len(value) > 3:
        raise serializers.ValidationError(
            "Meshtastic firmware supports at most 3 admin keys (Config.SecurityConfig.admin_key)"
        )
    return value


# Password field like Mesh.psk_b64
# TODO - FIX, should be returned to owner (although not to admin)
_ADMIN_KEYS_HELP = "Base64-encoded admin keys (up to 3). Warning: PUT replaces the whole list."


def _validate_location_pair(serializer, attrs):
    """latitude/longitude are always set/cleared together"""
    has_lat = "latitude" in serializer.initial_data
    has_lon = "longitude" in serializer.initial_data
    if has_lat != has_lon:
        raise serializers.ValidationError("latitude and longitude must be provided together")
    return attrs


class DeviceCreateSerializer(serializers.Serializer):
    """POST /devices input"""

    device_id = serializers.IntegerField()
    mesh_id = serializers.UUIDField()
    is_gateway = serializers.BooleanField(default=False)
    is_node = serializers.BooleanField(default=False)
    label = serializers.CharField(required=False, allow_null=True, allow_blank=True, default=None)
    admin_keys_b64 = serializers.ListField(
        child=serializers.CharField(),
        write_only=True,
        required=False,
        default=list,
        validators=[_max_three_admin_keys],
        help_text=_ADMIN_KEYS_HELP,
    )
    latitude = serializers.FloatField(required=False, allow_null=True, default=None)
    longitude = serializers.FloatField(required=False, allow_null=True, default=None)
    name = serializers.CharField(required=False, allow_null=True, allow_blank=True, default=None)
    short_name = serializers.CharField(required=False, allow_null=True, allow_blank=True, default=None)
    hardware_type = serializers.CharField(required=False, allow_null=True, allow_blank=True, default=None)
    role = serializers.CharField(required=False, allow_null=True, allow_blank=True, default=None)

    def validate(self, attrs):
        if attrs.get("is_gateway", False) == attrs.get("is_node", False):
            raise serializers.ValidationError("Exactly one of is_gateway or is_node must be true")
        return _validate_location_pair(self, attrs)


class DeviceReadSerializer(serializers.ModelSerializer):
    # TODO Fix - For the owner, they should be able to see admin_keys (not for admin)
    """Never returns admin_keys_b64"""

    mesh_id = serializers.UUIDField(read_only=True)
    postprocessing_blueprint_id = serializers.UUIDField(read_only=True)

    class Meta:
        model = Device
        fields = [
            "id",
            "device_id",
            "mesh_id",
            "label",
            "is_gateway",
            "is_node",
            "is_allowed",
            "latitude",
            "longitude",
            "location_overridden",
            "name",
            "short_name",
            "hardware_type",
            "role",
            "nodeinfo_overridden",
            "postprocessing_blueprint_id",
            "reading_interval_seconds",
            "publish_interval_seconds",
            "last_seen",
            "created_at",
            "updated_at",
        ]
        read_only_fields = fields


class DeviceUpdateSerializer(serializers.ModelSerializer):
    # TODO - Fix, role can be updated if the user changes it on the device itself
    """device_id/mesh/role are fixed at creation"""

    admin_keys_b64 = serializers.ListField(
        child=serializers.CharField(),
        write_only=True,
        required=False,
        validators=[_max_three_admin_keys],
        help_text=_ADMIN_KEYS_HELP,
    )

    # Ownership is checked in the view (the blueprint must belong to the same
    # owner as the device's mesh).
    postprocessing_blueprint_id = serializers.PrimaryKeyRelatedField(
        source="postprocessing_blueprint",
        queryset=PostprocessingBlueprint.objects.all(),
        required=False,
        allow_null=True,
    )

    class Meta:
        model = Device
        fields = [
            "label",
            "admin_keys_b64",
            "is_allowed",
            "latitude",
            "longitude",
            "name",
            "short_name",
            "hardware_type",
            "role",
            "postprocessing_blueprint_id",
            "reading_interval_seconds",
            "publish_interval_seconds",
        ]
        extra_kwargs = {
            "label": {"required": False},
            "is_allowed": {"required": False},
            "reading_interval_seconds": {"required": False},
            "publish_interval_seconds": {"required": False},
        }

    def validate(self, attrs):
        return _validate_location_pair(self, attrs)


class MeasurementTypeReadSerializer(serializers.ModelSerializer):
    """Freely readable by any verified owner."""

    origin = serializers.SerializerMethodField() # "measured" | "derived" | "quality"
    channel = serializers.SerializerMethodField()

    class Meta:
        model = MeasurementType
        fields = [
            "id", "payload_kind", "field_name", "channel", "origin", "display_name",
            "description", "native_unit", "quantity_kind", "vocabulary_uri",
            "created_at", "updated_at",
        ]
        read_only_fields = fields

    def get_origin(self, obj) -> str:
        return origin_of(obj.payload_kind)

    def get_channel(self, obj) -> str:
        return channel_name(obj.payload_kind, obj.field_name)


class MeasurementTypeCreateSerializer(serializers.ModelSerializer):
    """POST /measurement-types - superuser-only"""

    class Meta:
        model = MeasurementType
        fields = [
            "payload_kind", "field_name", "display_name", "description",
            "native_unit", "quantity_kind", "vocabulary_uri",
        ]
        validators = [] # Avoid DRF's auto UniqueTogetherValidator


class MeasurementTypeUpdateSerializer(serializers.ModelSerializer):
    """PUT /measurement-types/{id} - payload_kind/field_name are immutable"""

    class Meta:
        model = MeasurementType
        fields = ["display_name", "description", "native_unit", "quantity_kind", "vocabulary_uri"]
        extra_kwargs = {field: {"required": False} for field in fields}


class TelemetryVariantReadSerializer(serializers.ModelSerializer):
    class Meta:
        model = TelemetryVariant
        fields = [
            "id", "payload_kind", "name", "model", "datasheet_url", "description",
            "default_reading_interval_seconds", "created_at", "updated_at",
        ]
        read_only_fields = fields


class TelemetryVariantCreateSerializer(serializers.ModelSerializer):
    """POST /telemetry-variants - superuser-only"""

    payload_kind = serializers.CharField(validators=[])

    class Meta:
        model = TelemetryVariant
        fields = ["payload_kind", "name", "model", "datasheet_url", "description"]


class TelemetryVariantUpdateSerializer(serializers.ModelSerializer):
    """PUT /telemetry-variants/{id} - payload_kind is immutable"""

    class Meta:
        model = TelemetryVariant
        fields = ["name", "model", "datasheet_url", "description", "default_reading_interval_seconds"]
        extra_kwargs = {field: {"required": False} for field in fields}


class TelemetryVariantMeasurementReadSerializer(serializers.ModelSerializer):
    """GET /telemetry-variants/{id}/measurement-types.  Readable by
    any verified owner, same as MeasurementTypeReadSerializer."""

    measurement_type = MeasurementTypeReadSerializer(read_only=True)

    class Meta:
        model = TelemetryVariantMeasurement
        fields = ["id", "measurement_type", "valid_min", "valid_max", "created_at", "updated_at"]
        read_only_fields = fields


class TelemetryVariantMeasurementUpdateSerializer(serializers.ModelSerializer):
    """PUT /telemetry-variant-measurements/{id}. Only the curated
    plausibility range is editable"""

    class Meta:
        model = TelemetryVariantMeasurement
        fields = ["valid_min", "valid_max"]
        extra_kwargs = {field: {"required": False} for field in fields}


class DeviceTelemetryVariantSerializer(serializers.ModelSerializer):
    """GET /devices/{id}/telemetry-variants - which telemetry types this device
    has actually been seen reporting. Read-only (auto-discovered)"""

    telemetry_variant = TelemetryVariantReadSerializer(read_only=True)

    class Meta:
        model = Sensor
        fields = ["id", "telemetry_variant", "first_seen", "last_seen"]
        read_only_fields = fields


class DeviceMeasurementSerializer(serializers.ModelSerializer):
    """GET /devices/{id}/measurements - a flat list of the channels for this device"""

    measurement_type = MeasurementTypeReadSerializer(read_only=True)
    payload_kind = serializers.CharField(source="measurement_type.payload_kind", read_only=True)
    channel = serializers.SerializerMethodField()
    origin = serializers.SerializerMethodField()
    telemetry_variant_id = serializers.UUIDField(source="sensor.telemetry_variant_id", read_only=True)

    class Meta:
        model = Measurement
        fields = [
            "id",
            "payload_kind",
            "channel",
            "origin",
            "telemetry_variant_id",
            "measurement_type",
            "first_seen",
            "last_seen",
        ]
        read_only_fields = fields

    def get_channel(self, obj) -> str:
        return channel_name(obj.measurement_type.payload_kind, obj.measurement_type.field_name)

    def get_origin(self, obj) -> str:
        return origin_of(obj.measurement_type.payload_kind)


class PostprocessingStepSerializer(serializers.ModelSerializer):
    """Validates each step against common/algorithms"""

    class Meta:
        model = PostprocessingStep
        fields = ["id", "order", "function_name", "inputs", "params", "outputs"]
        read_only_fields = ["id"]

    def validate(self, attrs):
        errors = validate_step(attrs.get("function_name"), attrs.get("inputs"), attrs.get("params"))
        if errors:
            raise serializers.ValidationError({"function_name": errors})
        return attrs


class PostprocessingBlueprintSerializer(serializers.ModelSerializer):
    # TODO - Fix, steps should be orderable by their inputs and outputs as a tree
    """Steps are nested in read and write"""

    steps = PostprocessingStepSerializer(many=True)
    owner_id = serializers.UUIDField(read_only=True)

    class Meta:
        model = PostprocessingBlueprint
        fields = ["id", "name", "description", "owner_id", "steps", "created_at", "updated_at"]
        read_only_fields = ["id", "owner_id", "created_at", "updated_at"]
        validators = []

    def validate_steps(self, steps):
        if not steps:
            raise serializers.ValidationError("A blueprint needs at least one step")
        orders = [step["order"] for step in steps]
        if len(set(orders)) != len(orders):
            raise serializers.ValidationError("Step `order` values must be unique within a blueprint")
        return steps

    def create(self, validated_data):
        steps = validated_data.pop("steps")
        blueprint = PostprocessingBlueprint.objects.create(**validated_data)
        self._replace_steps(blueprint, steps)
        return blueprint

    def update(self, instance, validated_data):
        steps = validated_data.pop("steps", None)
        for field, value in validated_data.items():
            setattr(instance, field, value)
        instance.save()
        if steps is not None:
            self._replace_steps(instance, steps)
        return instance

    @staticmethod
    def _replace_steps(blueprint, steps) -> None:
        blueprint.steps.all().delete()
        PostprocessingStep.objects.bulk_create(
            [PostprocessingStep(blueprint=blueprint, **step) for step in steps]
        )
