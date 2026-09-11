from django.contrib import admin
from django.contrib.auth.admin import UserAdmin

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
)

@admin.register(Owner)
class OwnerAdmin(UserAdmin):
    # api_key_hash excluded
    fieldsets = (
        (None, {"fields": ("email", "password")}),
        ("Profile", {"fields": ("name",)}),
        ("Permissions", {"fields": ("is_active", "is_staff", "is_superuser", "groups", "user_permissions")}),
        ("Dates", {"fields": ("last_login",)}),
    )
    add_fieldsets = ((None, {"classes": ("wide",), "fields": ("email", "name", "password1", "password2")}),)
    list_display = ("email", "name", "is_superuser", "is_staff", "created_at")
    search_fields = ("email", "name")
    ordering = ("email",)
    readonly_fields = ("last_login",)


@admin.register(Mesh)
class MeshAdmin(admin.ModelAdmin):
    # psk_b64 is a password field
    list_display = ("channel_id", "label", "owner", "created_at")
    exclude = ("psk_b64",)
    readonly_fields = ("id", "created_at", "updated_at")
    search_fields = ("channel_id", "label")


@admin.register(Device)
class DeviceAdmin(admin.ModelAdmin):
    # admin_keys_b64 is a password field
    list_display = (
        "device_id",
        "label",
        "mesh",
        "is_gateway",
        "is_allowed",
        "name",
        "short_name",
        "hardware_type",
        "role",
        "latitude",
        "longitude",
        "last_seen",
        "created_at",
    )
    list_filter = ("is_gateway", "is_allowed")
    exclude = ("admin_keys_b64",)
    readonly_fields = ("id", "created_at", "updated_at")
    search_fields = ("device_id", "label")


@admin.register(MeasurementType)
class MeasurementTypeAdmin(admin.ModelAdmin):
    list_display = ("payload_kind", "field_name", "native_unit", "quantity_kind", "vocabulary_uri", "updated_at")
    list_filter = ("quantity_kind",)
    readonly_fields = ("id", "created_at", "updated_at")
    search_fields = ("payload_kind", "field_name", "display_name")


class PostprocessingStepInline(admin.TabularInline):
    # Steps are only meaningful as part of their blueprint,
    # so they're edited inline rather than as a standalone admin model.
    model = PostprocessingStep
    extra = 0


@admin.register(PostprocessingBlueprint)
class PostprocessingBlueprintAdmin(admin.ModelAdmin):
    list_display = ("name", "owner", "created_at", "updated_at")
    readonly_fields = ("id", "created_at", "updated_at")
    search_fields = ("name", "owner__email")
    inlines = [PostprocessingStepInline]


@admin.register(TelemetryVariant)
class TelemetryVariantAdmin(admin.ModelAdmin):
    list_display = ("payload_kind", "name", "model", "datasheet_url", "updated_at")
    readonly_fields = ("id", "created_at", "updated_at")
    search_fields = ("payload_kind", "name", "model")


@admin.register(Sensor)
class SensorAdmin(admin.ModelAdmin):
    # TODO - Needs fixing.
    list_display = ("device", "telemetry_variant", "first_seen", "last_seen")
    readonly_fields = ("id", "device", "telemetry_variant", "first_seen", "last_seen")
    search_fields = ("device__device_id",)

    def has_add_permission(self, request):
        return False


@admin.register(Measurement)
class MeasurementAdmin(admin.ModelAdmin):
    list_display = ("sensor", "measurement_type", "first_seen", "last_seen")
    readonly_fields = ("id", "sensor", "measurement_type", "first_seen", "last_seen")
    search_fields = ("sensor__device__device_id", "measurement_type__field_name")

    def has_add_permission(self, request):
        return False
