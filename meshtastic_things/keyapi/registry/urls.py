from django.urls import path

from . import views

urlpatterns = [
    path("health", views.HealthView.as_view()),
    path("metrics", views.MetricsView.as_view()),
    path("owners", views.OwnerListCreateView.as_view()),
    path("owners/csrf-cookie", views.CsrfCookieView.as_view()),
    path("owners/login", views.OwnerLoginView.as_view()),
    path("owners/logout", views.OwnerLogoutView.as_view()),
    path("owners/request-password-reset", views.OwnerRequestPasswordResetView.as_view()),
    path("owners/reset-password", views.OwnerResetPasswordView.as_view()),
    path("owners/verify-email", views.OwnerVerifyEmailView.as_view()),
    path("owners/me", views.OwnerMeView.as_view()),
    path("owners/me/rotate-key", views.OwnerRotateKeyView.as_view()),
    path("owners/me/resend-verification", views.OwnerResendVerificationView.as_view()),
    path("owners/me/change-password", views.OwnerChangePasswordView.as_view()),
    path("owners/me/devices", views.OwnerDeviceIdsView.as_view()),
    path("owners/<uuid:owner_id>", views.OwnerDetailView.as_view()),
    path("meshes", views.MeshListCreateView.as_view()),
    path("meshes/<uuid:mesh_id>", views.MeshDetailView.as_view()),
    path("meshes/<uuid:mesh_id>/devices", views.MeshDevicesView.as_view()),
    path("devices", views.DeviceListCreateView.as_view()),
    path("devices/<uuid:id>", views.DeviceDetailView.as_view()),
    path("devices/<uuid:id>/telemetry-variants", views.DeviceTelemetryVariantsView.as_view()),
    path("devices/<uuid:id>/measurements", views.DeviceMeasurementsView.as_view()),
    path("measurement-types", views.MeasurementTypeListCreateView.as_view()),
    path("measurement-types/<uuid:measurement_type_id>", views.MeasurementTypeDetailView.as_view()),
    path("telemetry-variants", views.TelemetryVariantListCreateView.as_view()),
    path("telemetry-variants/<uuid:telemetry_variant_id>", views.TelemetryVariantDetailView.as_view()),
    path(
        "telemetry-variants/<uuid:telemetry_variant_id>/measurement-types",
        views.TelemetryVariantMeasurementsView.as_view(),
    ),
    path(
        "telemetry-variant-measurements/<uuid:telemetry_variant_measurement_id>",
        views.TelemetryVariantMeasurementDetailView.as_view(),
    ),
    path("algorithms", views.AlgorithmListView.as_view()),
    path("postprocessing-blueprints", views.PostprocessingBlueprintListCreateView.as_view()),
    path("postprocessing-blueprints/<uuid:blueprint_id>", views.PostprocessingBlueprintDetailView.as_view()),
]
