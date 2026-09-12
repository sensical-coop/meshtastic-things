from rest_framework import status
from rest_framework.exceptions import NotFound, ValidationError
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from common.channels import channel_name, origin_of_measurement

from .influx_client import get_client
from .measurement_catalog import get_measurement_types
from .serializers import (
    DeviceMetricsResponseSerializer,
    LatestReadingsResponseSerializer,
    TimeseriesResponseSerializer,
)


def _forwarded_auth_header(request) -> dict:
    """Rebuild the caller's Authorization header for the measurement catalog lookup."""
    header = request.META.get("HTTP_AUTHORIZATION")
    if header:
        return {"Authorization": header}
    access_token = request.query_params.get("access_token")
    return {"Authorization": f"Bearer {access_token}"} if access_token else {}


def _require_owned_device(request, mesh_id, node_id: int) -> None:
    """Reject a device the caller does not own, without revealing whether it exists."""
    if not request.user.is_superuser and (str(mesh_id), node_id) not in request.user.devices:
        raise NotFound("Device not found")


def _resolve_device_uuid(request, device_uuid) -> tuple[str, int]:
    resolved = getattr(request.user, "devices_by_uuid", {}).get(str(device_uuid))
    if resolved is None:
        raise NotFound("Device not found")
    return resolved


class HealthView(APIView):
    """Report whether the service and the timeseries database are reachable."""

    # Infra probe, not sensitive data.
    authentication_classes = []
    permission_classes = [AllowAny]
    throttle_classes = []

    def get(self, request):
        try:
            if not get_client().ping():
                raise RuntimeError("InfluxDB ping returned false")
        except Exception as e:
            return Response({"detail": f"InfluxDB unavailable: {e}"}, status=status.HTTP_503_SERVICE_UNAVAILABLE)
        return Response({"status": "ok"})


class TimeseriesView(APIView):
    """Return a device's readings for one measurement over a time range.

    Downsampling covers numeric fields only, so text fields such as
    `timestamp_quality` are left out whenever `window` is set.

    Args:
        measurement: Measurement to read, for example `environment_metrics`. Required.
        field: Single field within the measurement. Defaults to every field.
        start: Start of the range, as a relative duration such as `-24h`, a date
            such as `2026-09-01`, or a timestamp such as `2026-09-01T13:45:00Z`.
            Defaults to `-1h`.
        stop: End of the range, in the same forms as `start`, or `now()`.
            Defaults to `now()`.
        window: Bucket size to downsample into, for example `6h`. Off by default.
        agg: Aggregate applied to each bucket, one of `mean`, `max`, `min`,
            `last`, `first`, `sum` or `count`. Defaults to `mean` and applies
            only when `window` is set.
    """

    def get(self, request, mesh_id, node_id):
        _require_owned_device(request, mesh_id, node_id)
        params = request.query_params
        measurement = params.get("measurement")
        if not measurement:
            raise ValidationError("measurement is required")
        try:
            series = get_client().timeseries(
                mesh_id,
                node_id,
                measurement,
                params.get("field"),
                params.get("start", "-1h"),
                params.get("stop", "now()"),
                params.get("window"),
                params.get("agg", "mean"),
            )
        except ValueError as e:
            raise ValidationError(str(e))
        body = {"mesh_id": mesh_id, "node_id": node_id, "measurement": measurement, "series": series}
        return Response(TimeseriesResponseSerializer(body).data)


class LatestView(APIView):
    """Return a device's most recent readings.

    Without `measurement`, every field the device has reported comes back in
    one call, each annotated with its unit from the measurement catalog.

    Args:
        measurement: Restrict to one measurement. Defaults to every measurement.
        lookback: How far back to search for a value, as a relative duration, a
            date or a timestamp. Defaults to `-30d`.
    """

    def get(self, request, mesh_id, node_id):
        _require_owned_device(request, mesh_id, node_id)
        params = request.query_params
        measurement = params.get("measurement")
        lookback = params.get("lookback", "-30d")
        try:
            if measurement:
                series = get_client().latest(mesh_id, node_id, measurement, lookback=lookback)
                body = {"mesh_id": mesh_id, "node_id": node_id, "measurement": measurement, "series": series}
                return Response(TimeseriesResponseSerializer(body).data)
            readings = get_client().latest_all(mesh_id, node_id, lookback=lookback)
        except ValueError as e:
            raise ValidationError(str(e))

        catalog = get_measurement_types(_forwarded_auth_header(request))
        decorated = []
        for reading in readings:
            meta = catalog.get(channel_name(reading["measurement"], reading["field"]), {})
            decorated.append(
                {
                    **reading,
                    "unit": meta.get("unit"),
                    "quantity_kind": meta.get("quantity_kind"),
                    "vocabulary_uri": meta.get("vocabulary_uri"),
                    "origin": origin_of_measurement(reading["measurement"]),
                }
            )
        body = {"mesh_id": mesh_id, "node_id": node_id, "readings": decorated}
        return Response(LatestReadingsResponseSerializer(body).data)


class MetricsView(APIView):
    """Return data-quality figures for a device over a time range.

    Covers the number of readings recorded, alarms raised per detector, the
    share of readings within their plausible range, and how complete each
    channel was. Figures stay empty until the quality checks have run for the
    device.

    Args:
        start: Start of the range, as a relative duration such as `-24h`, a date
            such as `2026-09-01`, or a timestamp such as `2026-09-01T13:45:00Z`.
            Defaults to `-1h`.
        stop: End of the range, in the same forms as `start`, or `now()`.
            Defaults to `now()`.
    """

    def get(self, request, mesh_id, node_id):
        _require_owned_device(request, mesh_id, node_id)
        params = request.query_params
        start = params.get("start", "-1h")
        stop = params.get("stop", "now()")
        try:
            metrics = get_client().device_metrics(mesh_id, node_id, start, stop)
        except ValueError as e:
            raise ValidationError(str(e))
        body = {"mesh_id": mesh_id, "node_id": node_id, "start": start, "stop": stop, **metrics}
        return Response(DeviceMetricsResponseSerializer(body).data)


class DeviceMetricsView(MetricsView):
    """Data-quality figures for a device, addressed by its registration ID."""

    def get(self, request, device_uuid):
        mesh_id, node_id = _resolve_device_uuid(request, device_uuid)
        return super().get(request, mesh_id, node_id)


class DeviceTimeseriesView(TimeseriesView):
    """Readings over a time range, addressed by the device's registration ID."""

    def get(self, request, device_uuid):
        mesh_id, node_id = _resolve_device_uuid(request, device_uuid)
        return super().get(request, mesh_id, node_id)


class DeviceLatestView(LatestView):
    """Most recent readings, addressed by the device's registration ID."""

    def get(self, request, device_uuid):
        mesh_id, node_id = _resolve_device_uuid(request, device_uuid)
        return super().get(request, mesh_id, node_id)
