from rest_framework import status
from rest_framework.exceptions import NotFound, ValidationError
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from common.channels import channel_name, origin_of_measurement

from .influx_client import get_client
from .measurement_catalog import get_measurement_types
from .serializers import LatestReadingsResponseSerializer, TimeseriesResponseSerializer


def _forwarded_auth_header(request) -> dict:
    """Reconstructs the caller's Authorization header for calling keyapi
    GET /measurement-types"""
    header = request.META.get("HTTP_AUTHORIZATION")
    if header:
        return {"Authorization": header}
    access_token = request.query_params.get("access_token")
    return {"Authorization": f"Bearer {access_token}"} if access_token else {}


def _require_owned_device(request, mesh_id, node_id: int) -> None:
    """Can't distinguish "doesn't exist" from "not yours"."""
    if not request.user.is_superuser and (str(mesh_id), node_id) not in request.user.devices:
        raise NotFound("Device not found")


def _resolve_device_uuid(request, device_uuid) -> tuple[str, int]:
    resolved = getattr(request.user, "devices_by_uuid", {}).get(str(device_uuid))
    if resolved is None:
        raise NotFound("Device not found")
    return resolved


class HealthView(APIView):
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
    """If no ?measurement= returns latest values across every field reported"""

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


class DeviceTimeseriesView(TimeseriesView):
    """Same query as TimeseriesView, addressed by keyapi's device UUID."""

    def get(self, request, device_uuid):
        mesh_id, node_id = _resolve_device_uuid(request, device_uuid)
        return super().get(request, mesh_id, node_id)


class DeviceLatestView(LatestView):
    """Same query as LatestView, addressed by keyapi's device UUID."""

    def get(self, request, device_uuid):
        mesh_id, node_id = _resolve_device_uuid(request, device_uuid)
        return super().get(request, mesh_id, node_id)
