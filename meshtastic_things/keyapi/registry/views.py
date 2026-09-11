from datetime import timedelta

import structlog
from django.conf import settings
from django.db import IntegrityError
from django.db.models import Q
from django.utils import timezone
from rest_framework import status
from rest_framework.exceptions import NotFound, ParseError, PermissionDenied
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from .auth import generate_api_key, hash_api_key
from .emailing import send_verification_email
from .exceptions import BadGateway, Conflict
from .kafka_publisher import get_publisher
from common.algorithms import REGISTRY, discover

from .models import (
    Device,
    Measurement,
    MeasurementType,
    Mesh,
    Owner,
    PostprocessingBlueprint,
    TelemetryVariant,
    TelemetryVariantMeasurement,
)
from .permissions import IsSuperuser, IsVerifiedOwner
from .serializers import (
    DeviceCreateSerializer,
    DeviceReadSerializer,
    DeviceUpdateSerializer,
    MeasurementTypeCreateSerializer,
    MeasurementTypeReadSerializer,
    MeasurementTypeUpdateSerializer,
    MeshSerializer,
    MeshUpdateSerializer,
    OwnerCreateSerializer,
    OwnerPublicSerializer,
    OwnerReadSerializer,
    OwnerUpdateSerializer,
    OwnerWithApiKeySerializer,
    PostprocessingBlueprintSerializer,
    DeviceMeasurementSerializer,
    DeviceTelemetryVariantSerializer,
    TelemetryVariantCreateSerializer,
    TelemetryVariantMeasurementReadSerializer,
    TelemetryVariantMeasurementUpdateSerializer,
    TelemetryVariantReadSerializer,
    TelemetryVariantUpdateSerializer,
)
from .throttling import ResendVerificationThrottle

log = structlog.get_logger()


class HealthView(APIView):
    # Open and unthrottled
    authentication_classes = []
    permission_classes = [AllowAny]
    throttle_classes = []

    def get(self, request):
        try:
            Mesh.objects.exists()
        except Exception as e:
            return Response({"detail": f"Database unavailable: {e}"}, status=status.HTTP_503_SERVICE_UNAVAILABLE)
        return Response({"status": "ok"})


class OwnerListCreateView(APIView):
    """POST /owners is the only unauthenticated write on this API"""

    permission_classes = [AllowAny]

    def get(self, request):
        owners = Owner.objects.all()
        return Response(OwnerPublicSerializer(owners, many=True).data)

    def post(self, request):
        serializer = OwnerCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        api_key = generate_api_key()
        verification_token = generate_api_key()
        try:
            owner = Owner.objects.create_user(
                email=serializer.validated_data["email"],
                name=serializer.validated_data["name"],
                api_key_hash=hash_api_key(api_key),
                is_active=False,
                email_verification_token_hash=hash_api_key(verification_token),
                email_verification_sent_at=timezone.now(),
            )
        except IntegrityError:
            raise Conflict(f"Owner with email {serializer.validated_data['email']} already exists")
        # api_key is never persisted or retrievable again
        owner.api_key = api_key
        try:
            send_verification_email(owner, verification_token)
        except Exception as e:
            # Signup already succeeded, resend-verification can recover.
            log.warning("verification_email_send_failed", owner_id=str(owner.id), error=str(e))
        return Response(OwnerWithApiKeySerializer(owner).data, status=status.HTTP_201_CREATED)


class OwnerVerifyEmailView(APIView):
    """GET /owners/verify-email?token=<token> - the emailed link, idempotent once
    verified"""

    authentication_classes = []
    permission_classes = [AllowAny]

    def get(self, request):
        token = request.query_params.get("token")
        if not token:
            raise ParseError("token query parameter is required")
        owner = Owner.objects.filter(email_verification_token_hash=hash_api_key(token)).first()
        if owner is not None and owner.is_active:
            return Response({"detail": "Email already verified"})
        ttl = timedelta(hours=settings.EMAIL_VERIFICATION_TTL_HOURS)
        expired = owner is None or owner.email_verification_sent_at is None or (
            timezone.now() > owner.email_verification_sent_at + ttl
        )
        if expired:
            raise ParseError(
                "Invalid or expired verification link - request a new one via "
                "POST /owners/me/resend-verification"
            )
        owner.is_active = True
        owner.email_verified_at = timezone.now()
        owner.save(update_fields=["is_active", "email_verified_at"])
        return Response({"detail": "Email verified"})


class OwnerResendVerificationView(APIView):
    """For IsAuthenticated owners only"""

    permission_classes = [IsAuthenticated]
    throttle_classes = [ResendVerificationThrottle]

    def post(self, request):
        owner = request.user
        if owner.is_active:
            return Response({"detail": "Email already verified"})
        verification_token = generate_api_key()
        owner.email_verification_token_hash = hash_api_key(verification_token)
        owner.email_verification_sent_at = timezone.now()
        owner.save(update_fields=["email_verification_token_hash", "email_verification_sent_at"])
        try:
            send_verification_email(owner, verification_token)
        except Exception as e:
            log.warning("verification_email_send_failed", owner_id=str(owner.id), error=str(e))
            raise BadGateway("Failed to send verification email, try again shortly")
        return Response({"detail": "Verification email sent"})


class OwnerDetailView(APIView):
    """Detail view similar to OwnerListCreateView.get"""

    permission_classes = [AllowAny]

    def get(self, request, owner_id):
        try:
            owner = Owner.objects.get(pk=owner_id)
        except (Owner.DoesNotExist, ValueError, TypeError):
            raise NotFound("Owner not found")
        return Response(OwnerPublicSerializer(owner).data)


class OwnerMeView(APIView):
    """GET /owners/me with token auth"""

    permission_classes = [IsAuthenticated]

    def get(self, request):
        data = OwnerReadSerializer(request.user).data
        data["devices"] = _devices_for_owner(request.user)
        return Response(data)

    def put(self, request):
        """Changing email resets verification (is_active -> False and a new link
        sent)"""
        email_changing = "email" in request.data and request.data["email"] != request.user.email
        serializer = OwnerUpdateSerializer(request.user, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        try:
            owner = serializer.save()
        except IntegrityError:
            raise Conflict(f"Email {request.data.get('email')} already in use")
        if email_changing:
            verification_token = generate_api_key()
            owner.is_active = False
            owner.email_verified_at = None
            owner.email_verification_token_hash = hash_api_key(verification_token)
            owner.email_verification_sent_at = timezone.now()
            owner.save(
                update_fields=[
                    "is_active",
                    "email_verified_at",
                    "email_verification_token_hash",
                    "email_verification_sent_at",
                ]
            )
            try:
                send_verification_email(owner, verification_token)
            except Exception as e:
                # TODO Fix - Send failure is logged only
                log.warning("verification_email_send_failed", owner_id=str(owner.id), error=str(e))
        return Response(OwnerReadSerializer(owner).data)

    def delete(self, request):
        """Publishes delete/unreject for every device before removing owner"""
        devices = Device.objects.filter(mesh__owner=request.user)
        gateways = list(devices.filter(is_gateway=True, is_allowed=True))
        rejected_nodes = list(devices.filter(is_gateway=False, is_allowed=False))
        publisher = get_publisher()
        for gateway in gateways:
            try:
                publisher.publish_gateway_delete(gateway)
            except Exception as e:
                raise BadGateway(f"Failed to publish delete for device {gateway.id}, owner NOT removed: {e}")
        for node in rejected_nodes:
            try:
                publisher.publish_node_unreject(node)
            except Exception as e:
                raise BadGateway(f"Failed to publish unreject for device {node.id}, owner NOT removed: {e}")
        api_key_hash = request.user.api_key_hash
        request.user.delete()
        if api_key_hash:
            # Best-effort: queryapi's cache would fall off on its own within 60s
            try:
                publisher.publish_key_revoked(api_key_hash)
            except Exception as e:
                log.warning("owner_auth_revocation_publish_failed", error=str(e))
        return Response(status=status.HTTP_204_NO_CONTENT)


class OwnerRotateKeyView(APIView):
    """Creates a new API key, invalidating the old one immediately"""

    permission_classes = [IsVerifiedOwner]

    def post(self, request):
        old_api_key_hash = request.user.api_key_hash
        new_api_key = generate_api_key()
        request.user.api_key_hash = hash_api_key(new_api_key)
        request.user.save(update_fields=["api_key_hash"])
        request.user.api_key = new_api_key
        if old_api_key_hash:
            # Best-effort: queryapi's cache would fall off on its own within 60s
            try:
                get_publisher().publish_key_revoked(old_api_key_hash)
            except Exception as e:
                log.warning("owner_auth_revocation_publish_failed", owner_id=str(request.user.id), error=str(e))
        return Response(OwnerWithApiKeySerializer(request.user).data)


def _devices_for_owner(owner: Owner) -> list[dict]:
    """Every Device the given owner may see"""
    # TODO - Fix, a superuser can query any owner's devices
    qs = Device.objects.all() if owner.is_superuser else Device.objects.filter(mesh__owner_id=owner.id)
    return DeviceReadSerializer(qs.order_by("mesh_id", "device_id"), many=True).data


class OwnerDeviceIdsView(APIView):
    """Every Device the caller may see"""
    permission_classes = [IsVerifiedOwner]

    def get(self, request):
        return Response({"devices": _devices_for_owner(request.user)})


def _get_mesh_or_404(mesh_id) -> Mesh:
    try:
        return Mesh.objects.get(pk=mesh_id)
    except (Mesh.DoesNotExist, ValueError, TypeError):
        raise NotFound("Mesh not found")


def _require_mesh_ownership(mesh: Mesh, owner: Owner) -> None:
    if not owner.is_superuser and mesh.owner_id != owner.id:
        raise PermissionDenied("You do not own this mesh")


def _get_device_or_404(id) -> Device:
    try:
        return Device.objects.get(pk=id)
    except (Device.DoesNotExist, ValueError, TypeError):
        raise NotFound("Device not found")


class MeshListCreateView(APIView):
    permission_classes = [IsVerifiedOwner]

    def get(self, request):
        """Every owner's meshes for a superuser, otherwise only the caller's own."""
        # TODO - Fix, a superuser can query any owner's meshes
        qs = Mesh.objects.all() if request.user.is_superuser else Mesh.objects.filter(owner=request.user)
        return Response(MeshSerializer(qs, many=True).data)

    def post(self, request):
        serializer = MeshSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        mesh = serializer.save(owner=request.user)
        return Response(MeshSerializer(mesh).data, status=status.HTTP_201_CREATED)


class MeshDetailView(APIView):
    permission_classes = [IsVerifiedOwner]

    def get(self, request, mesh_id):
        mesh = _get_mesh_or_404(mesh_id)
        _require_mesh_ownership(mesh, request.user)
        return Response(MeshSerializer(mesh).data)

    def put(self, request, mesh_id):
        """Changing psk_b64 re-publishes an upsert for every allowed gateway on
        this mesh; any publish failure rolls the whole update back"""
        mesh = _get_mesh_or_404(mesh_id)
        _require_mesh_ownership(mesh, request.user)
        changing_psk = "psk_b64" in request.data
        previous_psk = mesh.psk_b64
        serializer = MeshUpdateSerializer(mesh, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        updated = serializer.save()
        if changing_psk:
            publisher = get_publisher()
            for gateway in updated.devices.filter(is_gateway=True, is_allowed=True):
                try:
                    publisher.publish_gateway_upsert(gateway, updated)
                except Exception as e:
                    updated.psk_b64 = previous_psk
                    updated.save(update_fields=["psk_b64"])
                    raise BadGateway(f"Failed to publish key change, rolled back: {e}")
        return Response(MeshSerializer(updated).data)

    def delete(self, request, mesh_id):
        """Publishes delete/unreject and, if it doesn't fail,
        then cascades (DB FK CASCADE) to every device on the mesh."""
        mesh = _get_mesh_or_404(mesh_id)
        _require_mesh_ownership(mesh, request.user)
        gateways = list(mesh.devices.filter(is_gateway=True, is_allowed=True))
        rejected_nodes = list(mesh.devices.filter(is_gateway=False, is_allowed=False))
        publisher = get_publisher()
        for gateway in gateways:
            try:
                publisher.publish_gateway_delete(gateway)
            except Exception as e:
                raise BadGateway(f"Failed to publish delete for device {gateway.id}, mesh NOT removed: {e}")
        for node in rejected_nodes:
            try:
                publisher.publish_node_unreject(node)
            except Exception as e:
                raise BadGateway(f"Failed to publish unreject for device {node.id}, mesh NOT removed: {e}")
        mesh.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)


class MeshDevicesView(APIView):
    """Every device on this mesh, gateway or node"""

    permission_classes = [IsVerifiedOwner]

    def get(self, request, mesh_id):
        mesh = _get_mesh_or_404(mesh_id)
        _require_mesh_ownership(mesh, request.user)
        return Response(DeviceReadSerializer(mesh.devices.all(), many=True).data)


class DeviceListCreateView(APIView):
    permission_classes = [IsVerifiedOwner]

    def get(self, request):
        """Filters:
        - ?mesh_id=,
        - ?is_gateway=,
        - ?is_node=,
        - ?is_allowed=,
        - ?has_postprocessing= what quality_worker uses
        to find devices with a blueprint attached"""
        qs = Device.objects.all() if request.user.is_superuser else Device.objects.filter(mesh__owner=request.user)
        mesh_id = request.query_params.get("mesh_id")
        if mesh_id:
            qs = qs.filter(mesh_id=mesh_id)
        for flag in ("is_gateway", "is_node", "is_allowed"):
            value = request.query_params.get(flag)
            if value is not None:
                qs = qs.filter(**{flag: value.lower() in ("true", "1")})
        has_postprocessing = request.query_params.get("has_postprocessing")
        if has_postprocessing is not None:
            qs = qs.filter(postprocessing_blueprint__isnull=has_postprocessing.lower() not in ("true", "1"))
        return Response(DeviceReadSerializer(qs, many=True).data)

    def post(self, request):
        """Only the mesh owner may register a device. A device needs to be marked first as is_allowed = False to be taken over to another mesh."""
        input_serializer = DeviceCreateSerializer(data=request.data)
        input_serializer.is_valid(raise_exception=True)
        data = input_serializer.validated_data
        mesh = _get_mesh_or_404(data["mesh_id"])
        _require_mesh_ownership(mesh, request.user)
        try:
            device = Device.objects.create(
                device_id=data["device_id"],
                mesh=mesh,
                is_gateway=data["is_gateway"],
                label=data.get("label"),
                admin_keys_b64=data.get("admin_keys_b64", []),
                latitude=data.get("latitude"),
                longitude=data.get("longitude"),
                location_overridden=data.get("latitude") is not None,
                name=data.get("name"),
                short_name=data.get("short_name"),
                hardware_type=data.get("hardware_type"),
                role=data.get("role"),
                nodeinfo_overridden=any(
                    data.get(f) for f in ("name", "short_name", "hardware_type", "role")
                ),
            )
        except IntegrityError:
            raise Conflict(f"Device with device_id {data['device_id']} already has an allowed registration")
        if device.is_gateway:
            try:
                get_publisher().publish_gateway_upsert(device, mesh)
            except Exception as e:
                device.delete()
                raise BadGateway(f"Failed to publish gateway registration, rolled back: {e}")
        return Response(DeviceReadSerializer(device).data, status=status.HTTP_201_CREATED)


class DeviceDetailView(APIView):
    permission_classes = [IsVerifiedOwner]

    def get(self, request, id):
        device = _get_device_or_404(id)
        _require_mesh_ownership(device.mesh, request.user)
        return Response(DeviceReadSerializer(device).data)

    def put(self, request, id):
        """is_allowed, location, and nodeinfo follow the sticky-override
        model"""
        device = _get_device_or_404(id)
        _require_mesh_ownership(device.mesh, request.user)
        changing_allowed = "is_allowed" in request.data
        previous_allowed = device.is_allowed
        location_keys_present = "latitude" in request.data and "longitude" in request.data
        nodeinfo_fields = ("name", "short_name", "hardware_type", "role")
        nodeinfo_keys_present = any(k in request.data for k in nodeinfo_fields)
        nodeinfo_keys_all_present = all(k in request.data for k in nodeinfo_fields)

        serializer = DeviceUpdateSerializer(device, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        # A device may only point at a blueprint its own mesh owner owns
        blueprint = serializer.validated_data.get("postprocessing_blueprint")
        if blueprint is not None and not request.user.is_superuser and blueprint.owner_id != device.mesh.owner_id:
            raise PermissionDenied("You do not own this postprocessing blueprint")
        try:
            updated = serializer.save()
        except IntegrityError:
            raise Conflict(f"Device with device_id {device.device_id} already has an allowed registration")

        override_fields = []
        if location_keys_present:
            pinned = updated.latitude is not None or updated.longitude is not None
            if updated.location_overridden != pinned:
                updated.location_overridden = pinned
                override_fields.append("location_overridden")
        if nodeinfo_keys_present:
            cleared = nodeinfo_keys_all_present and all(getattr(updated, f) is None for f in nodeinfo_fields)
            pinned = not cleared
            if updated.nodeinfo_overridden != pinned:
                updated.nodeinfo_overridden = pinned
                override_fields.append("nodeinfo_overridden")
        if override_fields:
            updated.save(update_fields=override_fields)

        if any(k in request.data for k in ("reading_interval_seconds", "publish_interval_seconds")):
            # Publish device interval (or at least try)
            try:
                get_publisher().publish_device_intervals(updated)
            except Exception as e:
                log.warning("device_intervals_publish_failed", device_id=str(updated.id), error=str(e))

        if changing_allowed and updated.is_allowed != previous_allowed:
            attempted_allowed = updated.is_allowed
            publisher = get_publisher()
            try:
                if updated.is_gateway:
                    if attempted_allowed:
                        publisher.publish_gateway_upsert(updated, updated.mesh)
                    else:
                        publisher.publish_gateway_delete(updated)
                else:
                    if attempted_allowed:
                        publisher.publish_node_unreject(updated)
                    else:
                        publisher.publish_node_reject(updated)
            except Exception as e:
                updated.is_allowed = previous_allowed
                updated.save(update_fields=["is_allowed"])
                action = "allow" if attempted_allowed else "reject"
                raise BadGateway(f"Failed to publish device {action}, rolled back: {e}")
        return Response(DeviceReadSerializer(updated).data)

    def delete(self, request, id):
        """Publishes the matching Kafka delete/unreject first"""
        device = _get_device_or_404(id)
        _require_mesh_ownership(device.mesh, request.user)
        publisher = get_publisher()
        try:
            if device.is_gateway and device.is_allowed:
                publisher.publish_gateway_delete(device)
            elif not device.is_gateway and not device.is_allowed:
                publisher.publish_node_unreject(device)
        except Exception as e:
            raise BadGateway(f"Failed to publish device deletion, device NOT removed: {e}")
        device.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)


class DeviceTelemetryVariantsView(APIView):
    """Which telemetry types this device has been seen reporting.
    This view is read-only and fields auto-discovered."""

    permission_classes = [IsVerifiedOwner]

    def get(self, request, id):
        device = _get_device_or_404(id)
        _require_mesh_ownership(device.mesh, request.user)
        sensors = device.sensors.select_related("telemetry_variant")
        return Response(DeviceTelemetryVariantSerializer(sensors, many=True).data)


class DeviceMeasurementsView(APIView):
    """Every channel this device actually produces.
    This view is read-only and fields auto-discovered."""

    permission_classes = [IsVerifiedOwner]

    def get(self, request, id):
        device = _get_device_or_404(id)
        _require_mesh_ownership(device.mesh, request.user)
        measurements = (
            Measurement.objects.filter(sensor__device=device)
            .select_related("measurement_type", "sensor")
            .order_by("measurement_type__payload_kind", "measurement_type__field_name")
        )
        return Response(DeviceMeasurementSerializer(measurements, many=True).data)


def _require_superuser(request) -> None:
    if not IsSuperuser().has_permission(request, None):
        raise PermissionDenied("Only a superuser may modify this")


def filter_measurement_types(qs, request):
    kind = request.query_params.get("kind")
    if kind:
        qs = qs.filter(Q(payload_kind=kind) | Q(payload_kind__startswith=f"{kind}:"))
    payload_kind = request.query_params.get("payload_kind")
    if payload_kind:
        qs = qs.filter(payload_kind=payload_kind)
    field_name = request.query_params.get("field_name")
    if field_name:
        qs = qs.filter(field_name=field_name)
    return qs


def _get_measurement_type_or_404(measurement_type_id) -> MeasurementType:
    try:
        return MeasurementType.objects.get(pk=measurement_type_id)
    except (MeasurementType.DoesNotExist, ValueError, TypeError):
        raise NotFound("MeasurementType not found")


def _get_telemetry_variant_or_404(telemetry_variant_id) -> TelemetryVariant:
    try:
        return TelemetryVariant.objects.get(pk=telemetry_variant_id)
    except (TelemetryVariant.DoesNotExist, ValueError, TypeError):
        raise NotFound("TelemetryVariant not found")


class MeasurementTypeListCreateView(APIView):
    """Full CRUD on the shared catalog
    GET is open to any verified owner, writes are superuser-only"""

    permission_classes = [IsVerifiedOwner]

    def get(self, request):
        """Filters: ?kind=, ?payload_kind=, ?field_name=.

        `telemetry:<variant>` for real sensor fields
        `derived:<blueprint>` for blueprint outputs

        `?kind=derived` ("just the computed channels") is the common case
        `?payload_kind=telemetry:environment_metrics` ("just the env telemetry channels")
        `?payload_kind=derived:<blueprint>` ("just the derived outputs of a blueprint")
        """
        return Response(
            MeasurementTypeReadSerializer(filter_measurement_types(MeasurementType.objects.all(), request), many=True).data
        )

    def post(self, request):
        _require_superuser(request)
        serializer = MeasurementTypeCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        try:
            measurement_type = serializer.save()
        except IntegrityError:
            raise Conflict(
                f"MeasurementType for {serializer.validated_data['payload_kind']}:"
                f"{serializer.validated_data['field_name']} already exists"
            )
        return Response(MeasurementTypeReadSerializer(measurement_type).data, status=status.HTTP_201_CREATED)


class MeasurementTypeDetailView(APIView):
    permission_classes = [IsVerifiedOwner]

    def get(self, request, measurement_type_id):
        return Response(MeasurementTypeReadSerializer(_get_measurement_type_or_404(measurement_type_id)).data)

    def put(self, request, measurement_type_id):
        """payload_kind/field_name are fixed after creation"""
        _require_superuser(request)
        measurement_type = _get_measurement_type_or_404(measurement_type_id)
        serializer = MeasurementTypeUpdateSerializer(measurement_type, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        updated = serializer.save()
        return Response(MeasurementTypeReadSerializer(updated).data)

    def delete(self, request, measurement_type_id):
        _require_superuser(request)
        _get_measurement_type_or_404(measurement_type_id).delete()
        return Response(status=status.HTTP_204_NO_CONTENT)


class TelemetryVariantListCreateView(APIView):
    """Full CRUD on the shared catalog
    GET is open to any verified owner, writes are superuser-only"""

    permission_classes = [IsVerifiedOwner]

    def get(self, request):
        return Response(TelemetryVariantReadSerializer(TelemetryVariant.objects.all(), many=True).data)

    def post(self, request):
        # TODO - Not really needed
        _require_superuser(request)
        serializer = TelemetryVariantCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        try:
            telemetry_variant = serializer.save()
        except IntegrityError:
            raise Conflict(f"TelemetryVariant for {serializer.validated_data['payload_kind']} already exists")
        return Response(TelemetryVariantReadSerializer(telemetry_variant).data, status=status.HTTP_201_CREATED)


class TelemetryVariantDetailView(APIView):
    permission_classes = [IsVerifiedOwner]

    def get(self, request, telemetry_variant_id):
        return Response(TelemetryVariantReadSerializer(_get_telemetry_variant_or_404(telemetry_variant_id)).data)

    def put(self, request, telemetry_variant_id):
        """payload_kind is fixed after creation"""
        _require_superuser(request)
        telemetry_variant = _get_telemetry_variant_or_404(telemetry_variant_id)
        serializer = TelemetryVariantUpdateSerializer(telemetry_variant, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        updated = serializer.save()
        # default_reading_interval_seconds lives on the variant but is used
        # per-channel, so one edit re-publishes every field under it.
        _publish_quality_config(updated.measurement_links.select_related("measurement_type", "telemetry_variant"))
        return Response(TelemetryVariantReadSerializer(updated).data)

    def delete(self, request, telemetry_variant_id):
        _require_superuser(request)
        _get_telemetry_variant_or_404(telemetry_variant_id).delete()
        return Response(status=status.HTTP_204_NO_CONTENT)


class TelemetryVariantMeasurementsView(APIView):
    """GET /telemetry-variants/{id}/measurement-types
    Open to any verified owner
    Links themselves are created by manage.py seed_measurement_catalog."""

    permission_classes = [IsVerifiedOwner]

    def get(self, request, telemetry_variant_id):
        telemetry_variant = _get_telemetry_variant_or_404(telemetry_variant_id)
        links = telemetry_variant.measurement_links.select_related("measurement_type").all()
        return Response(TelemetryVariantMeasurementReadSerializer(links, many=True).data)


def _publish_quality_config(links) -> None:
    """Best-effort: a failed publish must not fail the API write."""
    publisher = get_publisher()
    for link in links:
        try:
            publisher.publish_variant_measurement_config(link)
        except Exception as e:
            log.warning("quality_config_publish_failed", link_id=str(link.id), error=str(e))


def _get_telemetry_variant_measurement_or_404(telemetry_variant_measurement_id) -> TelemetryVariantMeasurement:
    try:
        return TelemetryVariantMeasurement.objects.get(pk=telemetry_variant_measurement_id)
    except (TelemetryVariantMeasurement.DoesNotExist, ValueError, TypeError):
        raise NotFound("TelemetryVariantMeasurement not found")


class TelemetryVariantMeasurementDetailView(APIView):
    """PUT /telemetry-variant-measurements/{id}
    Change plausible value range, superuser-only."""

    permission_classes = [IsVerifiedOwner]

    def get(self, request, telemetry_variant_measurement_id):
        link = _get_telemetry_variant_measurement_or_404(telemetry_variant_measurement_id)
        return Response(TelemetryVariantMeasurementReadSerializer(link).data)

    def put(self, request, telemetry_variant_measurement_id):
        _require_superuser(request)
        link = _get_telemetry_variant_measurement_or_404(telemetry_variant_measurement_id)
        serializer = TelemetryVariantMeasurementUpdateSerializer(link, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        updated = serializer.save()
        _publish_quality_config([updated])
        return Response(TelemetryVariantMeasurementReadSerializer(updated).data)


class AlgorithmListView(APIView):
    """GET /algorithms
    Algorithms are the postprocessing functions available to blueprint
    steps, with their declared inputs/outputs/params/mode.
    Read-open to any verified owner"""

    permission_classes = [IsVerifiedOwner]

    def get(self, request):
        discover()
        return Response([spec.as_dict() for spec in sorted(REGISTRY.values(), key=lambda s: s.name)])


def _get_blueprint_or_404(blueprint_id) -> PostprocessingBlueprint:
    try:
        return PostprocessingBlueprint.objects.get(pk=blueprint_id)
    except (PostprocessingBlueprint.DoesNotExist, ValueError, TypeError):
        raise NotFound("PostprocessingBlueprint not found")


def _require_blueprint_ownership(blueprint: PostprocessingBlueprint, owner: Owner) -> None:
    if not owner.is_superuser and blueprint.owner_id != owner.id:
        raise PermissionDenied("You do not own this postprocessing blueprint")


class PostprocessingBlueprintListCreateView(APIView):
    permission_classes = [IsVerifiedOwner]

    def get(self, request):
        qs = (
            PostprocessingBlueprint.objects.all()
            if request.user.is_superuser
            else PostprocessingBlueprint.objects.filter(owner=request.user)
        )
        return Response(PostprocessingBlueprintSerializer(qs.prefetch_related("steps"), many=True).data)

    def post(self, request):
        serializer = PostprocessingBlueprintSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        try:
            blueprint = serializer.save(owner=request.user)
        except IntegrityError:
            raise Conflict(f"You already have a blueprint named {request.data.get('name')!r}")
        return Response(PostprocessingBlueprintSerializer(blueprint).data, status=status.HTTP_201_CREATED)


class PostprocessingBlueprintDetailView(APIView):
    permission_classes = [IsVerifiedOwner]

    def get(self, request, blueprint_id):
        blueprint = _get_blueprint_or_404(blueprint_id)
        _require_blueprint_ownership(blueprint, request.user)
        return Response(PostprocessingBlueprintSerializer(blueprint).data)

    def put(self, request, blueprint_id):
        """Passing `steps` replaces the whole ordered list; while omitting it leaves
        the existing steps alone."""
        blueprint = _get_blueprint_or_404(blueprint_id)
        _require_blueprint_ownership(blueprint, request.user)
        serializer = PostprocessingBlueprintSerializer(blueprint, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        try:
            updated = serializer.save()
        except IntegrityError:
            raise Conflict(f"You already have a blueprint named {request.data.get('name')!r}")
        return Response(PostprocessingBlueprintSerializer(updated).data)

    def delete(self, request, blueprint_id):
        """Devices pointing at this blueprint keep working
        FK is SET_NULL, so they just stop being postprocessed."""
        blueprint = _get_blueprint_or_404(blueprint_id)
        _require_blueprint_ownership(blueprint, request.user)
        blueprint.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)
