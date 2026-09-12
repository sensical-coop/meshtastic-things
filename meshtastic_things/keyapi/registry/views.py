from datetime import timedelta

import structlog
from django.conf import settings
from django.contrib.auth import login as auth_login, logout as auth_logout
from django.contrib.auth.password_validation import validate_password
from django.core.cache import cache
from django.core.exceptions import ValidationError as DjangoValidationError
from django.db import IntegrityError
from django.db.models import Q
from django.middleware.csrf import get_token as get_csrf_token
from django.utils import timezone
from rest_framework import status
from rest_framework.exceptions import AuthenticationFailed, NotFound, ParseError, PermissionDenied, Throttled, ValidationError
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from .auth import generate_api_key, hash_api_key
from .emailing import send_password_reset_email, send_verification_email
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
    MetricsResponseSerializer,
    OwnerChangePasswordSerializer,
    OwnerCreateSerializer,
    OwnerLoginSerializer,
    OwnerPublicSerializer,
    OwnerReadSerializer,
    OwnerRequestPasswordResetSerializer,
    OwnerResetPasswordSerializer,
    OwnerUpdateSerializer,
    OwnerWithApiKeySerializer,
    PostprocessingBlueprintSerializer,
    DeviceMeasurementSerializer,
    DeviceTelemetryVariantSerializer,
    TelemetryVariantMeasurementReadSerializer,
    TelemetryVariantMeasurementUpdateSerializer,
    TelemetryVariantReadSerializer,
    TelemetryVariantUpdateSerializer,
)
from .throttling import ResendVerificationThrottle

log = structlog.get_logger()


class HealthView(APIView):
    """Report whether the service and its database are reachable."""

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


class MetricsView(APIView):
    """Return totals for registered owners, meshes, devices and active gateways."""

    permission_classes = [AllowAny]

    def get(self, request):
        body = {
            "devices": Device.objects.count(),
            "owners": Owner.objects.count(),
            "meshes": Mesh.objects.count(),
            "active_gateways": Device.objects.filter(is_gateway=True, is_allowed=True).count(),
        }
        return Response(MetricsResponseSerializer(body).data)


class OwnerListCreateView(APIView):
    permission_classes = [AllowAny]

    def get(self, request):
        """List registered owners. Email addresses are not included."""
        owners = Owner.objects.all()
        return Response(OwnerPublicSerializer(owners, many=True).data)

    def post(self, request):
        """Register an owner and return its API key.

        The only write on this API that needs no authentication. The key is
        shown in this response and cannot be retrieved again afterwards. A new
        owner stays inactive until its email address is verified.
        """
        serializer = OwnerCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        api_key = generate_api_key()
        verification_token = generate_api_key()
        try:
            owner = Owner.objects.create_user(
                email=serializer.validated_data["email"],
                name=serializer.validated_data["name"],
                password=serializer.validated_data["password"],
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
    """Confirm an owner's email address using the token from the verification link."""

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
    """Send a fresh verification email to the authenticated owner."""

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


class CsrfCookieView(APIView):
    """Set the CSRF cookie a browser client needs before logging in."""

    authentication_classes = []
    permission_classes = [AllowAny]
    throttle_classes = []

    def get(self, request):
        get_csrf_token(request)
        return Response({"detail": "CSRF cookie set"})


LOGIN_FAILED_RATE_LIMIT = 10
LOGIN_FAILED_RATE_WINDOW_SECONDS = 300


class OwnerLoginView(APIView):
    """Sign in with email and password, returning the owner profile and a session cookie.

    Repeated failures from the same address are rate limited.
    """

    permission_classes = [AllowAny]

    def post(self, request):
        serializer = OwnerLoginSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        cache_key = f"keyapi:failed-login:{request.META.get('REMOTE_ADDR', 'unknown')}"
        if (cache.get(cache_key) or 0) >= LOGIN_FAILED_RATE_LIMIT:
            raise Throttled(detail="Too many failed login attempts, slow down")

        owner = Owner.objects.filter(email=serializer.validated_data["email"]).first()
        if owner is None or not owner.check_password(serializer.validated_data["password"]):
            try:
                cache.incr(cache_key)
            except ValueError:
                cache.set(cache_key, 1, LOGIN_FAILED_RATE_WINDOW_SECONDS)
            raise AuthenticationFailed("Invalid email or password")
        if not owner.is_active:
            raise PermissionDenied("Email not verified - see POST /owners/me/resend-verification")

        auth_login(request, owner, backend="django.contrib.auth.backends.ModelBackend")
        data = OwnerReadSerializer(owner).data
        data["devices"] = _devices_for_owner(owner)
        return Response(data)


class OwnerLogoutView(APIView):
    """End the current session."""

    authentication_classes = []
    permission_classes = [AllowAny]

    def post(self, request):
        auth_logout(request)
        return Response({"detail": "Logged out"})


class OwnerRequestPasswordResetView(APIView):
    """Email a password reset link.

    The response is the same whether or not the address is registered, so it
    cannot be used to discover accounts.
    """

    authentication_classes = []
    permission_classes = [AllowAny]

    def post(self, request):
        serializer = OwnerRequestPasswordResetSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        owner = Owner.objects.filter(email=serializer.validated_data["email"]).first()
        if owner is not None:
            reset_token = generate_api_key()
            owner.password_reset_token_hash = hash_api_key(reset_token)
            owner.password_reset_sent_at = timezone.now()
            owner.save(update_fields=["password_reset_token_hash", "password_reset_sent_at"])
            try:
                send_password_reset_email(owner, reset_token)
            except Exception as e:
                log.warning("password_reset_email_send_failed", owner_id=str(owner.id), error=str(e))
        return Response({"detail": "If that email is registered, a reset link has been sent"})


class OwnerResetPasswordView(APIView):
    """Set a new password using the token from a reset link."""

    authentication_classes = []
    permission_classes = [AllowAny]

    def post(self, request):
        serializer = OwnerResetPasswordSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        owner = Owner.objects.filter(
            password_reset_token_hash=hash_api_key(serializer.validated_data["token"])
        ).first()
        ttl = timedelta(hours=settings.PASSWORD_RESET_TTL_HOURS)
        expired = owner is None or owner.password_reset_sent_at is None or (
            timezone.now() > owner.password_reset_sent_at + ttl
        )
        if expired:
            raise ParseError(
                "Invalid or expired reset link - request a new one via "
                "POST /owners/request-password-reset"
            )
        try:
            validate_password(serializer.validated_data["new_password"], user=owner)
        except DjangoValidationError as e:
            raise ValidationError({"new_password": list(e.messages)})
        owner.set_password(serializer.validated_data["new_password"])
        owner.password_reset_token_hash = None
        owner.password_reset_sent_at = None
        owner.save(update_fields=["password", "password_reset_token_hash", "password_reset_sent_at"])
        return Response({"detail": "Password reset"})


class OwnerDetailView(APIView):
    """Return one owner's public details. Email addresses are not included."""

    permission_classes = [AllowAny]

    def get(self, request, owner_id):
        try:
            owner = Owner.objects.get(pk=owner_id)
        except (Owner.DoesNotExist, ValueError, TypeError):
            raise NotFound("Owner not found")
        return Response(OwnerPublicSerializer(owner).data)


class OwnerMeView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        """Return the authenticated owner's own record, including their devices."""
        data = OwnerReadSerializer(request.user).data
        data["devices"] = _devices_for_owner(request.user)
        return Response(data)

    def put(self, request):
        """Update the authenticated owner's name or email address.

        Changing the address deactivates the account until the new one is
        verified, and sends a fresh verification email.
        """
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
                # TODO - Evaluate if there is a place where failures can be visibly to user (for instance, a task manager UI)
                log.warning("verification_email_send_failed", owner_id=str(owner.id), error=str(e))
        return Response(OwnerReadSerializer(owner).data)

    def delete(self, request):
        """Delete the authenticated owner, along with their meshes and devices.

        Their gateways are deregistered from the decoder first, and nothing is
        removed if that fails.
        """
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
    """Issue a new API key and revoke the previous one immediately.

    The new key is shown in this response and cannot be retrieved again.
    """

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


class OwnerChangePasswordView(APIView):
    """Change the authenticated owner's password, confirming the current one first."""

    permission_classes = [IsVerifiedOwner]

    def post(self, request):
        serializer = OwnerChangePasswordSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        owner = request.user
        if not owner.check_password(serializer.validated_data["current_password"]):
            raise ValidationError({"current_password": ["Incorrect password"]})
        try:
            validate_password(serializer.validated_data["new_password"], user=owner)
        except DjangoValidationError as e:
            raise ValidationError({"new_password": list(e.messages)})
        owner.set_password(serializer.validated_data["new_password"])
        owner.save(update_fields=["password"])
        return Response({"detail": "Password changed"})


def _devices_for_owner(owner: Owner) -> list[dict]:
    """Return the devices the given owner may access, or all of them for a superuser."""
    qs = Device.objects.select_related("mesh")
    qs = qs.all() if owner.is_superuser else qs.filter(mesh__owner_id=owner.id)
    return DeviceReadSerializer(
        qs.order_by("mesh_id", "device_id"), many=True, context={"viewer_id": owner.id}
    ).data


class OwnerDeviceIdsView(APIView):
    """List the devices the authenticated owner may access."""

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
        return Device.objects.select_related("mesh").get(pk=id)
    except (Device.DoesNotExist, ValueError, TypeError):
        raise NotFound("Device not found")


class MeshListCreateView(APIView):
    permission_classes = [IsVerifiedOwner]

    def get(self, request):
        """List the caller's meshes, or every mesh for a superuser.

        The pre-shared key is included only for meshes the caller owns.
        """
        qs = Mesh.objects.all() if request.user.is_superuser else Mesh.objects.filter(owner=request.user)
        return Response(MeshSerializer(qs, many=True, context={"viewer_id": request.user.id}).data)

    def post(self, request):
        """Create a mesh owned by the caller."""
        serializer = MeshSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        mesh = serializer.save(owner=request.user)
        return Response(
            MeshSerializer(mesh, context={"viewer_id": request.user.id}).data, status=status.HTTP_201_CREATED
        )


class MeshDetailView(APIView):
    permission_classes = [IsVerifiedOwner]

    def get(self, request, mesh_id):
        """Return one mesh."""
        mesh = _get_mesh_or_404(mesh_id)
        _require_mesh_ownership(mesh, request.user)
        return Response(MeshSerializer(mesh, context={"viewer_id": request.user.id}).data)

    def put(self, request, mesh_id):
        """Update a mesh.

        Changing the pre-shared key redistributes it to every allowed gateway
        on the mesh, and the whole update is rolled back if that fails.
        """
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
        return Response(MeshSerializer(updated, context={"viewer_id": request.user.id}).data)

    def delete(self, request, mesh_id):
        """Delete a mesh and every device registered on it.

        Its gateways are deregistered from the decoder first, and nothing is
        removed if that fails.
        """
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
    """List every device registered on this mesh, gateways and nodes alike."""

    permission_classes = [IsVerifiedOwner]

    def get(self, request, mesh_id):
        mesh = _get_mesh_or_404(mesh_id)
        _require_mesh_ownership(mesh, request.user)
        return Response(
            DeviceReadSerializer(
                mesh.devices.select_related("mesh").all(), many=True, context={"viewer_id": request.user.id}
            ).data
        )


class DeviceListCreateView(APIView):
    permission_classes = [IsVerifiedOwner]

    def get(self, request):
        """List the caller's registered devices, or every device for a superuser.

        Args:
            mesh_id: Restrict to devices on one mesh.
            is_gateway: Restrict to gateways, or to plain nodes when false.
            is_allowed: Restrict to allowed registrations, or to rejected ones
                when false.
            has_postprocessing: Restrict to devices with a blueprint attached,
                or to those without one when false.
        """
        qs = Device.objects.select_related("mesh")
        qs = qs.all() if request.user.is_superuser else qs.filter(mesh__owner=request.user)
        mesh_id = request.query_params.get("mesh_id")
        if mesh_id:
            qs = qs.filter(mesh_id=mesh_id)
        for flag in ("is_gateway", "is_allowed"):
            value = request.query_params.get(flag)
            if value is not None:
                qs = qs.filter(**{flag: value.lower() in ("true", "1")})
        has_postprocessing = request.query_params.get("has_postprocessing")
        if has_postprocessing is not None:
            qs = qs.filter(postprocessing_blueprint__isnull=has_postprocessing.lower() not in ("true", "1"))
        return Response(DeviceReadSerializer(qs, many=True, context={"viewer_id": request.user.id}).data)

    def post(self, request):
        """Register a device on a mesh the caller owns.

        Registering a gateway authorizes its traffic to be decoded. A
        device can only move to another mesh once its current registration has
        been disallowed.
        """
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
        return Response(
            DeviceReadSerializer(device, context={"viewer_id": request.user.id}).data,
            status=status.HTTP_201_CREATED,
        )


class DeviceDetailView(APIView):
    permission_classes = [IsVerifiedOwner]

    def get(self, request, id):
        """Return one registered device."""
        device = _get_device_or_404(id)
        _require_mesh_ownership(device.mesh, request.user)
        return Response(DeviceReadSerializer(device, context={"viewer_id": request.user.id}).data)

    def put(self, request, id):
        """Update a registered device.

        Position and identity fields are filled in from what the device reports
        until they are set here, after which the values given take precedence.
        Disallowing a gateway stops its traffic being decoded.
        """
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
        return Response(DeviceReadSerializer(updated, context={"viewer_id": request.user.id}).data)

    def delete(self, request, id):
        """Delete a device registration.

        The device is deregistered from the decoder first, and the record is
        kept if that fails.
        """
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
    """List the telemetry types this device has been seen reporting.

    Entries appear on their own as telemetry arrives and cannot be edited here.
    """

    permission_classes = [IsVerifiedOwner]

    def get(self, request, id):
        device = _get_device_or_404(id)
        _require_mesh_ownership(device.mesh, request.user)
        variants = device.telemetry_variants.select_related("telemetry_variant")
        return Response(DeviceTelemetryVariantSerializer(variants, many=True).data)


class DeviceMeasurementsView(APIView):
    """List the measurement channels this device produces, measured and computed alike.

    Entries appear on their own as telemetry arrives and cannot be edited here.
    """

    permission_classes = [IsVerifiedOwner]

    def get(self, request, id):
        device = _get_device_or_404(id)
        _require_mesh_ownership(device.mesh, request.user)
        measurements = (
            Measurement.objects.filter(device_telemetry_variant__device=device)
            .select_related("measurement_type", "device_telemetry_variant")
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
    permission_classes = [IsVerifiedOwner]

    def get(self, request):
        """List the shared catalog of measurement types and their units.

        Sensor fields are named `telemetry:<variant>` and blueprint outputs
        `derived:<blueprint>`.

        Args:
            kind: Restrict to one namespace, such as `telemetry`, `derived` or
                `quality`.
            payload_kind: Restrict to one exact payload kind, such as
                `telemetry:environment_metrics`.
            field_name: Restrict to one field name, such as `temperature`.
        """
        return Response(
            MeasurementTypeReadSerializer(filter_measurement_types(MeasurementType.objects.all(), request), many=True).data
        )

    def post(self, request):
        """Add a measurement type to the shared catalog. Superusers only."""
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
        """Return one measurement type."""
        return Response(MeasurementTypeReadSerializer(_get_measurement_type_or_404(measurement_type_id)).data)

    def put(self, request, measurement_type_id):
        """Update a measurement type. Its payload kind and field name are fixed. Superusers only."""
        _require_superuser(request)
        measurement_type = _get_measurement_type_or_404(measurement_type_id)
        serializer = MeasurementTypeUpdateSerializer(measurement_type, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        updated = serializer.save()
        return Response(MeasurementTypeReadSerializer(updated).data)

    def delete(self, request, measurement_type_id):
        """Remove a measurement type from the catalog. Superusers only."""
        _require_superuser(request)
        _get_measurement_type_or_404(measurement_type_id).delete()
        return Response(status=status.HTTP_204_NO_CONTENT)


class TelemetryVariantListCreateView(APIView):
    """List the known telemetry variants, such as device or environment metrics.

    Variants are registered automatically as devices report them, so there is
    no way to create one here.
    """

    permission_classes = [IsVerifiedOwner]

    def get(self, request):
        return Response(TelemetryVariantReadSerializer(TelemetryVariant.objects.all(), many=True).data)


class TelemetryVariantDetailView(APIView):
    permission_classes = [IsVerifiedOwner]

    def get(self, request, telemetry_variant_id):
        """Return one telemetry variant."""
        return Response(TelemetryVariantReadSerializer(_get_telemetry_variant_or_404(telemetry_variant_id)).data)

    def put(self, request, telemetry_variant_id):
        """Update a telemetry variant. Its payload kind is fixed. Superusers only.

        The expected reading interval set here is used to judge whether devices
        reporting this variant are publishing as often as they should.
        """
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
        """Delete a telemetry variant. Superusers only."""
        _require_superuser(request)
        _get_telemetry_variant_or_404(telemetry_variant_id).delete()
        return Response(status=status.HTTP_204_NO_CONTENT)


class TelemetryVariantMeasurementsView(APIView):
    """List the measurement types reported under this telemetry variant."""

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
    permission_classes = [IsVerifiedOwner]

    def get(self, request, telemetry_variant_measurement_id):
        """Return one measurement channel, including its plausible value range."""
        link = _get_telemetry_variant_measurement_or_404(telemetry_variant_measurement_id)
        return Response(TelemetryVariantMeasurementReadSerializer(link).data)

    def put(self, request, telemetry_variant_measurement_id):
        """Set the plausible value range for a channel. Superusers only.

        Readings outside the range are flagged by the quality checks.
        """
        _require_superuser(request)
        link = _get_telemetry_variant_measurement_or_404(telemetry_variant_measurement_id)
        serializer = TelemetryVariantMeasurementUpdateSerializer(link, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        updated = serializer.save()
        _publish_quality_config([updated])
        return Response(TelemetryVariantMeasurementReadSerializer(updated).data)


class AlgorithmListView(APIView):
    """List the functions available to postprocessing steps.

    Each entry declares the inputs it needs, the outputs it produces and the
    parameters it accepts.
    """

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
        """List the caller's postprocessing blueprints, or every one for a superuser."""
        qs = (
            PostprocessingBlueprint.objects.all()
            if request.user.is_superuser
            else PostprocessingBlueprint.objects.filter(owner=request.user)
        )
        return Response(PostprocessingBlueprintSerializer(qs.prefetch_related("steps"), many=True).data)

    def post(self, request):
        """Create a postprocessing blueprint, an ordered set of steps that compute
        new channels from a device's readings."""
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
        """Return one postprocessing blueprint and its steps."""
        blueprint = _get_blueprint_or_404(blueprint_id)
        _require_blueprint_ownership(blueprint, request.user)
        return Response(PostprocessingBlueprintSerializer(blueprint).data)

    def put(self, request, blueprint_id):
        """Update a postprocessing blueprint.

        Supplying `steps` replaces the whole ordered list; omitting it leaves
        the existing steps untouched.
        """
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
        """Delete a postprocessing blueprint.

        Devices using it keep recording telemetry and simply stop producing
        computed channels.
        """
        blueprint = _get_blueprint_or_404(blueprint_id)
        _require_blueprint_ownership(blueprint, request.user)
        blueprint.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)
