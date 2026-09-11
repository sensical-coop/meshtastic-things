import hashlib
import os

import requests
from django.core.cache import cache
from drf_spectacular.extensions import OpenApiAuthenticationExtension
from rest_framework import authentication, exceptions

# queryapi has no Owner model or Postgres
# TODO Fix
CACHE_TTL_SECONDS = 60

INTROSPECT_RATE_LIMIT = 30
INTROSPECT_RATE_WINDOW_SECONDS = 60

class AuthenticatedOwner:
    is_authenticated = True
    is_anonymous = False

    def __init__(self, id, email, is_superuser, devices):
        self.id = id
        self.pk = id
        self.email = email
        self.is_superuser = is_superuser
        self.devices = frozenset((d["mesh_id"], d["device_id"]) for d in devices)
        self.devices_by_uuid = {
            str(d["id"]): (d["mesh_id"], d["device_id"]) for d in devices if d.get("id")
        }


class KeyapiUnavailable(exceptions.APIException):
    status_code = 503
    default_detail = "Could not reach the Key Management API to verify credentials"
    default_code = "keyapi_unavailable"


class KeyapiIntrospectionAuthentication(authentication.BaseAuthentication):
    """Authenticates the caller by asking keyapi whether their bearer token
    belongs to a real Owner, caching the answer briefly.
    """

    keyword = b"bearer"

    def authenticate(self, request):
        token = self._token_from_header(request) or request.query_params.get("access_token")
        if not token:
            return None

        cache_key = f"queryapi:owner:{hashlib.sha256(token.encode()).hexdigest()}"
        cached = cache.get(cache_key)
        if cached is not None:
            if cached == "invalid":
                raise exceptions.AuthenticationFailed("Invalid API key")
            return (AuthenticatedOwner(**cached), None)

        self._guard_introspection_rate(request)

        keyapi_url = os.environ.get("QUERYAPI__KEYAPI_URL", "http://key-management-api:8090")
        timeout = float(os.environ.get("QUERYAPI__KEYAPI_TIMEOUT", "3"))
        auth_header = {"Authorization": f"Bearer {token}"}
        try:
            resp = requests.get(f"{keyapi_url}/owners/me", headers=auth_header, timeout=timeout)
        except requests.RequestException as e:
            raise KeyapiUnavailable() from e

        if resp.status_code in (401, 403):
            cache.set(cache_key, "invalid", CACHE_TTL_SECONDS)
            raise exceptions.AuthenticationFailed("Invalid API key")
        if resp.status_code != 200:
            raise KeyapiUnavailable()
        body = resp.json()

        owner_data = {
            "id": body["id"],
            "email": body["email"],
            "is_superuser": body["is_superuser"],
            "devices": body["devices"],
        }
        cache.set(cache_key, owner_data, CACHE_TTL_SECONDS)
        return (AuthenticatedOwner(**owner_data), None)

    def _guard_introspection_rate(self, request):
        ip = request.META.get("REMOTE_ADDR", "unknown")
        cache_key = f"queryapi:introspect-attempts:{ip}"
        try:
            count = cache.incr(cache_key)
        except ValueError:
            cache.set(cache_key, 1, INTROSPECT_RATE_WINDOW_SECONDS)
            count = 1
        if count > INTROSPECT_RATE_LIMIT:
            raise exceptions.Throttled(detail="Too many unrecognized credentials, slow down")

    def _token_from_header(self, request) -> str | None:
        parts = authentication.get_authorization_header(request).split()
        if len(parts) == 2 and parts[0].lower() == self.keyword:
            return parts[1].decode()
        return None

    def authenticate_header(self, request):
        return "Bearer"


class KeyapiIntrospectionAuthenticationScheme(OpenApiAuthenticationExtension):
    """Registers KeyapiIntrospectionAuthentication with drf-spectacular so /docs
    shows an Authorize button"""

    target_class = KeyapiIntrospectionAuthentication
    name = "bearerAuth"

    def get_security_definition(self, auto_schema):
        return {"type": "http", "scheme": "bearer", "bearerFormat": "API key"}
