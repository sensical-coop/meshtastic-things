from django.core.cache import cache
from drf_spectacular.extensions import OpenApiAuthenticationExtension
from rest_framework import authentication, exceptions

from .auth import hash_api_key
from .models import Owner

# DRF's dispatch runs authentication before check_throttles, so a request with an
# invalid bearer token never reaches registry.throttling.OwnerRateThrottle - that
# throttle only ever sees requests that already succeeded. This guard closes that
# gap directly: a per-source-IP cap on *failed* attempts specifically (not valid
# ones - a legitimate heavy user's normal traffic is already governed separately by
# OwnerRateThrottle's per-owner budget, not per-IP). Same pattern as queryapi's
# KeyapiIntrospectionAuthentication._guard_introspection_rate.
FAILED_AUTH_RATE_LIMIT = 30
FAILED_AUTH_RATE_WINDOW_SECONDS = 60


class OwnerBearerAuthentication(authentication.BaseAuthentication):
    """
    Authenticates the caller as the Owner whose API key was presented, via
    `Authorization: Bearer <api_key>`. Falls back to an `access_token` query
    parameter and only checked when no Authorization header is
    present.
    """

    keyword = b"bearer"

    def authenticate(self, request):
        token = self._token_from_header(request) or request.query_params.get("access_token")
        if not token:
            return None

        cache_key = f"keyapi:failed-auth:{request.META.get('REMOTE_ADDR', 'unknown')}"
        if (cache.get(cache_key) or 0) >= FAILED_AUTH_RATE_LIMIT:
            raise exceptions.Throttled(detail="Too many invalid credentials, slow down")

        owner = Owner.objects.filter(api_key_hash=hash_api_key(token)).first()
        if owner is None:
            self._record_failed_attempt(cache_key)
            raise exceptions.AuthenticationFailed("Invalid API key")
        return (owner, None)

    def _record_failed_attempt(self, cache_key: str) -> None:
        try:
            cache.incr(cache_key)
        except ValueError:
            cache.set(cache_key, 1, FAILED_AUTH_RATE_WINDOW_SECONDS)

    def _token_from_header(self, request) -> str | None:
        parts = authentication.get_authorization_header(request).split()
        if len(parts) == 2 and parts[0].lower() == self.keyword:
            return parts[1].decode()
        return None

    def authenticate_header(self, request):
        return "Bearer"


class OwnerBearerAuthenticationScheme(OpenApiAuthenticationExtension):
    """Registers OwnerBearerAuthentication with drf-spectacular so /docs shows an
    Authorize button"""

    target_class = OwnerBearerAuthentication
    name = "bearerAuth"

    def get_security_definition(self, auto_schema):
        return {"type": "http", "scheme": "bearer", "bearerFormat": "API key"}
