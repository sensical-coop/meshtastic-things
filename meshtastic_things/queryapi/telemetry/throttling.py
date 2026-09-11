from rest_framework.throttling import SimpleRateThrottle


class OwnerRateThrottle(SimpleRateThrottle):
    """Throttles authenticated traffic per Owner"""

    scope = "owner"

    def get_cache_key(self, request, view):
        if not (request.user and getattr(request.user, "is_authenticated", False)):
            return None
        return self.cache_format % {"scope": self.scope, "ident": request.user.id}
