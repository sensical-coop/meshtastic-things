from rest_framework.throttling import AnonRateThrottle, SimpleRateThrottle

# TODO Review


class OwnerRateThrottle(SimpleRateThrottle):
    """Throttles authenticated traffic per Owner"""

    scope = "owner"

    def get_cache_key(self, request, view):
        if not (request.user and getattr(request.user, "is_authenticated", False)):
            return None
        return self.cache_format % {"scope": self.scope, "ident": request.user.id}


class AnonReadThrottle(AnonRateThrottle):
    """Any anonymous GET request"""

    scope = "anon"

    def get_cache_key(self, request, view):
        if request.method != "GET":
            return None
        return super().get_cache_key(request, view)


class SignupThrottle(SimpleRateThrottle):
    """POST /owners"""

    scope = "owner-signup"

    def get_cache_key(self, request, view):
        if request.method != "POST" or request.path != "/owners":
            return None
        return self.cache_format % {"scope": self.scope, "ident": self.get_ident(request)}


class ResendVerificationThrottle(SimpleRateThrottle):
    """POST /owners/me/resend-verification"""

    scope = "resend-verification"

    def get_cache_key(self, request, view):
        if request.method != "POST" or request.path != "/owners/me/resend-verification":
            return None
        if not (request.user and getattr(request.user, "is_authenticated", False)):
            return None
        return self.cache_format % {"scope": self.scope, "ident": request.user.id}
