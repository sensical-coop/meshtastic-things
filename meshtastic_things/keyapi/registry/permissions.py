from rest_framework.permissions import BasePermission


class IsVerifiedOwner(BasePermission):
    """Authenticated AND is_active (email-verified)."""

    def has_permission(self, request, view):
        return bool(request.user and request.user.is_authenticated and request.user.is_active)


class IsSuperuser(BasePermission):

    def has_permission(self, request, view):
        return bool(request.user and request.user.is_authenticated and request.user.is_superuser)
