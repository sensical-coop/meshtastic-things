"""Production settings - fail at startup on misconfiguration rather than
serving requests with an unsafe one. See keyapi's prod.py for the reasoning."""
import os

from django.core.exceptions import ImproperlyConfigured

from common.env import env_bool, env_int, env_list, env_str

from .base import *  # noqa: F401,F403

# DEBUG in production leaks settings and stack traces.
DEBUG = False

ALLOWED_HOSTS = env_list("QUERYAPI__ALLOWED_HOSTS")
if not ALLOWED_HOSTS:
    raise ImproperlyConfigured(
        "QUERYAPI__ALLOWED_HOSTS must list the hostnames this API is served on "
        "(e.g. api.yourdomain.com)"
    )
if "*" in ALLOWED_HOSTS:
    raise ImproperlyConfigured(
        "QUERYAPI__ALLOWED_HOSTS is '*', which accepts any Host header. Name "
        "the real hostnames"
    )

# Behind Caddy by default; still overridable for an unusual topology.
if env_bool("QUERYAPI__BEHIND_TLS", True):
    SECURE_SSL_REDIRECT = True
    SECURE_HSTS_SECONDS = 31536000
    SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
