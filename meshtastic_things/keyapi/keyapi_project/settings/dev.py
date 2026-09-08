"""Local development settings.
"""
import os

from common.env import env_bool, env_int, env_list, env_str

from .base import *  # noqa: F401,F403
from .base import INSTALLED_APPS, MIDDLEWARE  # noqa: F401  (re-exported for clarity)

DEBUG = env_bool("KEYAPI__DEBUG", True)
ALLOWED_HOSTS = env_list("KEYAPI__ALLOWED_HOSTS", ["*"])

# Email
# Defaults to MailHog (the `mailhog` service in docker-compose.yml),
EMAIL_BACKEND = env_str("KEYAPI__EMAIL_BACKEND", "django.core.mail.backends.smtp.EmailBackend")
EMAIL_HOST = env_str("KEYAPI__EMAIL_HOST", "mailhog")
EMAIL_PORT = env_int("KEYAPI__EMAIL_PORT", 1025)
EMAIL_HOST_USER = env_str("KEYAPI__EMAIL_HOST_USER", "")
EMAIL_HOST_PASSWORD = env_str("KEYAPI__EMAIL_HOST_PASSWORD", "")
EMAIL_USE_TLS = env_bool("KEYAPI__EMAIL_USE_TLS", False)
EMAIL_USE_SSL = env_bool("KEYAPI__EMAIL_USE_SSL", False)
DEFAULT_FROM_EMAIL = env_str("KEYAPI__DEFAULT_FROM_EMAIL", "noreply@example.com")

EMAIL_VERIFICATION_TTL_HOURS = env_int("KEYAPI__EMAIL_VERIFICATION_TTL_HOURS", 24)
PUBLIC_BASE_URL = env_str("KEYAPI__PUBLIC_BASE_URL", "http://localhost:8090")

if env_bool("KEYAPI__BEHIND_TLS", False):
    SECURE_SSL_REDIRECT = True
    SECURE_HSTS_SECONDS = 31536000
    SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True
