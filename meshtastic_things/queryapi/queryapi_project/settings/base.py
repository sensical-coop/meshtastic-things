"""Settings shared by every environment.
"""
import os
from pathlib import Path

from django.core.exceptions import ImproperlyConfigured

from common.env import env_bool, env_int, env_list, env_str
from common.gateway import DATA_PREFIX, spectacular_servers

BASE_DIR = Path(__file__).resolve().parent.parent.parent

SECRET_KEY = env_str("QUERYAPI__SECRET_KEY")
if not SECRET_KEY:
    raise ImproperlyConfigured("QUERYAPI__SECRET_KEY must be set - see env.example")
DEBUG = env_bool("QUERYAPI__DEBUG", False)
ALLOWED_HOSTS = env_list("QUERYAPI__ALLOWED_HOSTS")

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "rest_framework",
    "drf_spectacular",
    "telemetry",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.middleware.common.CommonMiddleware",
]

ROOT_URLCONF = "queryapi_project.urls"
WSGI_APPLICATION = "queryapi_project.wsgi.application"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {"context_processors": []},
    },
]

# Never used
DATABASES = {"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}}

CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.redis.RedisCache",
        "LOCATION": env_str("QUERYAPI__REDIS_URL", "redis://redis:6379/1"),
    }
}

TIME_ZONE = "UTC"
USE_TZ = True

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# No trailing slash on any route
APPEND_SLASH = False

REST_FRAMEWORK = {
    "DEFAULT_AUTHENTICATION_CLASSES": ["telemetry.authentication.KeyapiIntrospectionAuthentication"],
    "DEFAULT_PERMISSION_CLASSES": ["rest_framework.permissions.IsAuthenticated"],
    "UNAUTHENTICATED_USER": None,
    "DEFAULT_THROTTLE_CLASSES": ["telemetry.throttling.OwnerRateThrottle"],
    "DEFAULT_THROTTLE_RATES": {"owner": "120/min"},
    "DEFAULT_SCHEMA_CLASS": "drf_spectacular.openapi.AutoSchema",
}

# See the same block in keyapi's settings and common/gateway.py
SPECTACULAR_SETTINGS = {
    "TITLE": "Meshtastic Query API",
    "VERSION": "1.0.0",
    "SERVERS": spectacular_servers(
        DATA_PREFIX,
        env_str("QUERYAPI__DIRECT_URL", f"http://localhost:{env_str('QUERYAPI__PORT', '8091')}"),
    ),
}

# CORS: no origins allowed by default. Set a comma-separated allowlist in
# production if this API is ever called from a browser.
CORS_ALLOWED_ORIGINS = env_list("QUERYAPI__CORS_ORIGINS")
if CORS_ALLOWED_ORIGINS:
    INSTALLED_APPS.insert(0, "corsheaders")
    MIDDLEWARE.insert(0, "corsheaders.middleware.CorsMiddleware")

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {"console": {"class": "logging.StreamHandler"}},
    "root": {"handlers": ["console"], "level": "INFO"},
}
