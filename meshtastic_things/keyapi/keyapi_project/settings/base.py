"""Settings shared by every environment.
"""

import os
from pathlib import Path

import dj_database_url
from django.core.exceptions import ImproperlyConfigured

from common.env import env_bool, env_int, env_list, env_str
from common.gateway import MANAGEMENT_PREFIX, spectacular_servers

BASE_DIR = Path(__file__).resolve().parent.parent.parent

SECRET_KEY = env_str("KEYAPI__SECRET_KEY")
if not SECRET_KEY:
    raise ImproperlyConfigured("KEYAPI__SECRET_KEY must be set - see env.example")
DEBUG = env_bool("KEYAPI__DEBUG", False)
ALLOWED_HOSTS = env_list("KEYAPI__ALLOWED_HOSTS")

INSTALLED_APPS = [
    "registry",
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "rest_framework",
    "drf_spectacular",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "keyapi_project.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "keyapi_project.wsgi.application"

# KEYAPI__DB_DSN is postgresql://user:pass@host:port/db
DATABASES = {
    "default": dj_database_url.parse(
        env_str("KEYAPI__DB_DSN", "postgresql://unset:unset@localhost:5432/unset")
    )
}

AUTH_USER_MODEL = "registry.Owner"

CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.redis.RedisCache",
        "LOCATION": env_str("KEYAPI__REDIS_URL", "redis://redis:6379/0"),
    }
}

AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"
STATIC_ROOT = BASE_DIR / "staticfiles"
STATICFILES_STORAGE = "whitenoise.storage.CompressedManifestStaticFilesStorage"

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# No trailing slash on any route
APPEND_SLASH = False

REST_FRAMEWORK = {
    "DEFAULT_AUTHENTICATION_CLASSES": [
        "registry.authentication.OwnerBearerAuthentication",
        "rest_framework.authentication.SessionAuthentication",
    ],
    "DEFAULT_PERMISSION_CLASSES": ["registry.permissions.IsVerifiedOwner"],
    "UNAUTHENTICATED_USER": "django.contrib.auth.models.AnonymousUser",
    "DEFAULT_THROTTLE_CLASSES": [
        "registry.throttling.OwnerRateThrottle",
        "registry.throttling.AnonReadThrottle",
        "registry.throttling.SignupThrottle",
        "registry.throttling.ResendVerificationThrottle",
        "registry.throttling.RequestPasswordResetThrottle",
    ],
    "DEFAULT_THROTTLE_RATES": {
        "owner": "120/min",
        "anon": "60/min",
        "owner-signup": "5/hour",
        "resend-verification": "3/hour",
        "request-password-reset": "5/hour",
    },
    "DEFAULT_SCHEMA_CLASS": "drf_spectacular.openapi.AutoSchema",
}

# SERVERS is important
SPECTACULAR_SETTINGS = {
    "TITLE": "Meshtastic Key Management API",
    "VERSION": "1.0.0",
    "SERVERS": spectacular_servers(
        MANAGEMENT_PREFIX,
        env_str("KEYAPI__DIRECT_URL", f"http://localhost:{env_str('KEYAPI__PORT', '8090')}"),
    ),
}

# CORS: no origins allowed by default.
# Set a comma-separated allowlist in production
# if this API is ever called from a browser - see docs/api-reference.md.
CORS_ALLOWED_ORIGINS = env_list("KEYAPI__CORS_ORIGINS")
if CORS_ALLOWED_ORIGINS:
    INSTALLED_APPS.insert(0, "corsheaders")
    MIDDLEWARE.insert(0, "corsheaders.middleware.CorsMiddleware")
    CORS_ALLOW_CREDENTIALS = True
    CSRF_TRUSTED_ORIGINS = CORS_ALLOWED_ORIGINS

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {"console": {"class": "logging.StreamHandler"}},
    "root": {"handlers": ["console"], "level": "INFO"},
}
