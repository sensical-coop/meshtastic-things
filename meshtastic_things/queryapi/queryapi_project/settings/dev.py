"""Local development settings"""
import os

from common.env import env_bool, env_int, env_list, env_str

from .base import *  # noqa: F401,F403

DEBUG = env_bool("QUERYAPI__DEBUG", True)
ALLOWED_HOSTS = env_list("QUERYAPI__ALLOWED_HOSTS", ["*"])

# Off by default
if env_bool("QUERYAPI__BEHIND_TLS", False):
    SECURE_SSL_REDIRECT = True
    SECURE_HSTS_SECONDS = 31536000
    SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
