"""Settings package
"""
import os

from django.core.exceptions import ImproperlyConfigured

if os.environ.get("DJANGO_SETTINGS_MODULE", "").strip() == __name__:
    raise ImproperlyConfigured(
        f"DJANGO_SETTINGS_MODULE is '{__name__}', which is a package, not a "
        f"settings module. Use '{__name__}.dev' or '{__name__}.prod' - "
        "see docs/deploy.md#django-settings."
    )
