"""Fetches keyapi's GET /measurement-types"""
import os

import requests
import structlog
from django.core.cache import cache

from common.channels import channel_name

log = structlog.get_logger()

CACHE_KEY = "queryapi:measurement-types"
CACHE_TTL_SECONDS = 300


def list_measurement_types(auth_header: dict) -> list[dict]:
    """Raw catalog rows from keyapi's GET /measurement-types"""
    cached = cache.get(CACHE_KEY)
    if cached is not None:
        return cached

    keyapi_url = os.environ.get("QUERYAPI__KEYAPI_URL", "http://key-management-api:8090")
    timeout = float(os.environ.get("QUERYAPI__KEYAPI_TIMEOUT", "3"))
    try:
        resp = requests.get(f"{keyapi_url}/measurement-types", headers=auth_header, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.warning("measurement_type_catalog_fetch_failed", error=str(e))
        return []

    rows = resp.json()
    cache.set(CACHE_KEY, rows, CACHE_TTL_SECONDS)
    return rows


def get_measurement_types(auth_header: dict) -> dict[str, dict]:
    """Returns {channel_name: {"unit", "quantity_kind", "vocabulary_uri"}},
    derived from list_measurement_types()."""
    return {
        channel_name(row["payload_kind"], row["field_name"]): {
            "unit": row["native_unit"],
            "quantity_kind": row["quantity_kind"],
            "vocabulary_uri": row["vocabulary_uri"],
        }
        for row in list_measurement_types(auth_header)
    }
