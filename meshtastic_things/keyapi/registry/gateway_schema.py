"""One OpenAPI document for the whole external surface.

Each service already publishes its own schema, but neither describes what a
consumer actually faces: two base URLs, two Swagger UIs, and paths that are
missing the prefix Caddy strips on the way in. This merges them into a single
valid OpenAPI 3.0 document whose paths are the real external ones
(`/management/devices`, `/data/devices/{uuid}/latest`), so one Swagger UI and
one generated client cover everything.

Built at request time from both live schemas rather than checked in, so it is
generated from the same `urls.py` that serves the traffic and cannot drift.
keyapi's own half is generated in-process; queryapi's is fetched over HTTP -
the same cross-service call convention queryapi already uses to read keyapi's
measurement-type catalog, and cached for the same reason.

Reachable externally at the gateway root (`GET /schema`), which Caddy rewrites
onto this view. It is also reachable as `/management/gateway-schema` by virtue
of living in keyapi's urls.py, which is harmless - the document it returns is
the same either way.
"""
import os

import requests
import structlog
from django.core.cache import cache
from drf_spectacular.generators import SchemaGenerator
from drf_spectacular.utils import extend_schema
from rest_framework import status
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from common.gateway import DATA_PREFIX, MANAGEMENT_PREFIX, public_base_url

log = structlog.get_logger()

CACHE_KEY = "keyapi:gateway-schema:queryapi"
CACHE_TTL_SECONDS = 60


def _fetch_queryapi_schema() -> dict | None:
    """queryapi's own OpenAPI document, cached. None if it can't be reached."""
    cached = cache.get(CACHE_KEY)
    if cached is not None:
        return cached

    url = os.environ.get("KEYAPI__QUERYAPI_URL", "http://query-api:8091")
    timeout = float(os.environ.get("KEYAPI__QUERYAPI_TIMEOUT", "5"))
    try:
        resp = requests.get(f"{url}/schema", headers={"Accept": "application/json"}, timeout=timeout)
        resp.raise_for_status()
        schema = resp.json()
    except (requests.RequestException, ValueError) as e:
        log.warning("gateway_schema_fetch_failed", service="queryapi", error=str(e))
        return None

    cache.set(CACHE_KEY, schema, CACHE_TTL_SECONDS)
    return schema


_HTTP_METHODS = {"get", "put", "post", "delete", "options", "head", "patch", "trace"}


def _namespaced_tags(operations: dict, name: str) -> dict:
    """Prefix every operation's tags with the service name.

    Tags need the same de-collision the paths get: each service tags by its own
    first path segment, so `devices`, `meshes`, `health` and `schema` are
    emitted by both. Swagger UI groups by tag, so without this queryapi's
    operations are absorbed into keyapi's sections and `/data/*` gets no
    section of its own.
    """
    namespaced = {}
    for key, value in operations.items():
        if key in _HTTP_METHODS and isinstance(value, dict) and value.get("tags"):
            value = {**value, "tags": [f"{name}/{tag}" for tag in value["tags"]]}
        namespaced[key] = value
    return namespaced


def _prefixed_paths(schema: dict, prefix: str) -> dict:
    """Re-key a service's paths onto the external ones, namespacing tags too.

    This is the whole reason the merge is safe: the two services both define
    `/health` and `/schema`, and prefixing turns those into `/management/health`
    and `/data/health` - distinct, and each pointing at the service that
    actually answers it.
    """
    name = prefix.strip("/")
    return {
        f"{prefix}{path}": _namespaced_tags(operations, name)
        for path, operations in (schema.get("paths") or {}).items()
    }


def merge_schemas(management: dict, data: dict) -> dict:
    """Combine two service schemas into one gateway document.

    Components are namespaced by service on collision only. Left alone
    otherwise, because a $ref inside an operation points at the original name -
    renaming unconditionally would mean rewriting every reference in both
    documents for a problem neither has today (measured: zero collisions).
    """
    merged = {
        "openapi": management.get("openapi", "3.0.3"),
        "info": {
            "title": "Meshtastic Gateway API",
            "version": management.get("info", {}).get("version", "1.0.0"),
            "description": (
                "Every externally reachable endpoint, across both services.\n\n"
                f"`{MANAGEMENT_PREFIX}/*` is the Key Management API, covering owners, meshes, "
                "devices, the measurement catalog and postprocessing blueprints.\n\n"
                f"`{DATA_PREFIX}/*` is the Query API, covering the readings those devices "
                "have recorded.\n\n"
                "Generated at request time from both services' live schemas."
            ),
        },
        "servers": [{"url": public_base_url(), "description": "Gateway"}],
        "paths": {**_prefixed_paths(management, MANAGEMENT_PREFIX), **_prefixed_paths(data, DATA_PREFIX)},
    }

    components: dict = {}
    for name, schema in ((MANAGEMENT_PREFIX.strip("/"), management), (DATA_PREFIX.strip("/"), data)):
        for section, entries in (schema.get("components") or {}).items():
            target = components.setdefault(section, {})
            for key, value in entries.items():
                if key in target and target[key] != value:
                    log.warning("gateway_schema_component_collision", section=section, name=key)
                    target[f"{name.capitalize()}{key}"] = value
                else:
                    target[key] = value
    if components:
        merged["components"] = components
    return merged


class GatewaySchemaView(APIView):
    """Public, like the per-service `/schema` endpoints it aggregates - an API
    description is not sensitive, and requiring a key would defeat the point of
    publishing one."""

    authentication_classes = []
    permission_classes = [AllowAny]
    throttle_classes = []

    # Kept out of keyapi's own schema, and so out of the merged document it
    # generates: it's a gateway-level endpoint that only happens to be hosted
    # here, and listing it as "/management/gateway-schema" would advertise a
    # path nobody should call.
    @extend_schema(exclude=True)
    def get(self, request):
        data = _fetch_queryapi_schema()
        if data is None:
            # A half document is worse than none: a client generated from it
            # would be silently missing every timeseries method, with nothing
            # to indicate the omission.
            return Response(
                {"detail": "Could not reach the Query API to build the combined schema"},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        management = SchemaGenerator().get_schema(request=None, public=True)
        return Response(merge_schemas(management, data))
