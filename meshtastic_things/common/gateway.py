"""Services behind the reverse proxy.

Needed for Swagger and docs
"""

import os

# Must match the Caddyfile's handle_path directives.
MANAGEMENT_PREFIX = "/management"
DATA_PREFIX = "/data"


def public_base_url() -> str:
    return os.environ.get("GATEWAY__PUBLIC_URL", "http://localhost").rstrip("/")


GATEWAY_DESCRIPTION = "(external)"
DIRECT_DESCRIPTION = "(local development)"

def spectacular_servers(prefix: str, direct_url: str) -> list[dict]:
    """The `servers` block for one service's own OpenAPI document.
    """
    return [
        {"url": f"{public_base_url()}{prefix}", "description": GATEWAY_DESCRIPTION},
        {"url": direct_url, "description": DIRECT_DESCRIPTION},
    ]


def ordered_servers(
    prefix: str,
    direct_url: str,
    forwarded_host: str | None = None,
    forwarded_proto: str | None = None,
) -> list[dict]:
    """The same two servers, with the one this request actually arrived on first.
    """
    gateway_origin = (
        f"{forwarded_proto or 'http'}://{forwarded_host}" if forwarded_host else public_base_url()
    )
    gateway = {"url": f"{gateway_origin}{prefix}", "description": GATEWAY_DESCRIPTION}
    direct = {"url": direct_url, "description": DIRECT_DESCRIPTION}
    return [gateway, direct] if forwarded_host else [direct, gateway]
