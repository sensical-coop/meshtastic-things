"""A schema view that knows which origin it was reached on.
"""
from drf_spectacular.views import SpectacularAPIView, SpectacularSwaggerView
from common.gateway import ordered_servers

class GatewayAwareSchemaView(SpectacularAPIView):
    """SpectacularAPIView with the `servers` block ordered per request.
    """

    gateway_prefix: str = ""
    direct_url: str = ""

    def _get_schema_response(self, request):
        response = super()._get_schema_response(request)
        response.data["servers"] = ordered_servers(
            self.gateway_prefix,
            self.direct_url,
            # Set by Caddy's reverse_proxy;
            forwarded_host=request.META.get("HTTP_X_FORWARDED_HOST"),
            forwarded_proto=request.META.get("HTTP_X_FORWARDED_PROTO"),
        )
        return response


class GatewayAwareDocsView(SpectacularSwaggerView):
    """Swagger UI for one service, pointed at that service's schema.
    """

    gateway_prefix: str = ""

    def _get_schema_url(self, request):
        if request.META.get("HTTP_X_FORWARDED_HOST"):
            self.url = f"{self.gateway_prefix}/schema"
        return super()._get_schema_url(request)


class GatewayDocsView(SpectacularSwaggerView):
    """Swagger UI for the merged gateway document, pointed at whichever URL is
    same-origin for the reader.
    """

    gateway_url = "/schema"
    direct_url = "/gateway-schema"

    def _get_schema_url(self, request):
        self.url = self.gateway_url if request.META.get("HTTP_X_FORWARDED_HOST") else self.direct_url
        return super()._get_schema_url(request)
