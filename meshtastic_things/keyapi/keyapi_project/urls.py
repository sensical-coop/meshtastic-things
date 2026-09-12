from django.conf import settings
from django.contrib import admin
from django.urls import include, path

from common.gateway import MANAGEMENT_PREFIX
from common.schema import GatewayAwareDocsView, GatewayAwareSchemaView, GatewayDocsView

from registry.gateway_schema import GatewaySchemaView


class KeyapiSchemaView(GatewayAwareSchemaView):
    """Return this service's OpenAPI document."""

    gateway_prefix = MANAGEMENT_PREFIX
    direct_url = settings.SPECTACULAR_SETTINGS["SERVERS"][-1]["url"]


urlpatterns = [
    path("admin", admin.site.urls),
    path("schema", KeyapiSchemaView.as_view(), name="schema"),
    path("docs", GatewayAwareDocsView.as_view(url_name="schema", gateway_prefix=MANAGEMENT_PREFIX)),
    path("gateway-schema", GatewaySchemaView.as_view(), name="gateway-schema"),
    path("gateway-docs", GatewayDocsView.as_view()),
    path("", include("registry.urls")),
]
