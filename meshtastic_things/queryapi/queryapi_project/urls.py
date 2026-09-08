from django.conf import settings
from django.urls import include, path
from common.gateway import DATA_PREFIX
from common.schema import GatewayAwareDocsView, GatewayAwareSchemaView


class QueryapiSchemaView(GatewayAwareSchemaView):
    """`servers` ordered by the origin of the request arrived"""

    gateway_prefix = DATA_PREFIX
    direct_url = settings.SPECTACULAR_SETTINGS["SERVERS"][-1]["url"]


urlpatterns = [
    path("schema", QueryapiSchemaView.as_view(), name="schema"),
    path("docs", GatewayAwareDocsView.as_view(url_name="schema", gateway_prefix=DATA_PREFIX)),
    path("", include("telemetry.urls")),
]
