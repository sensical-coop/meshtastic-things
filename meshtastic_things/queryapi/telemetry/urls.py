from django.urls import path

from . import views

# queryapi serves values only
urlpatterns = [
    path("health", views.HealthView.as_view()),
    path("devices/<uuid:device_uuid>/timeseries", views.DeviceTimeseriesView.as_view()),
    path("devices/<uuid:device_uuid>/latest", views.DeviceLatestView.as_view()),
    path("meshes/<uuid:mesh_id>/nodes/<int:node_id>/timeseries", views.TimeseriesView.as_view()),
    path("meshes/<uuid:mesh_id>/nodes/<int:node_id>/latest", views.LatestView.as_view()),
]
