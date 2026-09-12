from rest_framework import serializers


class PointSerializer(serializers.Serializer):
    time = serializers.CharField()
    value = serializers.JSONField(allow_null=True)  # float | str | bool | None


class SeriesSerializer(serializers.Serializer):
    field = serializers.CharField()
    points = PointSerializer(many=True)


class TimeseriesResponseSerializer(serializers.Serializer):
    mesh_id = serializers.CharField()
    node_id = serializers.IntegerField()
    measurement = serializers.CharField()
    series = SeriesSerializer(many=True)


class LatestReadingSerializer(serializers.Serializer):
    measurement = serializers.CharField()
    field = serializers.CharField()
    value = serializers.JSONField(allow_null=True)
    time = serializers.CharField()
    unit = serializers.CharField(allow_null=True)
    quantity_kind = serializers.CharField(allow_null=True)
    vocabulary_uri = serializers.CharField(allow_null=True)
    # "measured", "derived", "quality"
    origin = serializers.CharField()


class LatestReadingsResponseSerializer(serializers.Serializer):
    mesh_id = serializers.CharField()
    node_id = serializers.IntegerField()
    readings = LatestReadingSerializer(many=True)


class ChannelQualitySerializer(serializers.Serializer):
    channel = serializers.CharField()
    completeness_ratio = serializers.FloatField(allow_null=True)
    observed = serializers.FloatField(allow_null=True)
    expected = serializers.FloatField(allow_null=True)
    largest_gap_seconds = serializers.FloatField(allow_null=True)


class DeviceMetricsResponseSerializer(serializers.Serializer):
    mesh_id = serializers.CharField()
    node_id = serializers.IntegerField()
    start = serializers.CharField()
    stop = serializers.CharField()
    points_ingested = serializers.FloatField()
    plausibility_ratio = serializers.FloatField(allow_null=True)
    alarms_by_detector = serializers.DictField(child=serializers.IntegerField())
    channels = ChannelQualitySerializer(many=True)


