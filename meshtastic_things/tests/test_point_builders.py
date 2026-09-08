from writer.point_builders import get_points


def _event(payload_kind: str, payload: dict, **overrides) -> dict:
    event = {
        "node_id": 123,
        "packet_id": 1,
        "rx_time": 1700000000,
        "portnum": 0,
        "channel_id": "LongFast",
        "gateway_id": "!aabb",
        "payload_kind": payload_kind,
        "payload": payload,
    }
    event.update(overrides)
    return event


def test_telemetry_produces_one_point_with_expected_fields():
    event = _event(
        "telemetry:device_metrics",
        {"time": 1700000010, "device_metrics": {"battery_level": 87, "voltage": 3.98}},
        portnum=67,
    )

    points = get_points(event)

    assert len(points) == 1
    line = points[0].to_line_protocol()
    assert line.startswith("device_metrics,")
    assert "battery_level=87" in line
    assert "node_id=123" in line
    assert "channel_id=LongFast" in line


def test_mesh_id_is_tagged_when_present():
    """node_id alone is not unique"""
    event = _event(
        "telemetry:device_metrics",
        {"time": 1700000010, "device_metrics": {"battery_level": 87}},
        portnum=67,
        mesh_id="11111111-2222-3333-4444-555555555555",
    )

    points = get_points(event)

    line = points[0].to_line_protocol()
    assert "mesh_id=11111111-2222-3333-4444-555555555555" in line


def test_mesh_id_tag_omitted_when_absent():
    event = _event("telemetry:device_metrics", {"time": 1700000010, "device_metrics": {"battery_level": 87}})

    points = get_points(event)

    assert "mesh_id" not in points[0].to_line_protocol()


def test_position_is_persisted_not_dropped():
    event = _event(
        "position",
        {"latitude_i": 413830000, "longitude_i": 24540000, "altitude": 100},
        portnum=3,
    )

    points = get_points(event)

    assert len(points) == 1
    line = points[0].to_line_protocol()
    assert line.startswith("position,")
    assert "latitude=41.383" in line
    assert "longitude=2.454" in line


def test_nodeinfo_is_persisted_with_name_tags():
    event = _event(
        "nodeinfo",
        {"id": "!abc123", "long_name": "Test Node", "short_name": "TN"},
        portnum=4,
    )

    points = get_points(event)

    assert len(points) == 1
    line = points[0].to_line_protocol()
    assert line.startswith("nodeinfo,")
    assert "short_name=TN" in line


def test_unknown_portnum_falls_back_to_generic_builder():
    event = _event("portnum:99", {"some_metric": 42, "some_text": "ignored"}, portnum=99)

    points = get_points(event)

    assert len(points) == 1
    line = points[0].to_line_protocol()
    assert line.startswith("portnum_99,")
    assert "some_metric=42" in line
    assert "some_text" not in line  # non-numeric fields aren't persisted by the fallback


def test_skip_list_portnums_are_dropped():
    event = _event("portnum:1", {"text": "hello"}, portnum=1)

    assert get_points(event) == []


def _alarm(**overrides):
    """The shape both quality checks produce"""
    alarm = {
        "mesh_id": "11111111-2222-3333-4444-555555555555",
        "device_id": 222,
        "channel": "environment_metrics.temperature",
        "detector": "flat_value",
        "source": "stream",
        "value": 21.5,
        "message": "Value flat at 21.5",
        "triggered_at": "2026-01-01T00:00:00+00:00",
    }
    alarm.update(overrides)
    return alarm


def test_alarm_event_is_persisted():
    points = get_points(_alarm())

    assert len(points) == 1
    line = points[0].to_line_protocol()
    assert line.startswith("alarms,")
    assert "detector=flat_value" in line
    assert "value=21.5" in line


def test_alarm_point_is_tagged_with_the_full_identity():
    # node_id alone isn't globally unique
    line = get_points(_alarm())[0].to_line_protocol()

    assert "mesh_id=11111111-2222-3333-4444-555555555555" in line
    assert "node_id=222" in line
    assert r"channel=environment_metrics.temperature" in line
    assert "source=stream" in line


def test_batch_tier_alarm_is_persisted_too():
    line = get_points(_alarm(detector="completeness", source="batch", value=0.5))[0].to_line_protocol()

    assert "detector=completeness" in line
    assert "source=batch" in line


def test_device_wide_alarm_omits_the_channel_tag():
    # absence checks for devices.
    line = get_points(_alarm(detector="absence", channel=None))[0].to_line_protocol()

    assert "channel=" not in line
    assert "detector=absence" in line


def test_legacy_alarm_shape_still_writes():
    legacy = {
        "node_id": 222,
        "field": "temperature",
        "detector": "stuck_value",
        "triggered_at": "2026-01-01T00:00:00+00:00",
        "value": 21.5,
        "message": "Value stuck at 21.5 for 5 consecutive readings",
    }

    line = get_points(legacy)[0].to_line_protocol()

    assert "node_id=222" in line
    assert r"channel=temperature" in line
    assert "mesh_id" not in line


def test_alarm_without_a_value_still_writes_its_message():
    line = get_points(_alarm(value=None))[0].to_line_protocol()

    assert "value=" not in line
    assert "message=" in line


def test_multiple_telemetry_submetrics_produce_multiple_points():
    event = _event(
        "telemetry:device_metrics",
        {
            "device_metrics": {"battery_level": 50},
            "environment_metrics": {"temperature": 21.5},
        },
        portnum=67,
    )

    points = get_points(event)

    measurements = {p.to_line_protocol().split(",")[0] for p in points}
    assert measurements == {"device_metrics", "environment_metrics"}
