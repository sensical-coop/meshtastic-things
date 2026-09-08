import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from tools.healthcheck import HealthState
from writer.influx_writer import InfluxWriter
from writer.s3_writer import S3Writer


def test_health_state_requires_kafka_and_a_sink():
    state = HealthState()
    assert not state.is_healthy()
    state.kafka_connected = True
    assert not state.is_healthy()


def test_health_state_healthy_with_influx_sink():
    state = HealthState()
    state.kafka_connected = True
    state.influx_connected = True
    assert state.is_healthy()


def test_health_state_healthy_with_s3_sink():
    state = HealthState()
    state.kafka_connected = True
    state.s3_connected = True
    assert state.is_healthy()


def test_health_state_healthy_with_mqtt_sink():
    state = HealthState()
    state.kafka_connected = True
    state.mqtt_connected = True
    assert state.is_healthy()


def _make_influx_writer():
    writer = object.__new__(InfluxWriter)
    writer.health_state = HealthState()
    writer.consumer_health = MagicMock()
    writer.influx_url = "http://influxdb:8086"
    return writer


def test_influx_writer_health_handler_ok_when_ping_succeeds(monkeypatch):
    monkeypatch.setenv("INFLUX__TOKEN", "t")
    monkeypatch.setenv("INFLUX__ORG", "o")
    writer = _make_influx_writer()
    writer.consumer_health.is_healthy.return_value = True

    mock_client = MagicMock()
    mock_client.ping = AsyncMock(return_value=True)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("writer.influx_writer.InfluxDBClientAsync", return_value=mock_client):
        response = asyncio.run(writer.health_handler(None))

    assert response.status == 200


def test_influx_writer_health_handler_unhealthy_when_ping_fails(monkeypatch):
    monkeypatch.setenv("INFLUX__TOKEN", "t")
    monkeypatch.setenv("INFLUX__ORG", "o")
    writer = _make_influx_writer()
    writer.consumer_health.is_healthy.return_value = True

    with patch("writer.influx_writer.InfluxDBClientAsync", side_effect=Exception("boom")):
        response = asyncio.run(writer.health_handler(None))

    assert response.status == 503


def test_influx_writer_health_handler_unhealthy_when_kafka_down(monkeypatch):
    monkeypatch.setenv("INFLUX__TOKEN", "t")
    monkeypatch.setenv("INFLUX__ORG", "o")
    writer = _make_influx_writer()
    writer.consumer_health.is_healthy.return_value = False

    mock_client = MagicMock()
    mock_client.ping = AsyncMock(return_value=True)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("writer.influx_writer.InfluxDBClientAsync", return_value=mock_client):
        response = asyncio.run(writer.health_handler(None))

    assert response.status == 503


def _make_s3_writer():
    writer = object.__new__(S3Writer)
    writer.health_state = HealthState()
    writer.consumer_health = MagicMock()
    writer.s3_client = MagicMock()
    writer.s3_bucket = "bucket"
    return writer


def test_s3_writer_health_handler_ok_when_bucket_reachable():
    writer = _make_s3_writer()
    writer.consumer_health.is_healthy.return_value = True

    response = asyncio.run(writer.health_handler(None))

    assert response.status == 200
    writer.s3_client.head_bucket.assert_called_once_with(Bucket="bucket")


def test_s3_writer_health_handler_unhealthy_when_bucket_unreachable():
    writer = _make_s3_writer()
    writer.consumer_health.is_healthy.return_value = True
    writer.s3_client.head_bucket.side_effect = Exception("no bucket")

    response = asyncio.run(writer.health_handler(None))

    assert response.status == 503
