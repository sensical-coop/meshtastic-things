"""InfluxDB read/write for the batch.

`derived` measurement so computed values never mix with raw sensor data
"""
import os
from datetime import datetime, timezone

import structlog
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from common.influx_query import split_channel, validate_mesh_id, validate_range_bound

log = structlog.get_logger()

# Computed series live here, tagged with the blueprint
DERIVED_MEASUREMENT = "derived"

class InfluxIO:
    def __init__(self, url: str, token: str, org: str, bucket: str):
        self._client = InfluxDBClient(url=url, token=token, org=org)
        self._query_api = self._client.query_api()
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)
        self.bucket = bucket

    def ping(self) -> bool:
        return self._client.ping()

    def read_channel(self, mesh_id: str, device_id: int, channel: str, lookback: str) -> list[tuple[datetime, float]]:
        """One channel ("<measurement>.<field>") over a window.
        """
        mesh_id = validate_mesh_id(mesh_id)
        lookback = validate_range_bound(lookback, "lookback")
        measurement, field = split_channel(channel)

        flux = f'''
from(bucket: "{self.bucket}")
  |> range(start: {lookback})
  |> filter(fn: (r) => r._measurement == "{measurement}" and r._field == "{field}"
       and r.mesh_id == "{mesh_id}" and r.node_id == "{int(device_id)}")
  |> sort(columns: ["_time"])
'''
        series = []
        for table in self._query_api.query(flux):
            for record in table.records:
                value = record.get_value()
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    series.append((record.get_time(), float(value)))
        return series

    def write_derived(
        self, mesh_id: str, device_id: int, blueprint_id: str, step_order: int, channel: str, series: list
    ) -> int:
        """Write one derived channel's points. Tagged with blueprint_id and
        step_order."""
        mesh_id = validate_mesh_id(mesh_id)
        points = []
        for timestamp, value in series:
            if value is None:
                continue
            points.append(
                Point(DERIVED_MEASUREMENT)
                .tag("mesh_id", mesh_id)
                .tag("node_id", str(int(device_id)))
                .tag("blueprint_id", str(blueprint_id))
                .tag("step_order", str(step_order))
                .field(channel, float(value))
                .time(timestamp, WritePrecision.NS)
            )
        if points:
            self._write_api.write(bucket=self.bucket, record=points)
        return len(points)

    def write_quality_metric(self, mesh_id: str, device_id: int, channel: str, fields: dict) -> None:
        """Completeness/ratio metrics as their own timeseries."""
        mesh_id = validate_mesh_id(mesh_id)
        point = (
            Point("quality")
            .tag("mesh_id", mesh_id)
            .tag("node_id", str(int(device_id)))
            .tag("channel", channel)
            .time(datetime.now(timezone.utc), WritePrecision.NS)
        )
        for name, value in fields.items():
            if value is not None:
                point = point.field(name, float(value))
        self._write_api.write(bucket=self.bucket, record=point)


_io: InfluxIO | None = None


def get_influx_io() -> InfluxIO:
    global _io
    if _io is None:
        _io = InfluxIO(
            url=os.environ["INFLUX__URL"],
            token=os.environ["INFLUX__TOKEN"],
            org=os.environ["INFLUX__ORG"],
            bucket=os.environ["INFLUX__BUCKET"],
        )
    return _io
