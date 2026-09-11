import os

from influxdb_client import InfluxDBClient

from common.influx_query import (  # noqa: F401
    ALLOWED_AGG,
    validate_duration as _validate_duration,
    validate_ident as _validate_ident,
    validate_mesh_id as _validate_mesh_id,
    validate_range_bound as _validate_range_bound,
)


class InfluxQueryClient:
    def __init__(self, url: str, token: str, org: str, bucket: str):
        self._client = InfluxDBClient(url=url, token=token, org=org)
        self._query_api = self._client.query_api()
        self.bucket = bucket

    def ping(self) -> bool:
        return self._client.ping()

    def timeseries(
        self,
        mesh_id: str,
        node_id: int,
        measurement: str,
        field: str | None,
        start: str,
        stop: str,
        window: str | None,
        agg: str,
    ) -> list[dict]:
        mesh_id = _validate_mesh_id(mesh_id)
        measurement = _validate_ident(measurement, "measurement")
        if field is not None:
            field = _validate_ident(field, "field")
        start = _validate_range_bound(start, "start")
        stop = _validate_range_bound(stop, "stop")
        if agg not in ALLOWED_AGG:
            raise ValueError(f"Invalid agg: {agg!r} (allowed: {sorted(ALLOWED_AGG)})")

        filters = f'r._measurement == "{measurement}" and r.mesh_id == "{mesh_id}" and r.node_id == "{int(node_id)}"'
        if field:
            filters += f' and r._field == "{field}"'

        # Aggregation is only defined over numbers
        imports, agg_stage, numeric_only = "", "", ""
        if window:
            window = _validate_duration(window, "window")
            imports = 'import "types"\n'
            numeric_only = (
                '|> filter(fn: (r) => types.isType(v: r._value, type: "float") '
                'or types.isType(v: r._value, type: "int"))'
            )
            agg_stage = f'|> aggregateWindow(every: {window}, fn: {agg}, createEmpty: false)'

        flux = f'''{imports}
from(bucket: "{self.bucket}")
  |> range(start: {start}, stop: {stop})
  |> filter(fn: (r) => {filters})
  {numeric_only}
  {agg_stage}
  |> sort(columns: ["_time"])
'''
        return self._records_to_series(self._query_api.query(flux))

    def latest(self, mesh_id: str, node_id: int, measurement: str, lookback: str = "-30d") -> list[dict]:
        mesh_id = _validate_mesh_id(mesh_id)
        measurement = _validate_ident(measurement, "measurement")
        lookback = _validate_range_bound(lookback, "lookback")

        flux = f'''
from(bucket: "{self.bucket}")
  |> range(start: {lookback})
  |> filter(fn: (r) => r._measurement == "{measurement}" and r.mesh_id == "{mesh_id}" and r.node_id == "{int(node_id)}")
  |> last()
'''
        return self._records_to_series(self._query_api.query(flux))

    def latest_all(self, mesh_id: str, node_id: int, lookback: str = "-30d") -> list[dict]:
        """No-?measurement= variant of latest() - combined query across every
        field; grouping by (_measurement, _field) avoids the schema-collision issue."""
        mesh_id = _validate_mesh_id(mesh_id)
        lookback = _validate_range_bound(lookback, "lookback")

        flux = f'''
from(bucket: "{self.bucket}")
  |> range(start: {lookback})
  |> filter(fn: (r) => r.mesh_id == "{mesh_id}" and r.node_id == "{int(node_id)}")
  |> group(columns: ["_measurement", "_field"])
  |> last()
'''
        return self._records_to_readings(self._query_api.query(flux))

    @staticmethod
    def _records_to_series(tables) -> list[dict]:
        series: dict[str, list[dict]] = {}
        for table in tables:
            for record in table.records:
                field = record.get_field()
                series.setdefault(field, []).append(
                    {"time": record.get_time().isoformat(), "value": record.get_value()}
                )
        return [{"field": field, "points": points} for field, points in sorted(series.items())]

    @staticmethod
    def _records_to_readings(tables) -> list[dict]:
        readings = []
        for table in tables:
            for record in table.records:
                readings.append(
                    {
                        "measurement": record.get_measurement(),
                        "field": record.get_field(),
                        "value": record.get_value(),
                        "time": record.get_time().isoformat(),
                    }
                )
        return sorted(readings, key=lambda r: (r["measurement"], r["field"]))


_client: InfluxQueryClient | None = None


def get_client() -> InfluxQueryClient:
    """Lazily-instantiated per-process singleton - each gunicorn worker gets its
    own InfluxDBClient on first use."""
    global _client
    if _client is None:
        _client = InfluxQueryClient(
            url=os.environ["INFLUX__URL"],
            token=os.environ["INFLUX__TOKEN"],
            org=os.environ["INFLUX__ORG"],
            bucket=os.environ["INFLUX__BUCKET"],
        )
    return _client
