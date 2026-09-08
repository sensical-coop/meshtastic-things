from unittest.mock import MagicMock

from writer.s3_writer import S3Writer, _flatten


def _make_s3_writer():
    writer = object.__new__(S3Writer)
    writer.s3_client = MagicMock()
    writer.s3_bucket = "bucket"
    writer.s3_prefix = "telemetry"
    return writer


def _put_object_keys(writer) -> list[str]:
    return [call.kwargs["Key"] for call in writer.s3_client.put_object.call_args_list]


def test_flatten_includes_mesh_id_when_present():
    event = {"node_id": 123, "mesh_id": "11111111-2222-3333-4444-555555555555", "payload": {}}

    row = _flatten(event)

    assert row["mesh_id"] == "11111111-2222-3333-4444-555555555555"


def test_flatten_mesh_id_is_none_for_pre_migration_events():
    event = {"node_id": 123, "payload": {}}

    row = _flatten(event)

    assert row["mesh_id"] is None


MESH_A = "11111111-2222-3333-4444-555555555555"
MESH_B = "99999999-8888-7777-6666-555544443333"


def test_flush_writes_one_file_per_mesh_id_and_node_id_pair():
    writer = _make_s3_writer()
    records = [
        {"node_id": 1, "mesh_id": MESH_A, "payload": {}},
        {"node_id": 2, "mesh_id": MESH_A, "payload": {}},
        {"node_id": 1, "mesh_id": MESH_B, "payload": {}},  # same node_id, different mesh
    ]

    writer.flush(records)

    keys = _put_object_keys(writer)
    assert len(keys) == 3
    assert any(f"mesh_id={MESH_A}/node_id=1/" in k for k in keys)
    assert any(f"mesh_id={MESH_A}/node_id=2/" in k for k in keys)
    assert any(f"mesh_id={MESH_B}/node_id=1/" in k for k in keys)


def test_flush_groups_same_mesh_and_node_into_one_file():
    writer = _make_s3_writer()
    records = [
        {"node_id": 1, "mesh_id": MESH_A, "payload": {}},
        {"node_id": 1, "mesh_id": MESH_A, "payload": {}},
    ]

    writer.flush(records)

    assert writer.s3_client.put_object.call_count == 1


def test_flush_omits_mesh_id_segment_for_pre_migration_records():
    writer = _make_s3_writer()
    records = [{"node_id": 1, "payload": {}}]

    writer.flush(records)

    key = _put_object_keys(writer)[0]
    assert "mesh_id=" not in key
    assert "node_id=1/" in key
    assert key.startswith("telemetry/node_id=1/dt=")


def test_flush_is_a_noop_for_empty_records():
    writer = _make_s3_writer()

    writer.flush([])

    writer.s3_client.put_object.assert_not_called()
