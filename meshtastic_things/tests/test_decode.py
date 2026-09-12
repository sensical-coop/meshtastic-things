import base64
import os

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from meshtastic.protobuf import mesh_pb2, mqtt_pb2, telemetry_pb2

from common import decode

GATEWAY_ID = "!aabbccdd"
MESH_ID = "mesh-1"


def _build_envelope(mp: mesh_pb2.MeshPacket, channel_id: str = "LongFast", gateway_id: str = GATEWAY_ID) -> bytes:
    se = mqtt_pb2.ServiceEnvelope()
    se.packet.CopyFrom(mp)
    se.channel_id = channel_id
    se.gateway_id = gateway_id
    return se.SerializeToString()


def _encrypted_packet(packet_id: int, node_id: int, data: mesh_pb2.Data, key_bytes: bytes) -> mesh_pb2.MeshPacket:
    mp = mesh_pb2.MeshPacket()
    mp.id = packet_id
    setattr(mp, "from", node_id)
    nonce = mp.id.to_bytes(8, "little") + getattr(mp, "from").to_bytes(8, "little")
    cipher = Cipher(algorithms.AES(key_bytes), modes.CTR(nonce), backend=default_backend())
    encryptor = cipher.encryptor()
    mp.encrypted = encryptor.update(data.SerializeToString()) + encryptor.finalize()
    return mp


def _plaintext_packet(packet_id: int, node_id: int, data: mesh_pb2.Data) -> mesh_pb2.MeshPacket:
    mp = mesh_pb2.MeshPacket()
    mp.id = packet_id
    setattr(mp, "from", node_id)
    mp.decoded.CopyFrom(data)
    return mp


def _gateway_state(key_bytes: bytes | None = None, mesh_id: str | None = MESH_ID) -> dict:
    psk_b64 = base64.b64encode(key_bytes).decode("ascii") if key_bytes else ""
    return {GATEWAY_ID: {"psk_b64": psk_b64, "mesh_id": mesh_id}}


def test_unregistered_gateway_returns_none():
    data = mesh_pb2.Data(portnum=67, payload=b"irrelevant")
    raw = _build_envelope(_plaintext_packet(1, 111, data))

    assert decode.decode_packet(raw, gateway_state={}, rejected_nodes=set()) is None


def test_rejected_node_returns_none():
    data = mesh_pb2.Data(portnum=67, payload=b"irrelevant")
    raw = _build_envelope(_plaintext_packet(1, 111, data))

    result = decode.decode_packet(
        raw, gateway_state=_gateway_state(), rejected_nodes={f"{MESH_ID}:111"}
    )
    assert result is None


def test_encrypted_with_correct_key_produces_decoded_and_sighting():
    key_bytes = os.urandom(32)
    pos = mesh_pb2.Position(latitude_i=123456789, longitude_i=-987654321)
    data = mesh_pb2.Data(portnum=3, payload=pos.SerializeToString())  # POSITION_APP
    raw = _build_envelope(_encrypted_packet(42, 999, data, key_bytes))

    result = decode.decode_packet(raw, gateway_state=_gateway_state(key_bytes), rejected_nodes=set())

    assert result is not None
    assert result["decoded"]["payload_kind"] == "position"
    assert result["decoded"]["mesh_id"] == MESH_ID
    assert result["sighting"]["key"] == f"{MESH_ID}:999"
    assert result["sighting"]["latitude"] == 12.3456789
    assert result["sighting"]["longitude"] == -98.7654321
    assert "discovery" not in result


def test_encrypted_with_wrong_key_returns_none():
    key_bytes = os.urandom(32)
    wrong_key = os.urandom(32)
    data = mesh_pb2.Data(portnum=67, payload=b"irrelevant")
    raw = _build_envelope(_encrypted_packet(1, 1, data, key_bytes))

    result = decode.decode_packet(raw, gateway_state=_gateway_state(wrong_key), rejected_nodes=set())
    assert result is None


def test_nodeinfo_sighting_promotes_identity_fields():
    info = mesh_pb2.User(long_name="Node One", short_name="N1", hw_model="TBEAM")
    data = mesh_pb2.Data(portnum=4, payload=info.SerializeToString())  # NODEINFO_APP
    raw = _build_envelope(_plaintext_packet(2, 222, data))

    result = decode.decode_packet(raw, gateway_state=_gateway_state(), rejected_nodes=set())

    assert result["sighting"]["name"] == "Node One"
    assert result["sighting"]["short_name"] == "N1"
    assert result["sighting"]["hardware_type"] == "TBEAM"


def test_telemetry_produces_discovery_with_numeric_fields():
    tel = telemetry_pb2.Telemetry()
    tel.device_metrics.battery_level = 87
    data = mesh_pb2.Data(portnum=67, payload=tel.SerializeToString())  # TELEMETRY_APP
    raw = _build_envelope(_plaintext_packet(3, 333, data))

    result = decode.decode_packet(raw, gateway_state=_gateway_state(), rejected_nodes=set())

    assert result["discovery"]["fields"] == ["battery_level"]
    assert result["discovery"]["payload_kind"] == "telemetry:device_metrics"


def _drops(raw, gateway_state, rejected_nodes=frozenset()):
    """decode_packet's drop reasons, which are otherwise invisible."""
    reasons = []
    result = decode.decode_packet(raw, gateway_state, rejected_nodes, on_drop=reasons.append)
    assert result is None
    return reasons


def test_a_wrong_mesh_key_says_so_rather_than_dropping_silently():
    """The failure that cost real debugging time: a registered gateway whose
    channel transmits with a different key looks exactly like no traffic."""
    key_bytes = os.urandom(32)
    data = mesh_pb2.Data(portnum=67, payload=b"irrelevant")
    raw = _build_envelope(_encrypted_packet(1, 111, data, key_bytes))

    reasons = _drops(raw, _gateway_state(os.urandom(32)))

    assert len(reasons) == 1
    assert "decrypt" in reasons[0]
    assert "mesh key" in reasons[0]


def test_unsupported_portnum_is_reported():
    data = mesh_pb2.Data(portnum=80, payload=b"\x08\x01")  # no protobuf schema
    raw = _build_envelope(_plaintext_packet(5, 555, data))

    reasons = _drops(raw, _gateway_state())

    assert len(reasons) == 1
    assert "portnum 80" in reasons[0]


def test_unregistered_gateway_and_rejected_node_are_reported():
    data = mesh_pb2.Data(portnum=67, payload=b"irrelevant")
    raw = _build_envelope(_plaintext_packet(1, 111, data))

    assert "unregistered gateway" in _drops(raw, {})[0]
    assert "rejected node" in _drops(raw, _gateway_state(), {f"{MESH_ID}:111"})[0]


def test_on_drop_is_optional_and_decoding_never_reports_a_drop():
    tel = telemetry_pb2.Telemetry()
    tel.device_metrics.battery_level = 50
    data = mesh_pb2.Data(portnum=67, payload=tel.SerializeToString())
    raw = _build_envelope(_plaintext_packet(6, 666, data))
    reasons = []

    result = decode.decode_packet(raw, _gateway_state(), set(), on_drop=reasons.append)

    assert result is not None and reasons == []
    # Callers that do not care keep working unchanged.
    assert decode.decode_packet(raw, _gateway_state(), set()) is not None


def test_no_mesh_id_skips_sighting_and_discovery():
    tel = telemetry_pb2.Telemetry()
    tel.device_metrics.battery_level = 50
    data = mesh_pb2.Data(portnum=67, payload=tel.SerializeToString())
    raw = _build_envelope(_plaintext_packet(4, 444, data))

    result = decode.decode_packet(
        raw, gateway_state=_gateway_state(mesh_id=None), rejected_nodes=set()
    )

    assert result is not None
    assert "mesh_id" not in result["decoded"]
    assert "sighting" not in result
    assert "discovery" not in result
