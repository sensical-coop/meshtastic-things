import base64
import os

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from meshtastic.protobuf import mesh_pb2, mqtt_pb2, telemetry_pb2

from common import codec


def _build_envelope(mp: mesh_pb2.MeshPacket, channel_id: str = "LongFast", gateway_id: str = "!aabbccdd") -> bytes:
    se = mqtt_pb2.ServiceEnvelope()
    se.packet.CopyFrom(mp)
    se.channel_id = channel_id
    se.gateway_id = gateway_id
    return se.SerializeToString()


def _telemetry_packet(packet_id: int, node_id: int, battery_level: int) -> mesh_pb2.MeshPacket:
    tel = telemetry_pb2.Telemetry()
    tel.time = 1700000010
    tel.device_metrics.battery_level = battery_level

    data = mesh_pb2.Data()
    data.portnum = 67  # TELEMETRY_APP
    data.payload = tel.SerializeToString()

    mp = mesh_pb2.MeshPacket()
    mp.id = packet_id
    setattr(mp, "from", node_id)
    mp.rx_time = 1700000001
    mp.decoded.CopyFrom(data)
    return mp


def test_decode_plaintext_telemetry():
    mp = _telemetry_packet(packet_id=1, node_id=123456, battery_level=87)
    raw = _build_envelope(mp)

    result = codec.decode_message(raw, key_bytes=None)

    assert result is not None
    assert result["node_id"] == 123456
    assert result["packet_id"] == 1
    assert result["portnum"] == 67
    assert result["channel_id"] == "LongFast"
    assert result["gateway_id"] == "!aabbccdd"
    assert result["payload_kind"] == "telemetry:device_metrics"
    assert result["payload"]["device_metrics"]["battery_level"] == 87


def test_decode_encrypted_with_correct_key():
    key_bytes = os.urandom(32)

    data = mesh_pb2.Data()
    data.portnum = 3  # POSITION_APP
    pos = mesh_pb2.Position()
    pos.latitude_i = 123456789
    pos.longitude_i = -987654321
    data.payload = pos.SerializeToString()

    mp = mesh_pb2.MeshPacket()
    mp.id = 42
    setattr(mp, "from", 999)
    mp.rx_time = 1700000000
    nonce = mp.id.to_bytes(8, "little") + getattr(mp, "from").to_bytes(8, "little")
    cipher = Cipher(algorithms.AES(key_bytes), modes.CTR(nonce), backend=default_backend())
    encryptor = cipher.encryptor()
    mp.encrypted = encryptor.update(data.SerializeToString()) + encryptor.finalize()

    raw = _build_envelope(mp)
    result = codec.decode_message(raw, key_bytes=key_bytes)

    assert result is not None
    assert result["payload_kind"] == "position"
    assert result["payload"]["latitude_i"] == 123456789


def test_decode_encrypted_with_wrong_key_returns_none_not_raises():
    key_bytes = os.urandom(32)
    wrong_key = os.urandom(32)

    data = mesh_pb2.Data()
    data.portnum = 67
    data.payload = b"irrelevant"

    mp = mesh_pb2.MeshPacket()
    mp.id = 1
    setattr(mp, "from", 1)
    nonce = mp.id.to_bytes(8, "little") + getattr(mp, "from").to_bytes(8, "little")
    cipher = Cipher(algorithms.AES(key_bytes), modes.CTR(nonce), backend=default_backend())
    encryptor = cipher.encryptor()
    mp.encrypted = encryptor.update(data.SerializeToString()) + encryptor.finalize()

    raw = _build_envelope(mp)
    result = codec.decode_message(raw, key_bytes=wrong_key)

    assert result is None


def test_decode_psk_expands_default_channel_key_shorthand():
    default_key = bytes(
        [0xD4, 0xF1, 0xBB, 0x3A, 0x20, 0x29, 0x07, 0x59, 0xF0, 0xBC, 0xFF, 0xAB, 0xCF, 0x4E, 0x69, 0x01]
    )
    assert codec.decode_psk("AQ==") == default_key  # index 1: the literal default key
    assert codec.decode_psk("Ag==") == default_key[:-1] + bytes([0x02])  # index 2: simple2
    assert codec.decode_psk("Cg==") == default_key[:-1] + bytes([0x0A])  # index 10: simple10


def test_decode_psk_treats_zero_byte_as_no_crypto():
    assert codec.decode_psk("AA==") is None
    assert codec.decode_psk("") is None
    assert codec.decode_psk(None) is None


def test_decode_psk_decodes_real_key():
    key_bytes = os.urandom(16)
    key_b64 = base64.b64encode(key_bytes).decode("ascii")
    assert codec.decode_psk(key_b64) == key_bytes
