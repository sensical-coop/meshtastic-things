import base64

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from google.protobuf.json_format import MessageToDict
from meshtastic import protocols
from meshtastic.protobuf import mesh_pb2, mqtt_pb2

from common import portnum_registry

# Channel.psk default AQ==
_DEFAULT_CHANNEL_KEY = bytes(
    [0xD4, 0xF1, 0xBB, 0x3A, 0x20, 0x29, 0x07, 0x59, 0xF0, 0xBC, 0xFF, 0xAB, 0xCF, 0x4E, 0x69, 0x01]
)


def parse_envelope(raw: bytes) -> mqtt_pb2.ServiceEnvelope:
    se = mqtt_pb2.ServiceEnvelope()
    se.ParseFromString(raw)
    return se

def parse_service_envelope(raw: bytes) -> mesh_pb2.MeshPacket:
    return parse_envelope(raw).packet

def decrypt_packet(mp: mesh_pb2.MeshPacket, key_bytes: bytes | None) -> mesh_pb2.Data | None:
    """Decrypt a payload. Returns None if there is a failure"""
    if not key_bytes:
        return None
    try:
        nonce = getattr(mp, "id").to_bytes(8, "little") + getattr(mp, "from").to_bytes(8, "little")
        cipher = Cipher(algorithms.AES(key_bytes), modes.CTR(nonce), backend=default_backend())
        decryptor = cipher.decryptor()
        decrypted_bytes = decryptor.update(getattr(mp, "encrypted")) + decryptor.finalize()

        data = mesh_pb2.Data()
        data.ParseFromString(decrypted_bytes)
        return data
    except Exception:
        # Do not log anything here
        return None

def decode_payload(mp: mesh_pb2.MeshPacket):
    """Returns (portnum, pb_dict, payload_kind) or None."""
    if not mp.HasField("decoded"):
        return None

    portnum = mp.decoded.portnum
    handler = protocols.get(portnum) if portnum else None
    if handler is None or handler.protobufFactory is None:
        return None

    pb = handler.protobufFactory()
    pb.ParseFromString(mp.decoded.payload)
    pb_dict = MessageToDict(pb, preserving_proto_field_name=True)
    payload_kind = portnum_registry.classify(portnum, pb)

    return portnum, pb_dict, payload_kind

def build_decoded_message(se: mqtt_pb2.ServiceEnvelope, portnum: int, pb_dict: dict, payload_kind: str) -> dict:
    mp = se.packet
    return {
        "node_id": getattr(mp, "from"),
        "packet_id": mp.id,
        "portnum": portnum,
        "rx_time": mp.rx_time,
        "hop_start": mp.hop_start,
        "channel_id": se.channel_id,
        "gateway_id": se.gateway_id,
        "payload_kind": payload_kind,
        "payload": pb_dict,
    }

def decode_message(raw: bytes, key_bytes: bytes | None) -> dict | None:
    """Parse, decrypt (if needed), decode, and shape the output.

    Returns None if the message can't be decrypted, has an unknown
    portnum, or has no protobuf schema for that portnum.
    """
    se = parse_envelope(raw)
    mp = se.packet

    if mp.HasField("encrypted") and not mp.HasField("decoded"):
        decrypted = decrypt_packet(mp, key_bytes)
        if decrypted is None:
            return None
        mp.decoded.CopyFrom(decrypted)

    result = decode_payload(mp)
    if result is None:
        return None

    portnum, pb_dict, payload_kind = result
    return build_decoded_message(se, portnum, pb_dict, payload_kind)


def decode_psk(key_b64: str) -> bytes | None:
    """Base64-decode a channel PSK read from the key store.

    """
    if not key_b64:
        return None
    raw = base64.b64decode(key_b64.encode("ascii"))
    if len(raw) == 1:
        index = raw[0]
        if index == 0:
            return None
        return _DEFAULT_CHANNEL_KEY[:-1] + bytes([(_DEFAULT_CHANNEL_KEY[-1] + index - 1) & 0xFF])
    return raw
