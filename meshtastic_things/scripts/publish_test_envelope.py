#!/usr/bin/env python3
"""Publish a Meshtastic raw envelope onto a Kafka topic"""
import argparse
import base64
import os

from kafka import KafkaProducer
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from meshtastic.protobuf import mesh_pb2, mqtt_pb2, telemetry_pb2


def build_telemetry_data(battery_level=None, voltage=None, temperature=None) -> bytes:
    tel = telemetry_pb2.Telemetry()
    if temperature is not None:
        tel.environment_metrics.temperature = temperature
    else:
        if battery_level is not None:
            tel.device_metrics.battery_level = battery_level
        if voltage is not None:
            tel.device_metrics.voltage = voltage

    data = mesh_pb2.Data()
    data.portnum = 67  # TELEMETRY_APP
    data.payload = tel.SerializeToString()
    return data.SerializeToString()


def build_position_data(latitude: float, longitude: float, altitude: float | None) -> bytes:
    pos = mesh_pb2.Position()
    pos.latitude_i = int(latitude * 1e7)
    pos.longitude_i = int(longitude * 1e7)
    if altitude is not None:
        pos.altitude = int(altitude)

    data = mesh_pb2.Data()
    data.portnum = 3  # POSITION_APP
    data.payload = pos.SerializeToString()
    return data.SerializeToString()


def build_nodeinfo_data(long_name: str, short_name: str) -> bytes:
    user = mesh_pb2.User()
    user.long_name = long_name
    user.short_name = short_name

    data = mesh_pb2.Data()
    data.portnum = 4  # NODEINFO_APP
    data.payload = user.SerializeToString()
    return data.SerializeToString()


def encrypt(data_bytes: bytes, packet_id: int, node_id: int, key_b64: str) -> bytes:
    key_bytes = base64.b64decode(key_b64.encode("ascii"))
    nonce = packet_id.to_bytes(8, "little") + node_id.to_bytes(8, "little")
    cipher = Cipher(algorithms.AES(key_bytes), modes.CTR(nonce), backend=default_backend())
    encryptor = cipher.encryptor()
    return encryptor.update(data_bytes) + encryptor.finalize()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_LOCAL", "localhost:9092"))
    parser.add_argument("--topic", default=os.environ.get("MQTT_BRIDGE__KAFKA_TOPIC", "mesh.telemetry.raw.v1"))
    parser.add_argument("--kind", choices=["telemetry", "position", "nodeinfo"], default="telemetry")
    parser.add_argument("--node-id", type=int, required=True)
    parser.add_argument("--packet-id", type=int, default=1)
    parser.add_argument("--channel-id", default="LongFast")
    parser.add_argument("--gateway-id", default="!testgw")
    parser.add_argument("--rx-time", type=int, default=0)
    parser.add_argument(
        "--encrypt-with-key",
        dest="key_b64",
        default=None,
        help="base64 AES key; if set, the packet is sent AES-CTR encrypted instead of plaintext-decoded",
    )
    parser.add_argument("--battery-level", type=int, default=87)
    parser.add_argument("--voltage", type=float, default=None)
    parser.add_argument("--temperature", type=float, default=None, help="sets environment_metrics instead of device_metrics")
    parser.add_argument("--latitude", type=float, default=41.3851)
    parser.add_argument("--longitude", type=float, default=2.1734)
    parser.add_argument("--altitude", type=float, default=50)
    parser.add_argument("--long-name", default="Test Node")
    parser.add_argument("--short-name", default="TST")
    args = parser.parse_args()

    if args.kind == "telemetry":
        data_bytes = build_telemetry_data(args.battery_level, args.voltage, args.temperature)
    elif args.kind == "position":
        data_bytes = build_position_data(args.latitude, args.longitude, args.altitude)
    else:
        data_bytes = build_nodeinfo_data(args.long_name, args.short_name)

    mp = mesh_pb2.MeshPacket()
    mp.id = args.packet_id
    setattr(mp, "from", args.node_id)
    mp.rx_time = args.rx_time
    mp.hop_start = 1

    if args.key_b64:
        mp.encrypted = encrypt(data_bytes, args.packet_id, args.node_id, args.key_b64)
    else:
        data = mesh_pb2.Data()
        data.ParseFromString(data_bytes)
        mp.decoded.CopyFrom(data)

    se = mqtt_pb2.ServiceEnvelope()
    se.packet.CopyFrom(mp)
    se.channel_id = args.channel_id
    se.gateway_id = args.gateway_id
    raw = se.SerializeToString()

    producer = KafkaProducer(bootstrap_servers=args.bootstrap)
    producer.send(args.topic, value=raw)
    producer.flush()

    print(
        f"Published {args.kind} envelope: node_id={args.node_id} packet_id={args.packet_id} "
        f"channel_id={args.channel_id} encrypted={bool(args.key_b64)} -> {args.topic} "
        f"({len(raw)} bytes) @ {args.bootstrap}"
    )


if __name__ == "__main__":
    main()
