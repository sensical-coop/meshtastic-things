from common import codec


def _discovered_fields(payload_kind: str, payload: dict) -> list[str]:
    """telemetry:<variant> only, numeric leaf fields only."""
    if not payload_kind.startswith("telemetry:"):
        return []
    variant = payload_kind.split(":", 1)[1]
    sub = payload.get(variant)
    if not isinstance(sub, dict):
        return []
    return [k for k, v in sub.items() if isinstance(v, (int, float)) and not isinstance(v, bool)]


def _build_sighting(mesh_id: str, decoded: dict) -> dict:
    sighting = {
        "key": f"{mesh_id}:{decoded['node_id']}",
        "mesh_id": mesh_id,
        "device_id": decoded["node_id"],
        "op": "upsert",
    }
    payload = decoded["payload"]
    if decoded["payload_kind"] == "position":
        lat_i, lon_i = payload.get("latitude_i"), payload.get("longitude_i")
        if lat_i is not None and lon_i is not None:
            sighting["latitude"] = lat_i / 1e7
            sighting["longitude"] = lon_i / 1e7
    elif decoded["payload_kind"] == "nodeinfo":
        for src_field, sighting_key in (
            ("long_name", "name"),
            ("short_name", "short_name"),
            ("hw_model", "hardware_type"),
            ("role", "role"),
        ):
            if payload.get(src_field):
                sighting[sighting_key] = payload[src_field]
    return sighting


def _build_discovery(mesh_id: str, decoded: dict, fields: list[str]) -> dict:
    return {
        "mesh_id": mesh_id,
        "device_id": decoded["node_id"],
        "payload_kind": decoded["payload_kind"],
        "fields": fields,
        "op": "upsert",
    }


def _drop(on_drop, reason: str) -> None:
    if on_drop is not None:
        on_drop(reason)
    return None


def decode_packet(raw: bytes, gateway_state, rejected_nodes, on_drop=None) -> dict | None:
    """Decrypt/decode one raw ServiceEnvelope.

    Returns None to drop the packet (unregistered gateway, rejected node, or
    undecodable). Otherwise a dict with a "decoded" event and, when the
    gateway's mesh is known, "sighting" and possibly "discovery" events.

    Args:
        on_drop: Called with the reason whenever a packet is dropped. A wrong
            mesh key and an unsupported portnum are otherwise indistinguishable
            from no traffic at all. The two runtimes log differently, so each
            supplies its own.
    """
    se = codec.parse_envelope(raw)
    gateway_id = se.gateway_id or "unparseable"
    state = gateway_state.get(gateway_id)
    if state is None:
        return _drop(on_drop, f"unregistered gateway {gateway_id}")

    key_b64 = state.get("psk_b64")
    mesh_id = state.get("mesh_id")
    key_bytes = codec.decode_psk(key_b64) if key_b64 else None

    node_id = getattr(se.packet, "from")
    if mesh_id and f"{mesh_id}:{node_id}" in rejected_nodes:
        return _drop(on_drop, f"rejected node {mesh_id}:{node_id}")

    mp = se.packet
    if mp.HasField("encrypted") and not mp.HasField("decoded"):
        decrypted = codec.decrypt_packet(mp, key_bytes)
        if decrypted is None:
            return _drop(
                on_drop,
                f"could not decrypt packet from node {node_id} via gateway {gateway_id}; "
                f"the mesh key does not match what the channel is transmitting with",
            )
        mp.decoded.CopyFrom(decrypted)

    result = codec.decode_payload(mp)
    if result is None:
        return _drop(on_drop, f"portnum {mp.decoded.portnum} has no protobuf schema, nothing to store")
    portnum, pb_dict, payload_kind = result
    decoded = codec.build_decoded_message(se, portnum, pb_dict, payload_kind)
    if mesh_id:
        decoded["mesh_id"] = mesh_id

    events = {"decoded": decoded}
    if mesh_id:
        events["sighting"] = _build_sighting(mesh_id, decoded)
        fields = _discovered_fields(decoded["payload_kind"], decoded["payload"])
        if fields:
            events["discovery"] = _build_discovery(mesh_id, decoded, fields)
    return events
