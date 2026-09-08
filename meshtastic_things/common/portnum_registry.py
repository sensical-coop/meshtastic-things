from meshtastic.protobuf import portnums_pb2

_NAMED_PORTNUMS = {
    portnums_pb2.PortNum.POSITION_APP: "position",
    portnums_pb2.PortNum.NODEINFO_APP: "nodeinfo",
    portnums_pb2.PortNum.TELEMETRY_APP: "telemetry",
}

_TELEMETRY_ONEOF = "variant"


def classify(portnum: int, pb) -> str:
    """Identify the shape of a decoded payload for a portnum.

    Add a new portnum here to make it cascade everywhere else.
    """
    base = _NAMED_PORTNUMS.get(portnum)
    if base is None:
        return f"portnum:{portnum}"
    if base == "telemetry":
        variant = pb.WhichOneof(_TELEMETRY_ONEOF) if pb is not None else None
        return f"telemetry:{variant}" if variant else "telemetry"
    return base
