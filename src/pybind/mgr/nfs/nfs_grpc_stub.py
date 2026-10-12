# NFS rolling upgrade gRPC stub.
# Uses channel.unary_unary() for raw gRPC calls — no protobuf dependency.
# Calls the real tsmService.tsmMember/IsItOkToUpgrade API.
# Defaults to ok_to_upgrade=False (not safe) on any parse failure.

from typing import Any, Tuple

DEFAULT_GRPC_PORT = 50051


def get_grpc_port(spec: Any = None) -> int:
    """Return the NFS-Ganesha gRPC server port.

    Returns default port for now. Will be updated to fetch
    the actual port from daemon spec once the NFS team adds it.
    """
    return DEFAULT_GRPC_PORT


class OkToUpgradeRequest:
    def __init__(self, ganesha_id: int = 0) -> None:
        self.ganesha_id = ganesha_id


class OkToUpgradeResponse:
    def __init__(self, ganesha_id: int = 0, ok_to_upgrade: bool = False,
                 nodes: str = '', length: int = 0) -> None:
        self.ganesha_id = ganesha_id
        self.ok_to_upgrade = ok_to_upgrade
        self.nodes = nodes
        self.length = length


def _encode_varint(value: int) -> bytes:
    """Encode an unsigned integer as a protobuf varint."""
    result = []
    while value > 0x7f:
        result.append((value & 0x7f) | 0x80)
        value >>= 7
    result.append(value & 0x7f)
    return bytes(result)


def _decode_varint(data: bytes, pos: int) -> Tuple[int, int]:
    """Decode a varint starting at pos. Returns (value, new_pos)."""
    value = 0
    shift = 0
    while pos < len(data):
        b = data[pos]
        value |= (b & 0x7f) << shift
        pos += 1
        if not (b & 0x80):
            return value, pos
        shift += 7
    raise ValueError('truncated varint')


def _serialize_request(request: OkToUpgradeRequest) -> bytes:
    """Serialize rollingUpgradeRequest with proper varint encoding."""
    if request.ganesha_id == 0:
        return b''
    return b'\x08' + _encode_varint(request.ganesha_id)


def _parse_response(data: bytes) -> OkToUpgradeResponse:
    """Parse rollingUpgradeResponse with proper wire-type handling.

    Defaults to ok_to_upgrade=False on any parse error — the safety
    gate must never default to permissive.
    """
    resp = OkToUpgradeResponse()
    try:
        i = 0
        while i < len(data):
            tag_byte, i = _decode_varint(data, i)
            field_number = tag_byte >> 3
            wire_type = tag_byte & 0x07

            if wire_type == 0:  # varint
                value, i = _decode_varint(data, i)
                if field_number == 1:
                    resp.ganesha_id = value
                elif field_number == 2:
                    resp.ok_to_upgrade = bool(value)
            elif wire_type == 2:  # length-delimited (string, bytes)
                length, i = _decode_varint(data, i)
                if i + length > len(data):
                    return OkToUpgradeResponse()
                i += length
            elif wire_type == 5:  # 32-bit fixed
                if i + 4 > len(data):
                    return OkToUpgradeResponse()
                i += 4
            elif wire_type == 1:  # 64-bit fixed
                if i + 8 > len(data):
                    return OkToUpgradeResponse()
                i += 8
            else:
                # Unknown wire type — cannot skip safely, default to not safe
                return OkToUpgradeResponse()
    except Exception:
        # Parse error — default to not safe
        return OkToUpgradeResponse()
    return resp


class NfsUpgradeStub:
    """Stub for tsmService.tsmMember gRPC service — calls IsItOkToUpgrade."""

    def __init__(self, channel) -> None:  # type: ignore
        self._is_it_ok = channel.unary_unary(
            '/tsmService.tsmMember/IsItOkToUpgrade',
            request_serializer=_serialize_request,
            response_deserializer=_parse_response,
        )

    def OkToUpgrade(self, request: OkToUpgradeRequest) -> OkToUpgradeResponse:
        """Call the real IsItOkToUpgrade gRPC API on NFS-Ganesha."""
        return self._is_it_ok(request, timeout=30)
