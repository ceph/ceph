# NFS rolling upgrade gRPC stub.
# Uses channel.unary_unary() for raw gRPC calls — no protobuf dependency.
# Calls the real tsmService.tsmMember/IsItOkToUpgrade API.


class OkToUpgradeRequest:
    def __init__(self, ganesha_id: int = 0) -> None:
        self.ganesha_id = ganesha_id


class OkToUpgradeResponse:
    def __init__(self, ganesha_id: int = 0, ok_to_upgrade: bool = False) -> None:
        self.ganesha_id = ganesha_id
        self.ok_to_upgrade = ok_to_upgrade


def _serialize_request(request: OkToUpgradeRequest) -> bytes:
    """Serialize rollingUpgradeRequest — field 1 (uint32 ganesha_id)."""
    if request.ganesha_id == 0:
        return b''
    return bytes([0x08, request.ganesha_id & 0x7f])


def _parse_response(data: bytes) -> OkToUpgradeResponse:
    """Parse rollingUpgradeResponse — extract ganesha_id and ok_to_upgrade."""
    resp = OkToUpgradeResponse()
    i = 0
    while i < len(data) - 1:
        if data[i] == 0x08:     # field 1: ganesha_id
            resp.ganesha_id = data[i + 1]
        elif data[i] == 0x10:   # field 2: ok_to_upgrade
            resp.ok_to_upgrade = bool(data[i + 1])
        i += 1
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
