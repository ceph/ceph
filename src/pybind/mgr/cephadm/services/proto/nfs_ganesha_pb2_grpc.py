# Stub file for NFS gRPC upgrade service.
# OkToUpgrade calls GetGracePeriod (existing NFS-Ganesha API) as a proxy
# until NFS-Ganesha implements the real OkToUpgrade gRPC method.
# Request uses integer ganesha_id (rank). No already_upgraded needed.

from . import nfs_ganesha_pb2


def _parse_grace_response(data: bytes) -> bool:
    """Parse GetNfsGraceResponse — field 1 (optional bool ingrace) = 0x08 0x00/0x01."""
    if data and len(data) >= 2 and data[0] == 0x08:
        return bool(data[1])
    return False


class NfsUpgradeStub:
    """Stub for NFS upgrade gRPC service.

    OkToUpgrade currently calls GetGracePeriod (existing NFS-Ganesha API)
    to validate the full gRPC connection path.
    Replace with real OkToUpgrade call once NFS-Ganesha implements it.
    """

    def __init__(self, channel: object) -> None:
        self._channel = channel
        self._get_grace = channel.unary_unary(
            '/nfsService.GetNfsGrace/GetGracePeriod',
            request_serializer=lambda x: b'',
            response_deserializer=_parse_grace_response,
        )

    def OkToUpgrade(self, request: nfs_ganesha_pb2.OkToUpgradeRequest) \
            -> nfs_ganesha_pb2.OkToUpgradeResponse:
        """Call GetGracePeriod as proxy for the real OkToUpgrade API.

        Returns ok_to_upgrade=True when NFS is NOT in grace period.
        Errors are NOT caught — caller handles them via _handle_grpc_error().
        """
        ingrace = self._get_grace(None)
        return nfs_ganesha_pb2.OkToUpgradeResponse(
            ganesha_id=request.ganesha_id,
            ok_to_upgrade=not ingrace,
        )
