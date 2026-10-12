# Dummy replacement for the auto-generated gRPC stub file.
# When NFS-Ganesha team provides the real .proto file, run:
#   python -m grpc_tools.protoc nfs_ganesha.proto
# and replace this file with the generated output.

from . import nfs_ganesha_pb2


class NfsUpgradeStub:
    """Dummy stub that mimics the real gRPC NfsUpgrade service stub.
    Returns hardcoded responses so cephadm logic can be tested end-to-end
    before NFS-Ganesha implements the real gRPC server.
    """

    def __init__(self, channel: object) -> None:
        # In real stub: channel is used to send requests over the network.
        # In dummy: we ignore it and return fake responses.
        self._channel = channel

    def OkToUpgrade(self, request: nfs_ganesha_pb2.OkToUpgradeRequest) \
            -> nfs_ganesha_pb2.OkToUpgradeResponse:
        # Dummy: always says the requested daemon is safe to upgrade.
        # Real implementation: NFS-Ganesha checks TSM state and returns
        # actual list of daemons safe to upgrade together.
        return nfs_ganesha_pb2.OkToUpgradeResponse(
            ok_to_upgrade=True,
            daemons=[request.to_upgrade],
        )

    def UpgradeComplete(self, request: nfs_ganesha_pb2.UpgradeCompleteRequest) \
            -> nfs_ganesha_pb2.UpgradeCompleteResponse:
        # Dummy: always acknowledges.
        return nfs_ganesha_pb2.UpgradeCompleteResponse(acknowledged=True)
