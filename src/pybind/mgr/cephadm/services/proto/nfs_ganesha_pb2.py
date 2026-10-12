# Dummy replacement for the auto-generated protobuf file.
# When NFS-Ganesha team provides the real .proto file, run:
#   python -m grpc_tools.protoc nfs_ganesha.proto
# and replace this file with the generated output.

from typing import List


class OkToUpgradeRequest:
    def __init__(self, ganesha_id: int = 0) -> None:
        self.ganesha_id = ganesha_id   # integer rank of the daemon to upgrade


class OkToUpgradeResponse:
    def __init__(self, ganesha_id: int = 0, ok_to_upgrade: bool = False,
                 nodes_list: List[int] = [], length: int = 0) -> None:
        self.ganesha_id    = ganesha_id     # which node was asked
        self.ok_to_upgrade = ok_to_upgrade  # safe to upgrade?
        self.nodes_list    = nodes_list     # optional batch of safe integer ranks
        self.length        = length         # number of nodes in nodes_list
