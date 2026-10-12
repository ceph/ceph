# Dummy replacement for the auto-generated protobuf file.
# When NFS-Ganesha team provides the real .proto file, run:
#   python -m grpc_tools.protoc nfs_ganesha.proto
# and replace this file with the generated output.

from typing import List


class EmptyRequest:
    pass


class OkToUpgradeRequest:
    def __init__(self, to_upgrade: str = '', already_upgraded: List[str] = []) -> None:
        self.to_upgrade = to_upgrade
        self.already_upgraded = already_upgraded


class OkToUpgradeResponse:
    def __init__(self, ok_to_upgrade: bool = False, daemons: List[str] = []) -> None:
        self.ok_to_upgrade = ok_to_upgrade
        self.daemons = daemons


class UpgradeCompleteRequest:
    def __init__(self, already_upgraded: List[str] = []) -> None:
        self.already_upgraded = already_upgraded


class UpgradeCompleteResponse:
    def __init__(self, acknowledged: bool = False) -> None:
        self.acknowledged = acknowledged
