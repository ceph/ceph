from typing import List, Optional

from mgr_module import MgrModule, CLIReadCommand, CLIWriteCommand, Option

from .fs.snapshot_mirror import FSSnapshotMirror

class Module(MgrModule):
    MODULE_OPTIONS: List[Option] = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fs_snapshot_mirror = FSSnapshotMirror(self)

    def notify(self, notify_type, notify_id):
        self.fs_snapshot_mirror.notify(notify_type)

    @CLIWriteCommand('fs snapshot mirror enable')
    def snapshot_mirror_enable(self,
                               fs_name: str):
        """Enable snapshot mirroring for a filesystem"""
        return self.fs_snapshot_mirror.enable_mirror(fs_name)

    @CLIWriteCommand('fs snapshot mirror disable')
    def snapshot_mirror_disable(self,
                                fs_name: str):
        """Disable snapshot mirroring for a filesystem"""
        return self.fs_snapshot_mirror.disable_mirror(fs_name)

    @CLIWriteCommand('fs snapshot mirror peer_add')
    def snapshot_mirorr_peer_add(self,
                                 fs_name: str,
                                 remote_cluster_spec: str,
                                 remote_fs_name: Optional[str] = None):
        """Add a remote filesystem peer"""
        return self.fs_snapshot_mirror.peer_add(fs_name, remote_cluster_spec,
                                                remote_fs_name)

    @CLIWriteCommand('fs snapshot mirror peer_remove')
    def snapshot_mirror_peer_remove(self,
                                    fs_name: str,
                                    peer_uuid: str):
        """Remove a filesystem peer"""
        return self.fs_snapshot_mirror.peer_remove(fs_name, peer_uuid)

    @CLIWriteCommand('fs snapshot mirror add')
    def snapshot_mirror_add_dir(self,
                                fs_name: str,
                                path: str):
        """Add a directory for snapshot mirroring"""
        return self.fs_snapshot_mirror.add_dir(fs_name, path)

    @CLIWriteCommand('fs snapshot mirror remove')
    def snapshot_mirror_remove_dir(self,
                                   fs_name: str,
                                   path: str):
        """Remove a snapshot mirrored directory"""
        return self.fs_snapshot_mirror.remove_dir(fs_name, path)

    @CLIReadCommand('fs snapshot mirror dirmap')
    def snapshot_mirror_dirmap(self,
                               fs_name: str,
                               path: str):
        """Get current mirror instance map for a directory"""
        return self.fs_snapshot_mirror.status(fs_name, path)

    @CLIReadCommand('fs snapshot mirror show distribution')
    def snapshot_mirror_distribution(self,
                                     fs_name: str):
        """Get current instance to directory map for a filesystem"""
        return self.fs_snapshot_mirror.show_distribution(fs_name)

    @CLIReadCommand('fs snapshot mirror daemon status')
    def snapshot_mirror_daemon_status(self,
                                      fs_name: str):
        """Get mirror daemon status"""
        return self.fs_snapshot_mirror.daemon_status(fs_name)
