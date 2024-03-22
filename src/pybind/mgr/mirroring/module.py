from typing import List, Optional

from mgr_module import MgrModule, CLIReadCommand, CLIWriteCommand, Option, NotifyType

from .fs.snapshot_mirror import FSSnapshotMirror

class Module(MgrModule):
    MODULE_OPTIONS: List[Option] = []
    NOTIFY_TYPES = [NotifyType.fs_map]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fs_snapshot_mirror = FSSnapshotMirror(self)

    def notify(self, notify_type: NotifyType, notify_id):
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
                                 remote_fs_name: Optional[str] = None,
                                 remote_mon_host: Optional[str] = None,
                                 cephx_key: Optional[str] = None):
        """Add a remote filesystem peer"""
        conf = {}
        if remote_mon_host and cephx_key:
            conf['mon_host'] = remote_mon_host
            conf['key'] = cephx_key
        return self.fs_snapshot_mirror.peer_add(fs_name, remote_cluster_spec,
                                                remote_fs_name, remote_conf=conf)

    @CLIReadCommand('fs snapshot mirror peer_list')
    def snapshot_mirror_peer_list(self,
                                  fs_name: str):
        """List configured peers for a file system"""
        return self.fs_snapshot_mirror.peer_list(fs_name)

    @CLIWriteCommand('fs snapshot mirror peer_remove')
    def snapshot_mirror_peer_remove(self,
                                    fs_name: str,
                                    peer_uuid: str):
        """Remove a filesystem peer"""
        return self.fs_snapshot_mirror.peer_remove(fs_name, peer_uuid)

    @CLIWriteCommand('fs snapshot mirror peer_bootstrap create')
    def snapshot_mirror_peer_bootstrap_create(self,
                                              fs_name: str,
                                              client_name: str,
                                              site_name: str):
        """Bootstrap a filesystem peer"""
        return self.fs_snapshot_mirror.peer_bootstrap_create(fs_name, client_name, site_name)

    @CLIWriteCommand('fs snapshot mirror peer_bootstrap import')
    def snapshot_mirror_peer_bootstrap_import(self,
                                              fs_name: str,
                                              token: str):
        """Import a bootstrap token"""
        return self.fs_snapshot_mirror.peer_bootstrap_import(fs_name, token)

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
    def snapshot_mirror_daemon_status(self):
        """Get mirror daemon status"""
        return self.fs_snapshot_mirror.daemon_status()
