import logging

from mgr_module import MgrModule

from .fs.snapshot_mirror import FSSnapshotMirror

log = logging.getLogger(__name__)

class Module(MgrModule):
    COMMANDS = [
        {
            'cmd':  'fs snapshot mirror enable '
                    'name=fs,type=CephString ',
            'desc': 'enable snapshot mirroring for a filesystem',
            'perm': 'rw'
        },
        {
            'cmd':  'fs snapshot mirror disable '
                    'name=fs,type=CephString ',
            'desc': 'disable snapshot mirroring for a filesystem',
            'perm': 'rw'
        },
        {
            'cmd':  'fs snapshot mirror peer_add '
                    'name=fs,type=CephString '
                    'name=remote_cluster_spec,type=CephString '
                    'name=remote_fs_name,type=CephString,req=false',
            'desc': 'add a mirror peer for a ceph filesystem',
            'perm': 'rw'
        },
        {
            'cmd':  'fs snapshot mirror peer_remove '
                    'name=fs,type=CephString '
                    'name=peer_uuid,type=CephString ',
            'desc': 'remove a mirror peer for a ceph filesystem',
            'perm': 'rw'
        },
        {
            'cmd':  'fs snapshot mirror add '
                    'name=fs,type=CephString '
                    'name=path,type=CephString ',
            'desc': 'add a directory for snapshot mirroring',
            'perm': 'rw'
        },
        {
            'cmd':  'fs snapshot mirror remove '
                    'name=fs,type=CephString '
                    'name=path,type=CephString ',
            'desc': 'remove a directory for snapshot mirroring',
            'perm': 'rw'
            },
        {
            'cmd':  'fs snapshot mirror dirmap '
                    'name=fs,type=CephString '
                    'name=path,type=CephString ',
            'desc': 'retrieve status for mirrored directory',
            'perm': 'r'
            },
        {
            'cmd':  'fs snapshot mirror show distribution '
                    'name=fs,type=CephString ',
            'desc': 'retrieve status for mirrored directory',
            'perm': 'r'
            },
        ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.fs_snapshot_mirror = FSSnapshotMirror(self)

    def notify(self, notify_type, notify_id):
        self.fs_snapshot_mirror.notify(notify_type)

    def handle_command(self, inbuf, cmd):
        prefix = cmd['prefix']
        if prefix.startswith('fs snapshot mirror enable'):
            return self.fs_snapshot_mirror.enable_mirror(cmd['fs'])
        elif prefix.startswith('fs snapshot mirror disable'):
            return self.fs_snapshot_mirror.disable_mirror(cmd['fs'])
        elif prefix.startswith('fs snapshot mirror add'):
            return self.fs_snapshot_mirror.add_dir(cmd['fs'], cmd['path'])
        elif prefix.startswith('fs snapshot mirror remove'):
            return self.fs_snapshot_mirror.remove_dir(cmd['fs'], cmd['path'])
        elif prefix.startswith('fs snapshot mirror peer_add'):
            return self.fs_snapshot_mirror.peer_add(cmd['fs'], cmd['remote_cluster_spec'],
                                                    cmd.get('remote_fs_name', None))
        elif prefix.startswith('fs snapshot mirror peer_remove'):
            return self.fs_snapshot_mirror.peer_remove(cmd['fs'], cmd['peer_uuid'])
        elif prefix.startswith('fs snapshot mirror dirmap'):
            return self.fs_snapshot_mirror.status(cmd['fs'], cmd['path'])
        elif prefix.startswith('fs snapshot mirror show distribution'):
            return self.fs_snapshot_mirror.show_distribution(cmd['fs'])
        else:
            raise NotImplementedError()
