from threading import Event
import errno
import json
try:
    import queue as Queue
except ImportError:
    import Queue

from mgr_module import MgrModule
import orchestrator

from ceph_volume_client import CephFSVolumeClient, VolumePath

from .volume import VolumeIndex

class PurgeJob(object):
    def __init__(self, volume_fscid, subvolume_path):
        """
        Purge tasks work in terms of FSCIDs, so that if we process
        a task later when a volume was deleted and recreated with
        the same name, we can correctly drop the task that was
        operating on the original volume.
        """
        self.fscid = volume_fscid
        self.subvolume_path = subvolume_path


class Module(orchestrator.OrchestratorClientMixin, MgrModule):
    COMMANDS = [
        {
            'cmd': 'fs volume ls',
            'desc': "List volumes",
            'perm': 'r'
        },
        {
            'cmd': 'fs volume create '
                   'name=name,type=CephString '
                   'name=size,type=CephString,req=false ',
            'desc': "Create a CephFS volume",
            'perm': 'rw'
        },
        {
            'cmd': 'fs volume rm '
                   'name=vol_name,type=CephString',
            'desc': "Delete a CephFS volume",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolume create '
                   'name=vol_name,type=CephString '
                   'name=sub_name,type=CephString '
                   'name=size,type=CephString,req=false ',
            'desc': "Create a CephFS subvolume within an existing volume",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolume rm '
                   'name=vol_name,type=CephString '
                   'name=sub_name,type=CephString',
            'desc': "Delete a CephFS subvolume",
            'perm': 'rw'
        },

        # volume ls [recursive]
        # subvolume ls <volume>
        # volume authorize/deauthorize
        # subvolume authorize/deauthorize

        # volume describe (free space, etc)
        # volume auth list (vc.get_authorized_ids)

        # snapshots?

        # FIXME: we're doing CephFSVolumeClient.recover on every
        # path where we instantiate and connect a client.  Perhaps
        # keep clients alive longer, or just pass a "don't recover"
        # flag in if it's the >1st time we connected a particular
        # volume in the lifetime of this module instance.
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self._initialized = Event()

        self._background_jobs = Queue.Queue()
        self.vindex = VolumeIndex(self)

    def serve(self):
        # TODO: discover any subvolumes pending purge, and enqueue
        # them in background_jobs at startup

        # TODO: consume background_jobs
        #   skip purge jobs if their fscid no longer exists

        # TODO: on volume delete, cancel out any background jobs that
        # affect subvolumes within that volume.

        # ... any background init needed?  Can get rid of this
        # and _initialized if not
        self._initialized.set()

    def handle_command(self, inbuf, cmd):
        self._initialized.wait()

        handler_name = "_cmd_" + cmd['prefix'].replace(" ", "_")
        try:
            handler = getattr(self, handler_name)
        except AttributeError:
            return -errno.EINVAL, "", "Unknown command"

        return handler(inbuf, cmd)

    def _cmd_fs_volume_create(self, inbuf, cmd):
        vol_id = cmd['name']
        # TODO: validate name against any rules for pool/fs names
        # (...are there any?)

        size = cmd.get('size', None)

        self.vindex.create(vol_id, size=size)

        # Create an MDS cluster
        # =====================
        spec = orchestrator.StatelessServiceSpec()
        spec.name = vol_id
        try:
            completion = self.add_stateless_service("mds", spec)
            self._orchestrator_wait([completion])
            orchestrator.raise_if_exception(completion)
        except (ImportError, orchestrator.OrchestratorError):
            return 0, "", "Volume created successfully (no MDS daemons created)"
        except Exception as e:
            # Don't let detailed orchestrator exceptions (python backtraces)
            # bubble out to the user
            self.log.exception("Failed to create MDS daemons")
            return -errno.EINVAL, "", str(e)

        return 0, "", ""

    def _volume_get_fs(self, vol_name):
        fs_map = self.get('fs_map')
        for fs in fs_map['filesystems']:
            if fs['mdsmap']['fs_name'] == vol_name:
                return fs

        # Fall through
        return None

    def _volume_exists(self, vol_name):
        return self._volume_get_fs(vol_name) is not None

    def _cmd_fs_subvolume_create(self, inbuf, cmd):
        vol_name = cmd['vol_name']
        sub_name = cmd['sub_name']

        size = cmd.get('size', None)

        if not self._volume_exists(vol_name):
            return -errno.ENOENT, "", \
                   "Volume not found, create it with `ceph volume create` " \
                   "before trying to create subvolumes"

        # TODO: validate that subvol size fits in volume size

        with CephFSVolumeClient(rados=self.rados, fs_name=vol_name) as vc:
            # TODO: support real subvolume groups rather than just
            # always having them 1:1 with subvolumes.
            vp = VolumePath(sub_name, sub_name)

            vc.create_volume(vp, size)

        return 0, "", ""

    def _cmd_fs_subvolume_rm(self, inbuf, cmd):
        vol_name = cmd['vol_name']
        sub_name = cmd['sub_name']

        fs = self._volume_get_fs(vol_name)
        if fs is None:
            return 0, "", "Volume '{0}' already deleted".format(vol_name)

        vol_fscid = fs['id']

        with CephFSVolumeClient(rados=self.rados, fs_name=vol_name) as vc:
            # TODO: support real subvolume groups rather than just
            # always having them 1:1 with subvolumes.
            vp = VolumePath(sub_name, sub_name)

            vc.delete_volume(vp)

        # TODO: create a progress event
        self._background_jobs.put(PurgeJob(vol_fscid, vp))

        return 0, "", ""

    def _cmd_fs_volume_rm(self, inbuf, cmd):
        vol_name = cmd['vol_name']

        # Tear down MDS daemons
        # =====================
        try:
            completion = self.remove_stateless_service("mds", vol_name)
            self._orchestrator_wait([completion])
            orchestrator.raise_if_exception(completion)
        except (ImportError, orchestrator.OrchestratorError):
            self.log.warning("OrchestratorError, not tearing down MDS daemons")
        except Exception as e:
            # Don't let detailed orchestrator exceptions (python backtraces)
            # bubble out to the user
            self.log.exception("Failed to tear down MDS daemons")
            return -errno.EINVAL, "", str(e)

        return self.vindex.rm(vol_name)

    def _cmd_fs_volume_ls(self, inbuf, cmd):
        return self.vindex.ls()
