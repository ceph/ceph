from threading import Event
import errno
import json
import Queue

from mgr_module import MgrModule
import orchestrator

from ceph_volume_client import CephFSVolumeClient, VolumePath

METADATA_POOL_NAME = "cephfs_%s_meta"
DATA_POOL_NAME = "cephfs_%s_data"


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


class Module(MgrModule):
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
            'desc': "Delete a CephFS volume",
            'perm': 'r'
        },
        {
            'cmd': 'fs volume delete '
                   'name=vol_name,type=CephString',
            'desc': "Delete a CephFS volume",
            'perm': 'r'
        },
        {
            'cmd': 'fs subvolume create '
                   'name=vol_name,type=CephString '
                   'name=sub_name,type=CephString '
                   'name=size,type=CephString,req=false ',
            'desc': "Create a CephFS subvolume within an existing volume",
            'perm': 'r'
        },
        {
            'cmd': 'fs subvolume delete '
                   'name=vol_name,type=CephString '
                   'name=sub_name,type=CephString',
            'desc': "Delete a CephFS subvolume",
            'perm': 'r'
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

    def _oremote(self, *args, **kwargs):
        """
        Helper for invoking `remote` on whichever orchestrator is enabled
        """
        # TODO: orchestrator_cli has a setting for orchestrator backend,
        # but we don't really want to start propagating that into
        # other modules individually -- maybe mgrmap should have
        # the setting?  or just a C++-level mgr config option?

        return self.remote("rook",
                           *args, **kwargs)

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

    def _orchestrator_wait(self, completions):
        """
        Helper to wait for completions to complete (reads) or
        become persistent (writes).

        Waits for writes to be *persistent* but not *effective*.
        """
        done = False

        while done is False:
            done = self._oremote("wait", completions)

            if not done:
                any_nonpersistent = False
                for c in completions:
                    if c.is_read:
                        if not c.is_complete:
                            any_nonpersistent = True
                            break
                    else:
                        if not c.is_persistent:
                            any_nonpersistent = True
                            break

                if any_nonpersistent:
                    time.sleep(5)
                else:
                    done = True

    def _poolset_name(self, volume_name):
        """
        Convention for naming poolsets for volumes

        :return: string
        """
        return "cephfs.{0}".format(volume_name)

    def _fallback_pool_names(self, poolset_name):
        return poolset_name + ".meta", poolset_name + ".data"

    def _cmd_fs_volume_create(self, inbuf, cmd):
        vol_id = cmd['name']
        # TODO: validate name against any rules for pool/fs names
        # (...are there any?)

        size = cmd.get('size', None)

        ps_name = self._poolset_name(vol_id)

        # Create a poolset
        # ================
        try:
            ps_r, ps_out, ps_err = self.remote(
                "poolsets",
                "handle_command",
                inbuf="",
                cmd={
                    'prefix': 'poolset create',
                    'app': 'cephfs',
                    'psname': ps_name,
                    'size': "0" if size is None else size
                })

            if ps_r != 0:
                self.log.error("Poolset error: {0} {1} {2}".format(
                    ps_r, ps_out, ps_err
                ))
                return ps_r, ps_out, ps_err

            # FIXME: make poolsets return the names, instead of using
            # magic knowledge of the pattern
            mdp_name = ps_name + ".meta"
            dp_name = ps_name + ".data"
        except ImportError:
            # Fallback for when poolsets is unavailable (e.g. Mimic/Luminous
            # backport)
            self.log.warning("poolsets module unavailable, using fallback "
                             "pool creation")

            mdp_name, dp_name = self._fallback_pool_names(ps_name)

            r, outb, outs = self.mon_command({
                'prefix': 'osd pool create',
                'pool': mdp_name,
                'pg_num': 8
            })
            if r != 0:
                return r, outb, outs

            r, outb, outs = self.mon_command({
                'prefix': 'osd pool create',
                'pool': dp_name,
                'pg_num': 8
            })
            if r != 0:
                return r, outb, outs

        # Create a filesystem
        # ====================
        r, outb, outs = self.mon_command({
            'prefix': 'fs new',
            'fs_name': vol_id,
            'metadata': mdp_name,
            'data': dp_name
        })

        if r != 0:
            self.log.error("Filesystem creation error: {0} {1} {2}".format(
                r, outb, outs
            ))
            return r, outb, outs

        # TODO: apply quotas to the filesystem root

        # Create an MDS cluster
        # =====================
        spec = orchestrator.StatelessServiceSpec()
        spec.name = vol_id
        try:
            completion = self._oremote(
                "add_stateless_service",
                "mds",
                spec
            )
            self._wait([completion])
        except ImportError:
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

    def _volume_get_mds_daemon_names(self, vol_name):
        fs = self._volume_get_fs(vol_name)
        if fs is None:
            return []

        return [i['name'] for i in fs['mdsmap']['info'].values()]

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

        with CephFSVolumeClient(rados=self.rados) as vc:
            # TODO: support real subvolume groups rather than just
            # always having them 1:1 with subvolumes.
            vp = VolumePath(sub_name, sub_name)

            vc.create_volume(vp, size)

        return 0, "", ""

    def _cmd_fs_subvolume_delete(self, inbuf, cmd):
        vol_name = cmd['vol_name']
        sub_name = cmd['sub_name']

        fs = self._volume_get_fs(vol_name)
        if fs is None:
            return 0, "", "Volume '{0}' already deleted".forma(vol_name)

        vol_fscid = fs['id']

        with CephFSVolumeClient(rados=self.rados) as vc:
            # TODO: support real subvolume groups rather than just
            # always having them 1:1 with subvolumes.
            vp = VolumePath(sub_name, sub_name)

            vc.delete_volume(vp)

        # TODO: create a progress event
        self._background_jobs.put(PurgeJob(vol_fscid, vp))

        return 0, "", ""

    def _cmd_fs_volume_delete(self, inbuf, cmd):
        vol_name = cmd['vol_name']

        # Tear down MDS daemons
        # =====================
        spec = orchestrator.StatelessServiceSpec()
        spec.name = vol_name
        try:
            completion = self._oremote(
                "rm_stateless_service",
                "mds",
                spec
            )
            self._wait([completion])
        except ImportError:
            self.log.warning("No orchestrator, not tearing down MDS daemons")
        except Exception as e:
            # Don't let detailed orchestrator exceptions (python backtraces)
            # bubble out to the user
            self.log.exception("Failed to tear down MDS daemons")
            return -errno.EINVAL, "", str(e)

        if self._volume_exists(vol_name):
            # In case orchestrator didn't tear down MDS daemons cleanly, or
            # there was no orchestrator, we force the daemons down.
            r, out, err = self.mon_command({
                'prefix': 'fs set',
                'fs_name': vol_name,
                'var': 'cluster_down',
                'val': 'true'
            })
            if r != 0:
                return r, out, err

            for mds_name in self._volume_get_mds_daemon_names(vol_name):
                r, out, err = self.mon_command({
                    'prefix': 'mds fail',
                    'role_or_gid': mds_name})
                if r != 0:
                    return r, out, err

            # Delete CephFS filesystem
            # =========================
            r, out, err = self.mon_command({
                'prefix': 'fs rm',
                'fs_name': vol_name,
                'sure': "--yes-i-really-mean-it"
            })
            if r != 0:
                return r, out, err
        else:
            self.log.warning("Filesystem already gone for volume '{0}'".format(
                vol_name
            ))

        # Delete poolset
        # ==============
        ps_name = self._poolset_name(vol_name)
        try:
            ps_name = self._poolset_name(vol_name)
            ps_r, ps_out, ps_err = self.remote(
                "poolsets",
                "handle_command",
                inbuf="",
                cmd={
                    'prefix': 'poolset delete',
                    'psname': ps_name
                })

            if ps_r != 0:
                self.log.error("Poolset error: {0} {1} {2}".format(
                    ps_r, ps_out, ps_err
                ))
                return ps_r, ps_out, ps_err
        except ImportError:
            # Fallback for when poolsets is unavailable (e.g. Mimic/Luminous
            # backport)
            mdp_name, dp_name = self._fallback_pool_names(ps_name)

            r, out, err = self.mon_command({
                'prefix': 'osd pool rm',
                'pool': mdp_name,
                'pool2': mdp_name,
                'sure': '--yes-i-really-really-mean-it'
            })
            if r != 0:
                return r, out, err

            r, out, err = self.mon_command({
                'prefix': 'osd pool rm',
                'pool': dp_name,
                'pool2': dp_name,
                'sure': '--yes-i-really-really-mean-it'
            })
            if r != 0:
                return r, out, err

        return 0, "", ""

    def _cmd_fs_volume_ls(self, inbuf, cmd):
        fs_map = self.get("fs_map")

        result = []

        for f in fs_map['filesystems']:
            result.append({
                'name': f['mdsmap']['fs_name']
            })

        return 0, json.dumps(result, indent=2), ""
