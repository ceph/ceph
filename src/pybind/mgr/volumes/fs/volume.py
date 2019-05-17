import json
import errno
import logging

import cephfs
import orchestrator

from .subvolspec import SubvolumeSpec
from .subvolume import SubVolume

log = logging.getLogger(__name__)

class VolumeClient(object):
    def __init__(self, mgr):
        self.mgr = mgr

    def gen_pool_names(self, volname):
        """
        return metadata and data pool name (from a filesystem/volume name) as a tuple
        """
        return "cephfs.{}.meta".format(volname), "cephfs.{}.data".format(volname)

    def get_fs(self, fs_name):
        fs_map = self.mgr.get('fs_map')
        for fs in fs_map['filesystems']:
            if fs['mdsmap']['fs_name'] == fs_name:
                return fs
        return None

    def get_mds_names(self, fs_name):
        fs = self.get_fs(fs_name)
        if fs is None:
            return []
        return [mds['name'] for mds in fs['mdsmap']['info'].values()]

    def volume_exists(self, volname):
        return self.get_fs(volname) is not None

    def create_pool(self, pool_name, pg_num, pg_num_min=None, pg_autoscale_factor=None):
        # create the given pool
        command = {'prefix': 'osd pool create', 'pool': pool_name, 'pg_num': pg_num}
        if pg_num_min:
            command['pg_num_min'] = pg_num_min
        r, outb, outs = self.mgr.mon_command(command)
        if r != 0:
            return r, outb, outs

        # set pg autoscale if needed
        if pg_autoscale_factor:
            command = {'prefix': 'osd pool set', 'pool': pool_name, 'var': 'pg_autoscale_bias',
                       'val': str(pg_autoscale_factor)}
            r, outb, outs = self.mgr.mon_command(command)
        return r, outb, outs

    def remove_pool(self, pool_name):
        command = {'prefix': 'osd pool rm', 'pool': pool_name, 'pool2': pool_name,
                   'yes_i_really_really_mean_it': True}
        return self.mgr.mon_command(command)

    def create_filesystem(self, fs_name, metadata_pool, data_pool):
        command = {'prefix': 'fs new', 'fs_name': fs_name, 'metadata': metadata_pool,
                   'data': data_pool}
        return self.mgr.mon_command(command)

    def remove_filesystem(self, fs_name):
        command = {'prefix': 'fs rm', 'fs_name': fs_name, 'yes_i_really_mean_it': True}
        return self.mgr.mon_command(command)

    def create_mds(self, fs_name):
        spec = orchestrator.StatelessServiceSpec()
        spec.name = fs_name
        try:
            completion = self.mgr.add_stateless_service("mds", spec)
            self.mgr._orchestrator_wait([completion])
            orchestrator.raise_if_exception(completion)
        except (ImportError, orchestrator.OrchestratorError):
            return 0, "", "Volume created successfully (no MDS daemons created)"
        except Exception as e:
            # Don't let detailed orchestrator exceptions (python backtraces)
            # bubble out to the user
            log.exception("Failed to create MDS daemons")
            return -errno.EINVAL, "", str(e)
        return 0, "", ""

    def set_mds_down(self, fs_name):
        command = {'prefix': 'fs set', 'fs_name': fs_name, 'var': 'cluster_down', 'val': 'true'}
        r, outb, outs = self.mgr.mon_command(command)
        if r != 0:
            return r, outb, outs
        for mds in self.get_mds_names(fs_name):
            command = {'prefix': 'mds fail', 'role_or_gid': mds}
            r, outb, outs = self.mgr.mon_command(command)
            if r != 0:
                return r, outb, outs
        return 0, "", ""

    ### volume operations -- create, rm, ls

    def create_volume(self, volname, size=None):
        """
        create volume  (pool, filesystem and mds)
        """
        metadata_pool, data_pool = self.gen_pool_names(volname)
        # create pools
        r, outs, outb = self.create_pool(metadata_pool, 16, pg_num_min=16, pg_autoscale_factor=4.0)
        if r != 0:
            return r, outb, outs
        r, outb, outs = self.create_pool(data_pool, 8)
        if r != 0:
            return r, outb, outs
        # create filesystem
        r, outb, outs = self.create_filesystem(volname, metadata_pool, data_pool)
        if r != 0:
            log.error("Filesystem creation error: {0} {1} {2}".format(r, outb, outs))
            return r, outb, outs
        # create mds
        return self.create_mds(volname)

    def delete_volume(self, volname):
        """
        delete the given module (tear down mds, remove filesystem)
        """
        # Tear down MDS daemons
        try:
            completion = self.mgr.remove_stateless_service("mds", volname)
            self.mgr._orchestrator_wait([completion])
            orchestrator.raise_if_exception(completion)
        except (ImportError, orchestrator.OrchestratorError):
            log.warning("OrchestratorError, not tearing down MDS daemons")
        except Exception as e:
            # Don't let detailed orchestrator exceptions (python backtraces)
            # bubble out to the user
            log.exception("Failed to tear down MDS daemons")
            return -errno.EINVAL, "", str(e)

        # In case orchestrator didn't tear down MDS daemons cleanly, or
        # there was no orchestrator, we force the daemons down.
        if self.volume_exists(volname):
            r, outb, outs = self.set_mds_down(volname)
            if r != 0:
                return r, outb, outs
            r, outb, outs = self.remove_filesystem(volname)
            if r != 0:
                return r, outb, outs
        else:
            log.warning("Filesystem already gone for volume '{0}'".format(volname))
        metadata_pool, data_pool = self.gen_pool_names(volname)
        r, outb, outs = self.remove_pool(metadata_pool)
        if r != 0:
            return r, outb, outs
        return self.remove_pool(data_pool)

    def list_volumes(self):
        result = []
        fs_map = self.mgr.get("fs_map")
        for f in fs_map['filesystems']:
            result.append({'name': f['mdsmap']['fs_name']})
        return 0, json.dumps(result, indent=2), ""

    def group_exists(self, sv, spec):
        # default group need not be explicitly created (as it gets created
        # at the time of subvolume, snapshot and other create operations).
        return spec.is_default_group() or sv.get_group_path(spec)

    ### subvolume operations

    def create_subvolume(self, volname, subvolname, groupname, size):
        if not self.volume_exists(volname):
            return -errno.ENOENT, "", \
                "Volume '{0}' not found, create it with `ceph fs volume create` " \
                "before trying to create subvolumes".format(volname)

        # TODO: validate that subvol size fits in volume size
        with SubVolume(self.mgr, fs_name=volname) as sv:
            spec = SubvolumeSpec(subvolname, groupname)
            if not self.group_exists(sv, spec):
                return -errno.ENOENT, "", \
                    "Subvolume group '{0}' not found, create it with `ceph fs subvolumegroup create` " \
                    "before creating subvolumes".format(groupname)
            return sv.create_subvolume(spec, size)

    def remove_subvolume(self, volname, subvolname, groupname, force):
        fs = self.get_fs(volname)
        if not fs:
            if force:
                return 0, "", ""
            else:
                return -errno.ENOENT, "", \
                    "Volume '{0}' not found, cannot remove subvolume '{1}'".format(volname, subvolname)

        with SubVolume(self.mgr, fs_name=volname) as sv:
            spec = SubvolumeSpec(subvolname, groupname)
            if not self.group_exists(sv, spec):
                if force:
                    return 0, "", ""
                else:
                    return -errno.ENOENT, "", \
                        "Subvolume group '{0}' not found, cannot remove subvolume '{1}'".format(groupname, subvolname)
            try:
                sv.remove_subvolume(spec)
            except cephfs.ObjectNotFound:
                if force:
                    return 0, "", ""
                else:
                    return -errno.ENOENT, "", \
                        "Subvolume '{0}' not found, cannot remove it".format(subvolname)
            sv.purge_subvolume(spec)
            return 0, "", ""

    def subvolume_getpath(self, volname, subvolname, groupname):
        if not self.volume_exists(volname):
            return -errno.ENOENT, "", "Volume '{0}' not found".format(volname)

        with SubVolume(self.mgr, fs_name=volname) as sv:
            spec = SubvolumeSpec(subvolname, groupname)
            if not self.group_exists(sv, spec):
                return -errno.ENOENT, "", "Subvolume group '{0}' not found".format(groupname)
            path = sv.get_subvolume_path(spec)
            if not path:
                return -errno.ENOENT, "", "Subvolume '{0}' not found".format(groupname)
            return 0, path, ""

    ### subvolume snapshot

    def create_subvolume_snapshot(self, volname, subvolname, snapname, groupname):
        if not self.volume_exists(volname):
            return -errno.ENOENT, "", \
                   "Volume '{0}' not found, cannot create snapshot '{1}'".format(volname, snapname)

        with SubVolume(self.mgr, fs_name=volname) as sv:
            spec = SubvolumeSpec(subvolname, groupname)
            if not self.group_exists(sv, spec):
                return -errno.ENOENT, "", \
                    "Subvolume group '{0}' not found, cannot create snapshot '{1}'".format(groupname, snapname)
            if not sv.get_subvolume_path(spec):
                return -errno.ENOENT, "", \
                       "Subvolume '{0}' not found, cannot create snapshot '{1}'".format(subvolname, snapname)
            sv.create_subvolume_snapshot(spec, snapname)
        return 0, "", ""

    def remove_subvolume_snapshot(self, volname, subvolname, snapname, groupname, force):
        if not self.volume_exists(volname):
            if force:
                return 0, "", ""
            else:
                return -errno.ENOENT, "", \
                    "Volume '{0}' not found, cannot remove subvolumegroup snapshot '{1}'".format(volname, snapname)

        with SubVolume(self.mgr, fs_name=volname) as sv:
            spec = SubvolumeSpec(subvolname, groupname)
            if not self.group_exists(sv, spec):
                if force:
                    return 0, "", ""
                else:
                    return -errno.ENOENT, "", \
                        "Subvolume group '{0}' already removed, cannot remove subvolume snapshot '{1}'".format(groupname, snapname)
            if not sv.get_subvolume_path(spec):
                if force:
                    return 0, "", ""
                else:
                    return -errno.ENOENT, "", \
                        "Subvolume '{0}' not found, cannot remove subvolume snapshot '{1}'".format(subvolname, snapname)
            try:
                sv.remove_subvolume_snapshot(spec, snapname)
            except cephfs.ObjectNotFound:
                if force:
                    return 0, "", ""
                else:
                    return -errno.ENOENT, "", \
                        "Subvolume snapshot '{0}' not found, cannot remove it".format(snapname)
        return 0, "", ""

    ### group operations

    def create_subvolume_group(self, volname, groupname):
        if not self.volume_exists(volname):
            return -errno.ENOENT, "", \
                   "Volume '{0}' not found, create it with `ceph fs volume create` " \
                   "before trying to create subvolume groups".format(volname)

        # TODO: validate that subvol size fits in volume size
        with SubVolume(self.mgr, fs_name=volname) as sv:
            spec = SubvolumeSpec("", groupname)
            sv.create_group(spec)
        return 0, "", ""

    def remove_subvolume_group(self, volname, groupname, force):
        if not self.volume_exists(volname):
            if force:
                return 0, "", ""
            else:
                return -errno.ENOENT, "", \
                    "Volume '{0}' not found, cannot remove subvolume group '{0}'".format(volname, groupname)

        with SubVolume(self.mgr, fs_name=volname) as sv:
            # TODO: check whether there are no subvolumes in the group
            spec = SubvolumeSpec("", groupname)
            try:
                sv.remove_group(spec)
            except cephfs.ObjectNotFound:
                if force:
                    return 0, "", ""
                else:
                    return -errno.ENOENT, "", \
                        "Subvolume group '{0}' not found".format(groupname)
        return 0, "", ""

    ### group snapshot

    def create_subvolume_group_snapshot(self, volname, groupname, snapname):
        if not self.volume_exists(volname):
            return -errno.ENOENT, "", \
                   "Volume '{0}' not found, cannot create snapshot '{1}'".format(volname, snapname)

        with SubVolume(self.mgr, fs_name=volname) as sv:
            spec = SubvolumeSpec("", groupname)
            if not self.group_exists(sv, spec):
                return -errno.ENOENT, "", \
                    "Subvolume group '{0}' not found, cannot create snapshot '{1}'".format(groupname, snapname)
            sv.create_group_snapshot(spec, snapname)
        return 0, "", ""

    def remove_subvolume_group_snapshot(self, volname, groupname, snapname, force):
        if not self.volume_exists(volname):
            if force:
                return 0, "", ""
            else:
                return -errno.ENOENT, "", \
                    "Volume '{0}' not found, cannot remove subvolumegroup snapshot '{1}'".format(volname, snapname)

        with SubVolume(self.mgr, fs_name=volname) as sv:
            spec = SubvolumeSpec("", groupname)
            if not self.group_exists(sv, spec):
                if force:
                    return 0, "", ""
                else:
                    return -errno.ENOENT, "", \
                        "Subvolume group '{0}' not found, cannot remove it".format(groupname)
            try:
                sv.remove_group_snapshot(spec, snapname)
            except:
                if force:
                    return 0, "", ""
                else:
                    return -errno.ENOENT, "", \
                        "Subvolume group snapshot '{0}' not found, cannot remove it".format(snapname)
        return 0, "", ""
