import cephfs

class VolumeName(object):
    VOLUME_PREFIX = "vol:"
    VOLUME_POOL_PREFIX = "cephfs-vol."

    def __init__(self, name):
        self.name = name
        self.fs_name = self.get_fsname(name)
        # XXX can mgr persist auth db?

    @classmethod
    def is_volume(cls, fs_name):
        return fs_name.startswith(cls.VOLUME_PREFIX)

    @classmethod
    def get_fsname(cls, name):
        return cls.VOLUME_PREFIX + name

    @classmethod
    def get_name(cls, fs_name):
        if fs_name.startswith(cls.VOLUME_PREFIX):
            return fs_name[len(cls.VOLUME_PREFIX):]
        return None

    def get_metadata_pool_name(self):
        return self.VOLUME_POOL_PREFIX + self.name + ".meta"

    def get_data_pool_name(self):
        return self.VOLUME_POOL_PREFIX + self.name + ".data"

class Volume():
    def __init__(vol_name, vmgr, size=None):
        self.name = vol_name
        self.fs_name = vol_name.get_fsname()
        # self.id = fscid
        self.vmgr = vmgr
        # TODO: apply quotas to the filesystem root
        # v.init_subvolumes()
        # options:
        # - pinning; max number of pins
        # - standby-replay
        # - quota (default)

        # default sub-volume

    def log(self, *args, **kwargs):
        self.vmgr.log(*args, **kwargs)

    def connect(self, premount_evict = None):
        """

        :param premount_evict: Optional auth_id to evict before mounting the filesystem: callers
                               may want to use this to specify their own auth ID if they expect
                               to be a unique instance and don't want to wait for caps to time
                               out after failure of another instance of themselves.
        """

        self.log.debug("Connecting to cephfs...")
        self.fs = cephfs.LibCephFS(rados_inst=self.vmgr.rados)
        self.log.debug("CephFS initializing...")
        self.fs.init()
        if premount_evict is not None:
            self.log.debug("Premount eviction of {0} starting".format(premount_evict))
            # self.evict(premount_evict)
            self.log.debug("Premount eviction of {0} completes".format(premount_evict))
        self.log.debug("CephFS mounting...")
        self.fs.mount(filesystem_name=self.fs_name)
        self.log.debug("Connection to cephfs complete")

        # Recover from partial auth updates due to a previous
        # crash.
        # self.recover()

#    def get_mon_addrs(self):
#        self.log.info("get_mon_addrs")
#        result = []
#        mon_map = self._rados_command("mon dump")
#        for mon in mon_map['mons']:
#            ip_port = mon['addr'].split("/")[0]
#            result.append(ip_port)
#
#        return result
#
    def disconnect(self):
        self.log.info("disconnect")
        if self.fs:
            self.log.debug("Disconnecting cephfs...")
            self.fs.shutdown()
            self.fs = None
            self.log.debug("Disconnecting cephfs complete")

#    def evict(self, auth_id, timeout, volume_path):
#        """
#        Evict all clients based on the authorization ID and optionally based on
#        the volume path mounted.  Assumes that the authorization key has been
#        revoked prior to calling this function.
#
#        This operation can throw an exception if the mon cluster is unresponsive, or
#        any individual MDS daemon is unresponsive for longer than the timeout passed in.
#        """
#
#        client_spec = ["auth_name={0}".format(auth_id)]
#        if volume_path:
#            client_spec.append("client_metadata.root={0}".
#                               format(self._get_path(volume_path)))
#
#        log.info("evict clients with {0}".format(', '.join(client_spec)))
#
#        mds_map = self.get_mds_map()
#        assert 0 in mds_map['in']
#
#        # It is sufficient to talk to rank 0 for this volume to evict the
#        # client, the MDS will send an osd blacklist command that will cause
#        # the other ranks to also kill the client's session.
#        # XXX Do we need this if it's not done in parallel?
#

class VolumeIndex(object):
    def __init__(self, vmgr):
        self.volumes = {}
        self.vmgr = vmgr

    def _get_volume(self, name):
        v = self.volumes.get(name)
        if v:
            return v
        else:
            fs_map = self.vmgr.get('fs_map')
            for fs in fs_map['filesystems']:
                fs_name = fs['mdsmap']['fs_name']
                name = VolumeName.get_name(fs_name)
                if name:
                    vol_name = VolumeName(name)
                    v = Volume(vol_name, self.vmgr)
                    self.volumes[name] = v
                    return v
            return None

    def _get_volumes(self):
        fs_map = self.vmgr.get('fs_map')
        for fs in fs_map['filesystems']:
            fs_name = fs['mdsmap']['fs_name']
            name = VolumeName.get_name(fs_name)
            if name:
                vol_name = VolumeName(name)
                yield Volume(vol_name, self.vmgr)

    def create(self, name, size=None):
        vol_name = VolumeName(name)

        metadata_pool_name = vol_name.get_metadata_pool_name()
        r, outb, outs = self.vmgr.mon_command({
            'prefix': 'osd pool create',
            'pool': metadata_pool_name,
            'pg_num': 16,
            'pg_num_min': 16,
        })
        if r != 0:
            return r, outb, outs

        # count fs metadata omap at 4x usual rate
        r, outb, outs = self.vmgr.mon_command({
            'prefix': 'osd pool set',
            'pool': metadata_pool_name,
            'var': "pg_autoscale_bias",
            'val': "4.0",
        })
        if r != 0:
            return r, outb, outs

        data_pool_name = vol_name.get_data_pool_name()
        r, outb, outs = self.vmgr.mon_command({
            'prefix': 'osd pool create',
            'pool': data_pool_name,
            'pg_num': 8
        })
        if r != 0:
            # TODO handle EEXIST
            return r, outb, outs

        r, outb, outs = self.vmgr.mon_command({
            'prefix': 'fs new',
            'fs_name': vol_name.fs_name,
            'metadata': metadata_pool_name,
            'data': data_pool_name,
        })
        if r != 0:
            self.vmgr.log.error("Filesystem creation error: {0} {1} {2}".format(
                r, outb, outs
            ))
            return r, outb, outs

            # TODO handle EEXIST

        v = Volume(vol_name, self.vmgr, size=size)
        self.volumes[name] = v

        return 0, "", ""

    def set_conf(self):
        # e.g. standby-replay; ???
        # standby-replay should be a setting on a FS
        # set default sub-volume data pool?
        pass

    def describe(self, name):
        pass

    def ls(self):
        result = []
        for volume in self._get_volumes():
            result.append({'name': volume.name})

        return 0, json.dumps(result, indent=2), ""

    def importfs(self, fs_name):
		# rename fs?
        pass

    def rm(self, name):
        vol_name = VolumeName(name)

        volume = self.get_volume(vol_name)
        if volume is None:
            self.log.warning("Filesystem already gone for volume '{0}'".format(name))
        else:
            cmd = {
                'prefix': 'fs fail',
                'fs_name': vol_name.fs_name,
            }
            r, out, err = self.vmgr.mon_command(cmd)
            if r != 0:
                pass
            cmd = {
                'prefix': 'fs rm',
                'fs_name': vol_name.fs_name,
                'yes_i_really_mean_it': True,
            }
            r, out, err = self.vmgr.mon_command(cmd)
            if r != 0:
                pass

        # Delete pools
        # ============
        metadata_pool_name = vol_name.get_metadata_pool_name()
        data_pool_name = vol_name.get_data_pool_name()

        r, out, err = self.vmgr.mon_command({
            'prefix': 'osd pool rm',
            'pool': metadata_pool_name,
            'pool2': metadata_pool_name,
            'yes_i_really_really_mean_it': True,
        })
        if r != 0:
            return r, out, err

        r, out, err = self.vmgr.mon_command({
            'prefix': 'osd pool rm',
            'pool': data_pool_name,
            'pool2': data_pool_name,
            'yes_i_really_really_mean_it': True,
        })
        if r != 0:
            return r, out, err

        return 0, "", ""
