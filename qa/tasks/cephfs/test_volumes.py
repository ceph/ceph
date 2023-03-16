import os
import json
import time
import errno
import random
import logging
import collections
import uuid
import unittest
from hashlib import md5
from textwrap import dedent
from io import StringIO

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.fuse_mount import FuseMount
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)

class TestVolumesHelper(CephFSTestCase):
    """Helper class for testing FS volume, subvolume group and subvolume operations."""
    TEST_VOLUME_PREFIX = "volume"
    TEST_SUBVOLUME_PREFIX="subvolume"
    TEST_GROUP_PREFIX="group"
    TEST_SNAPSHOT_PREFIX="snapshot"
    TEST_CLONE_PREFIX="clone"
    TEST_FILE_NAME_PREFIX="subvolume_file"

    # for filling subvolume with data
    CLIENTS_REQUIRED = 2
    MDSS_REQUIRED = 2

    # io defaults
    DEFAULT_FILE_SIZE = 1 # MB
    DEFAULT_NUMBER_OF_FILES = 1024

    def _fs_cmd(self, *args):
        return self.mgr_cluster.mon_manager.raw_cluster_cmd("fs", *args)

    def _raw_cmd(self, *args):
        return self.mgr_cluster.mon_manager.raw_cluster_cmd(*args)

    def __check_clone_state(self, state, clone, clone_group=None, timo=120):
        check = 0
        args = ["clone", "status", self.volname, clone]
        if clone_group:
            args.append(clone_group)
        args = tuple(args)
        while check < timo:
            result = json.loads(self._fs_cmd(*args))
            if result["status"]["state"] == state:
                break
            check += 1
            time.sleep(1)
        self.assertTrue(check < timo)

    def _get_clone_status(self, clone, clone_group=None):
        args = ["clone", "status", self.volname, clone]
        if clone_group:
            args.append(clone_group)
        args = tuple(args)
        result = json.loads(self._fs_cmd(*args))
        return result

    def _wait_for_clone_to_complete(self, clone, clone_group=None, timo=120):
        self.__check_clone_state("complete", clone, clone_group, timo)

    def _wait_for_clone_to_fail(self, clone, clone_group=None, timo=120):
        self.__check_clone_state("failed", clone, clone_group, timo)

    def _wait_for_clone_to_be_in_progress(self, clone, clone_group=None, timo=120):
        self.__check_clone_state("in-progress", clone, clone_group, timo)

    def _check_clone_canceled(self, clone, clone_group=None):
        self.__check_clone_state("canceled", clone, clone_group, timo=1)

    def _get_subvolume_snapshot_path(self, subvolume, snapshot, source_group, subvol_path, source_version):
        if source_version == 2:
            # v2
            if subvol_path is not None:
                (base_path, uuid_str) = os.path.split(subvol_path)
            else:
                (base_path, uuid_str) = os.path.split(self._get_subvolume_path(self.volname, subvolume, group_name=source_group))
            return os.path.join(base_path, ".snap", snapshot, uuid_str)

        # v1
        base_path = self._get_subvolume_path(self.volname, subvolume, group_name=source_group)
        return os.path.join(base_path, ".snap", snapshot)

    def _verify_clone_attrs(self, source_path, clone_path):
        path1 = source_path
        path2 = clone_path

        p = self.mount_a.run_shell(["find", path1])
        paths = p.stdout.getvalue().strip().split()

        # for each entry in source and clone (sink) verify certain inode attributes:
        # inode type, mode, ownership, [am]time.
        for source_path in paths:
            sink_entry = source_path[len(path1)+1:]
            sink_path = os.path.join(path2, sink_entry)

            # mode+type
            sval = int(self.mount_a.run_shell(['stat', '-c' '%f', source_path]).stdout.getvalue().strip(), 16)
            cval = int(self.mount_a.run_shell(['stat', '-c' '%f', sink_path]).stdout.getvalue().strip(), 16)
            self.assertEqual(sval, cval)

            # ownership
            sval = int(self.mount_a.run_shell(['stat', '-c' '%u', source_path]).stdout.getvalue().strip())
            cval = int(self.mount_a.run_shell(['stat', '-c' '%u', sink_path]).stdout.getvalue().strip())
            self.assertEqual(sval, cval)

            sval = int(self.mount_a.run_shell(['stat', '-c' '%g', source_path]).stdout.getvalue().strip())
            cval = int(self.mount_a.run_shell(['stat', '-c' '%g', sink_path]).stdout.getvalue().strip())
            self.assertEqual(sval, cval)

            # inode timestamps
            # do not check access as kclient will generally not update this like ceph-fuse will.
            sval = int(self.mount_a.run_shell(['stat', '-c' '%Y', source_path]).stdout.getvalue().strip())
            cval = int(self.mount_a.run_shell(['stat', '-c' '%Y', sink_path]).stdout.getvalue().strip())
            self.assertEqual(sval, cval)

    def _verify_clone_root(self, source_path, clone_path, clone, clone_group, clone_pool):
        # verifies following clone root attrs quota, data_pool and pool_namespace
        # remaining attributes of clone root are validated in _verify_clone_attrs

        clone_info = json.loads(self._get_subvolume_info(self.volname, clone, clone_group))

        # verify quota is inherited from source snapshot
        src_quota = self.mount_a.getfattr(source_path, "ceph.quota.max_bytes")
        # FIXME: kclient fails to get this quota value: https://tracker.ceph.com/issues/48075
        if isinstance(self.mount_a, FuseMount):
            self.assertEqual(clone_info["bytes_quota"], "infinite" if src_quota is None else int(src_quota))

        if clone_pool:
            # verify pool is set as per request
            self.assertEqual(clone_info["data_pool"], clone_pool)
        else:
            # verify pool and pool namespace are inherited from snapshot
            self.assertEqual(clone_info["data_pool"],
                             self.mount_a.getfattr(source_path, "ceph.dir.layout.pool"))
            self.assertEqual(clone_info["pool_namespace"],
                             self.mount_a.getfattr(source_path, "ceph.dir.layout.pool_namespace"))

    def _verify_clone(self, subvolume, snapshot, clone,
                      source_group=None, clone_group=None, clone_pool=None,
                      subvol_path=None, source_version=2, timo=120):
        # pass in subvol_path (subvolume path when snapshot was taken) when subvolume is removed
        # but snapshots are retained for clone verification
        path1 = self._get_subvolume_snapshot_path(subvolume, snapshot, source_group, subvol_path, source_version)
        path2 = self._get_subvolume_path(self.volname, clone, group_name=clone_group)

        check = 0
        # TODO: currently snapshot rentries are not stable if snapshot source entries
        #       are removed, https://tracker.ceph.com/issues/46747
        while check < timo and subvol_path is None:
            val1 = int(self.mount_a.getfattr(path1, "ceph.dir.rentries"))
            val2 = int(self.mount_a.getfattr(path2, "ceph.dir.rentries"))
            if val1 == val2:
                break
            check += 1
            time.sleep(1)
        self.assertTrue(check < timo)

        self._verify_clone_root(path1, path2, clone, clone_group, clone_pool)
        self._verify_clone_attrs(path1, path2)

    def _generate_random_volume_name(self, count=1):
        n = self.volume_start
        volumes = [f"{TestVolumes.TEST_VOLUME_PREFIX}_{i:016}" for i in range(n, n+count)]
        self.volume_start += count
        return volumes[0] if count == 1 else volumes

    def _generate_random_subvolume_name(self, count=1):
        n = self.subvolume_start
        subvolumes = [f"{TestVolumes.TEST_SUBVOLUME_PREFIX}_{i:016}" for i in range(n, n+count)]
        self.subvolume_start += count
        return subvolumes[0] if count == 1 else subvolumes

    def _generate_random_group_name(self, count=1):
        n = self.group_start
        groups = [f"{TestVolumes.TEST_GROUP_PREFIX}_{i:016}" for i in range(n, n+count)]
        self.group_start += count
        return groups[0] if count == 1 else groups

    def _generate_random_snapshot_name(self, count=1):
        n = self.snapshot_start
        snaps = [f"{TestVolumes.TEST_SNAPSHOT_PREFIX}_{i:016}" for i in range(n, n+count)]
        self.snapshot_start += count
        return snaps[0] if count == 1 else snaps

    def _generate_random_clone_name(self, count=1):
        n = self.clone_start
        clones = [f"{TestVolumes.TEST_CLONE_PREFIX}_{i:016}" for i in range(n, n+count)]
        self.clone_start += count
        return clones[0] if count == 1 else clones

    def _enable_multi_fs(self):
        self._fs_cmd("flag", "set", "enable_multiple", "true", "--yes-i-really-mean-it")

    def _create_or_reuse_test_volume(self):
        result = json.loads(self._fs_cmd("volume", "ls"))
        if len(result) == 0:
            self.vol_created = True
            self.volname = self._generate_random_volume_name()
            self._fs_cmd("volume", "create", self.volname)
        else:
            self.volname = result[0]['name']

    def  _get_volume_info(self, vol_name, human_readable=False):
        if human_readable:
            args = ["volume", "info", vol_name, human_readable]
        else:
            args = ["volume", "info", vol_name]
        args = tuple(args)
        vol_md = self._fs_cmd(*args)
        return vol_md

    def  _get_subvolume_group_path(self, vol_name, group_name):
        args = ("subvolumegroup", "getpath", vol_name, group_name)
        path = self._fs_cmd(*args)
        # remove the leading '/', and trailing whitespaces
        return path[1:].rstrip()

    def  _get_subvolume_group_info(self, vol_name, group_name):
        args = ["subvolumegroup", "info", vol_name, group_name]
        args = tuple(args)
        group_md = self._fs_cmd(*args)
        return group_md

    def  _get_subvolume_path(self, vol_name, subvol_name, group_name=None):
        args = ["subvolume", "getpath", vol_name, subvol_name]
        if group_name:
            args.append(group_name)
        args = tuple(args)
        path = self._fs_cmd(*args)
        # remove the leading '/', and trailing whitespaces
        return path[1:].rstrip()

    def  _get_subvolume_info(self, vol_name, subvol_name, group_name=None):
        args = ["subvolume", "info", vol_name, subvol_name]
        if group_name:
            args.append(group_name)
        args = tuple(args)
        subvol_md = self._fs_cmd(*args)
        return subvol_md

    def _get_subvolume_snapshot_info(self, vol_name, subvol_name, snapname, group_name=None):
        args = ["subvolume", "snapshot", "info", vol_name, subvol_name, snapname]
        if group_name:
            args.append(group_name)
        args = tuple(args)
        snap_md = self._fs_cmd(*args)
        return snap_md

    def _delete_test_volume(self):
        self._fs_cmd("volume", "rm", self.volname, "--yes-i-really-mean-it")

    def _do_subvolume_pool_and_namespace_update(self, subvolume, pool=None, pool_namespace=None, subvolume_group=None):
        subvolpath = self._get_subvolume_path(self.volname, subvolume, group_name=subvolume_group)

        if pool is not None:
            self.mount_a.setfattr(subvolpath, 'ceph.dir.layout.pool', pool, sudo=True)

        if pool_namespace is not None:
            self.mount_a.setfattr(subvolpath, 'ceph.dir.layout.pool_namespace', pool_namespace, sudo=True)

    def _do_subvolume_attr_update(self, subvolume, uid, gid, mode, subvolume_group=None):
        subvolpath = self._get_subvolume_path(self.volname, subvolume, group_name=subvolume_group)

        # mode
        self.mount_a.run_shell(['sudo', 'chmod', mode, subvolpath], omit_sudo=False)

        # ownership
        self.mount_a.run_shell(['sudo', 'chown', uid, subvolpath], omit_sudo=False)
        self.mount_a.run_shell(['sudo', 'chgrp', gid, subvolpath], omit_sudo=False)

    def _do_subvolume_io(self, subvolume, subvolume_group=None, create_dir=None,
                         number_of_files=DEFAULT_NUMBER_OF_FILES, file_size=DEFAULT_FILE_SIZE):
        # get subvolume path for IO
        args = ["subvolume", "getpath", self.volname, subvolume]
        if subvolume_group:
            args.append(subvolume_group)
        args = tuple(args)
        subvolpath = self._fs_cmd(*args)
        self.assertNotEqual(subvolpath, None)
        subvolpath = subvolpath[1:].rstrip() # remove "/" prefix and any trailing newline

        io_path = subvolpath
        if create_dir:
            io_path = os.path.join(subvolpath, create_dir)
            self.mount_a.run_shell_payload(f"mkdir -p {io_path}")

        log.debug("filling subvolume {0} with {1} files each {2}MB size under directory {3}".format(subvolume, number_of_files, file_size, io_path))
        for i in range(number_of_files):
            filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, i)
            self.mount_a.write_n_mb(os.path.join(io_path, filename), file_size)

    def _do_subvolume_io_mixed(self, subvolume, subvolume_group=None):
        subvolpath = self._get_subvolume_path(self.volname, subvolume, group_name=subvolume_group)

        reg_file = "regfile.0"
        dir_path = os.path.join(subvolpath, "dir.0")
        sym_path1 = os.path.join(subvolpath, "sym.0")
        # this symlink's ownership would be changed
        sym_path2 = os.path.join(dir_path, "sym.0")

        self.mount_a.run_shell(["mkdir", dir_path])
        self.mount_a.run_shell(["ln", "-s", "./{}".format(reg_file), sym_path1])
        self.mount_a.run_shell(["ln", "-s", "./{}".format(reg_file), sym_path2])
        # flip ownership to nobody. assumption: nobody's id is 65534
        self.mount_a.run_shell(["sudo", "chown", "-h", "65534:65534", sym_path2], omit_sudo=False)

    def _wait_for_trash_empty(self, timeout=60):
        # XXX: construct the trash dir path (note that there is no mgr
        # [sub]volume interface for this).
        trashdir = os.path.join("./", "volumes", "_deleting")
        self.mount_a.wait_for_dir_empty(trashdir, timeout=timeout)

    def _wait_for_subvol_trash_empty(self, subvol, group="_nogroup", timeout=30):
        trashdir = os.path.join("./", "volumes", group, subvol, ".trash")
        try:
            self.mount_a.wait_for_dir_empty(trashdir, timeout=timeout)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOENT:
                pass
            else:
                raise

    def _assert_meta_location_and_version(self, vol_name, subvol_name, subvol_group=None, version=2, legacy=False):
        if legacy:
            subvol_path = self._get_subvolume_path(vol_name, subvol_name, group_name=subvol_group)
            m = md5()
            m.update(("/"+subvol_path).encode('utf-8'))
            meta_filename = "{0}.meta".format(m.digest().hex())
            metapath = os.path.join(".", "volumes", "_legacy", meta_filename)
        else:
            group = subvol_group if subvol_group is not None else '_nogroup'
            metapath = os.path.join(".", "volumes", group, subvol_name, ".meta")

        out = self.mount_a.run_shell(['sudo', 'cat', metapath], omit_sudo=False)
        lines = out.stdout.getvalue().strip().split('\n')
        sv_version = -1
        for line in lines:
            if line == "version = " + str(version):
                sv_version = version
                break
        self.assertEqual(sv_version, version, "version expected was '{0}' but got '{1}' from meta file at '{2}'".format(
                         version, sv_version, metapath))

    def _create_v1_subvolume(self, subvol_name, subvol_group=None, has_snapshot=True, subvol_type='subvolume', state='complete'):
        group = subvol_group if subvol_group is not None else '_nogroup'
        basepath = os.path.join("volumes", group, subvol_name)
        uuid_str = str(uuid.uuid4())
        createpath = os.path.join(basepath, uuid_str)
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', createpath], omit_sudo=False)

        # create a v1 snapshot, to prevent auto upgrades
        if has_snapshot:
            snappath = os.path.join(createpath, ".snap", "fake")
            self.mount_a.run_shell(['sudo', 'mkdir', '-p', snappath], omit_sudo=False)

        # add required xattrs to subvolume
        default_pool = self.mount_a.getfattr(".", "ceph.dir.layout.pool")
        self.mount_a.setfattr(createpath, 'ceph.dir.layout.pool', default_pool, sudo=True)

        # create a v1 .meta file
        meta_contents = "[GLOBAL]\nversion = 1\ntype = {0}\npath = {1}\nstate = {2}\n".format(subvol_type, "/" + createpath, state)
        if state == 'pending':
            # add a fake clone source
            meta_contents = meta_contents + '[source]\nvolume = fake\nsubvolume = fake\nsnapshot = fake\n'
        meta_filepath1 = os.path.join(self.mount_a.mountpoint, basepath, ".meta")
        self.mount_a.client_remote.write_file(meta_filepath1, meta_contents, sudo=True)
        return createpath

    def _update_fake_trash(self, subvol_name, subvol_group=None, trash_name='fake', create=True):
        group = subvol_group if subvol_group is not None else '_nogroup'
        trashpath = os.path.join("volumes", group, subvol_name, '.trash', trash_name)
        if create:
            self.mount_a.run_shell(['sudo', 'mkdir', '-p', trashpath], omit_sudo=False)
        else:
            self.mount_a.run_shell(['sudo', 'rmdir', trashpath], omit_sudo=False)

    def _configure_guest_auth(self, guest_mount, authid, key):
        """
        Set up auth credentials for a guest client.
        """
        # Create keyring file for the guest client.
        keyring_txt = dedent("""
        [client.{authid}]
            key = {key}

        """.format(authid=authid,key=key))

        guest_mount.client_id = authid
        guest_mount.client_remote.write_file(guest_mount.get_keyring_path(),
                                             keyring_txt, sudo=True)
        # Add a guest client section to the ceph config file.
        self.config_set("client.{0}".format(authid), "debug client", 20)
        self.config_set("client.{0}".format(authid), "debug objecter", 20)
        self.set_conf("client.{0}".format(authid),
                      "keyring", guest_mount.get_keyring_path())

    def _auth_metadata_get(self, filedata):
        """
        Return a deserialized JSON object, or None
        """
        try:
            data = json.loads(filedata)
        except json.decoder.JSONDecodeError:
            data = None
        return data

    def setUp(self):
        super(TestVolumesHelper, self).setUp()
        self.volname = None
        self.vol_created = False
        self._enable_multi_fs()
        self._create_or_reuse_test_volume()
        self.config_set('mon', 'mon_allow_pool_delete', True)
        self.volume_start = random.randint(1, (1<<20))
        self.subvolume_start = random.randint(1, (1<<20))
        self.group_start = random.randint(1, (1<<20))
        self.snapshot_start = random.randint(1, (1<<20))
        self.clone_start = random.randint(1, (1<<20))

    def tearDown(self):
        if self.vol_created:
            self._delete_test_volume()
        super(TestVolumesHelper, self).tearDown()


class TestVolumes(TestVolumesHelper):
    """Tests for FS volume operations."""
    def test_volume_create(self):
        """
        That the volume can be created and then cleans up
        """
        volname = self._generate_random_volume_name()
        self._fs_cmd("volume", "create", volname)
        volumels = json.loads(self._fs_cmd("volume", "ls"))

        if not (volname in ([volume['name'] for volume in volumels])):
            raise RuntimeError("Error creating volume '{0}'".format(volname))

        # check that the pools were created with the correct config
        pool_details = json.loads(self._raw_cmd("osd", "pool", "ls", "detail", "--format=json"))
        pool_flags = {}
        for pool in pool_details:
            pool_flags[pool["pool_id"]] = pool["flags_names"].split(",")

        volume_details = json.loads(self._fs_cmd("get", volname, "--format=json"))
        for data_pool_id in volume_details['mdsmap']['data_pools']:
            self.assertIn("bulk", pool_flags[data_pool_id])
        meta_pool_id = volume_details['mdsmap']['metadata_pool']
        self.assertNotIn("bulk", pool_flags[meta_pool_id])

        # clean up
        self._fs_cmd("volume", "rm", volname, "--yes-i-really-mean-it")

    def test_volume_ls(self):
        """
        That the existing and the newly created volumes can be listed and
        finally cleans up.
        """
        vls = json.loads(self._fs_cmd("volume", "ls"))
        volumes = [volume['name'] for volume in vls]

        #create new volumes and add it to the existing list of volumes
        volumenames = self._generate_random_volume_name(2)
        for volumename in volumenames:
            self._fs_cmd("volume", "create", volumename)
        volumes.extend(volumenames)

        # list volumes
        try:
            volumels = json.loads(self._fs_cmd('volume', 'ls'))
            if len(volumels) == 0:
                raise RuntimeError("Expected the 'fs volume ls' command to list the created volumes.")
            else:
                volnames = [volume['name'] for volume in volumels]
                if collections.Counter(volnames) != collections.Counter(volumes):
                    raise RuntimeError("Error creating or listing volumes")
        finally:
            # clean up
            for volume in volumenames:
                self._fs_cmd("volume", "rm", volume, "--yes-i-really-mean-it")

    def test_volume_rm(self):
        """
        That the volume can only be removed when --yes-i-really-mean-it is used
        and verify that the deleted volume is not listed anymore.
        """
        for m in self.mounts:
            m.umount_wait()
        try:
            self._fs_cmd("volume", "rm", self.volname)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EPERM:
                raise RuntimeError("expected the 'fs volume rm' command to fail with EPERM, "
                                   "but it failed with {0}".format(ce.exitstatus))
            else:
                self._fs_cmd("volume", "rm", self.volname, "--yes-i-really-mean-it")

                #check if it's gone
                volumes = json.loads(self._fs_cmd("volume", "ls", "--format=json-pretty"))
                if (self.volname in [volume['name'] for volume in volumes]):
                    raise RuntimeError("Expected the 'fs volume rm' command to succeed. "
                                       "The volume {0} not removed.".format(self.volname))
        else:
            raise RuntimeError("expected the 'fs volume rm' command to fail.")

    def test_volume_rm_arbitrary_pool_removal(self):
        """
        That the arbitrary pool added to the volume out of band is removed
        successfully on volume removal.
        """
        for m in self.mounts:
            m.umount_wait()
        new_pool = "new_pool"
        # add arbitrary data pool
        self.fs.add_data_pool(new_pool)
        vol_status = json.loads(self._fs_cmd("status", self.volname, "--format=json-pretty"))
        self._fs_cmd("volume", "rm", self.volname, "--yes-i-really-mean-it")

        #check if fs is gone
        volumes = json.loads(self._fs_cmd("volume", "ls", "--format=json-pretty"))
        volnames = [volume['name'] for volume in volumes]
        self.assertNotIn(self.volname, volnames)

        #check if osd pools are gone
        pools = json.loads(self._raw_cmd("osd", "pool", "ls", "--format=json-pretty"))
        for pool in vol_status["pools"]:
            self.assertNotIn(pool["name"], pools)

    def test_volume_rm_when_mon_delete_pool_false(self):
        """
        That the volume can only be removed when mon_allowd_pool_delete is set
        to true and verify that the pools are removed after volume deletion.
        """
        for m in self.mounts:
            m.umount_wait()
        self.config_set('mon', 'mon_allow_pool_delete', False)
        try:
            self._fs_cmd("volume", "rm", self.volname, "--yes-i-really-mean-it")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EPERM,
                             "expected the 'fs volume rm' command to fail with EPERM, "
                             "but it failed with {0}".format(ce.exitstatus))
        vol_status = json.loads(self._fs_cmd("status", self.volname, "--format=json-pretty"))
        self.config_set('mon', 'mon_allow_pool_delete', True)
        self._fs_cmd("volume", "rm", self.volname, "--yes-i-really-mean-it")

        #check if fs is gone
        volumes = json.loads(self._fs_cmd("volume", "ls", "--format=json-pretty"))
        volnames = [volume['name'] for volume in volumes]
        self.assertNotIn(self.volname, volnames,
                         "volume {0} exists after removal".format(self.volname))
        #check if pools are gone
        pools = json.loads(self._raw_cmd("osd", "pool", "ls", "--format=json-pretty"))
        for pool in vol_status["pools"]:
            self.assertNotIn(pool["name"], pools,
                             "pool {0} exists after volume removal".format(pool["name"]))

    def test_volume_rename(self):
        """
        That volume, its file system and pools, can be renamed.
        """
        for m in self.mounts:
            m.umount_wait()
        oldvolname = self.volname
        newvolname = self._generate_random_volume_name()
        new_data_pool, new_metadata_pool = f"cephfs.{newvolname}.data", f"cephfs.{newvolname}.meta"
        self._fs_cmd("volume", "rename", oldvolname, newvolname,
                     "--yes-i-really-mean-it")
        volumels = json.loads(self._fs_cmd('volume', 'ls'))
        volnames = [volume['name'] for volume in volumels]
        # volume name changed
        self.assertIn(newvolname, volnames)
        self.assertNotIn(oldvolname, volnames)
        # pool names changed
        self.fs.get_pool_names(refresh=True)
        self.assertEqual(new_metadata_pool, self.fs.get_metadata_pool_name())
        self.assertEqual(new_data_pool, self.fs.get_data_pool_name())

    def test_volume_rename_idempotency(self):
        """
        That volume rename is idempotent.
        """
        for m in self.mounts:
            m.umount_wait()
        oldvolname = self.volname
        newvolname = self._generate_random_volume_name()
        new_data_pool, new_metadata_pool = f"cephfs.{newvolname}.data", f"cephfs.{newvolname}.meta"
        self._fs_cmd("volume", "rename", oldvolname, newvolname,
                     "--yes-i-really-mean-it")
        self._fs_cmd("volume", "rename", oldvolname, newvolname,
                     "--yes-i-really-mean-it")
        volumels = json.loads(self._fs_cmd('volume', 'ls'))
        volnames = [volume['name'] for volume in volumels]
        self.assertIn(newvolname, volnames)
        self.assertNotIn(oldvolname, volnames)
        self.fs.get_pool_names(refresh=True)
        self.assertEqual(new_metadata_pool, self.fs.get_metadata_pool_name())
        self.assertEqual(new_data_pool, self.fs.get_data_pool_name())

    def test_volume_rename_fails_without_confirmation_flag(self):
        """
        That renaming volume fails without --yes-i-really-mean-it flag.
        """
        newvolname = self._generate_random_volume_name()
        try:
            self._fs_cmd("volume", "rename", self.volname, newvolname)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EPERM,
                "invalid error code on renaming a FS volume without the "
                "'--yes-i-really-mean-it' flag")
        else:
            self.fail("expected renaming of FS volume to fail without the "
                      "'--yes-i-really-mean-it' flag")

    def test_volume_rename_for_more_than_one_data_pool(self):
        """
        That renaming a volume with more than one data pool does not change
        the name of the data pools.
        """
        for m in self.mounts:
            m.umount_wait()
        self.fs.add_data_pool('another-data-pool')
        oldvolname = self.volname
        newvolname = self._generate_random_volume_name()
        self.fs.get_pool_names(refresh=True)
        orig_data_pool_names = list(self.fs.data_pools.values())
        new_metadata_pool = f"cephfs.{newvolname}.meta"
        self._fs_cmd("volume", "rename", self.volname, newvolname,
                     "--yes-i-really-mean-it")
        volumels = json.loads(self._fs_cmd('volume', 'ls'))
        volnames = [volume['name'] for volume in volumels]
        # volume name changed
        self.assertIn(newvolname, volnames)
        self.assertNotIn(oldvolname, volnames)
        self.fs.get_pool_names(refresh=True)
        # metadata pool name changed
        self.assertEqual(new_metadata_pool, self.fs.get_metadata_pool_name())
        # data pool names unchanged
        self.assertCountEqual(orig_data_pool_names, list(self.fs.data_pools.values()))

    def test_volume_info(self):
        """
        Tests the 'fs volume info' command
        """
        vol_fields = ["pools", "used_size", "pending_subvolume_deletions", "mon_addrs"]
        group = self._generate_random_group_name()
        # create subvolumegroup
        self._fs_cmd("subvolumegroup", "create", self.volname, group)
        # get volume metadata
        vol_info = json.loads(self._get_volume_info(self.volname))
        for md in vol_fields:
            self.assertIn(md, vol_info,
                          f"'{md}' key not present in metadata of volume")
        self.assertEqual(vol_info["used_size"], 0,
                         "Size should be zero when volumes directory is empty")

    def test_volume_info_pending_subvol_deletions(self):
        """
        Tests the pending_subvolume_deletions in 'fs volume info' command
        """
        subvolname = self._generate_random_subvolume_name()
        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--mode=777")
        # create 3K zero byte files
        self._do_subvolume_io(subvolname, number_of_files=3000, file_size=0)
        # Delete the subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolname)
        # get volume metadata
        vol_info = json.loads(self._get_volume_info(self.volname))
        self.assertNotEqual(vol_info['pending_subvolume_deletions'], 0,
                            "pending_subvolume_deletions should be 1")
        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_volume_info_without_subvolumegroup(self):
        """
        Tests the 'fs volume info' command without subvolume group
        """
        vol_fields = ["pools", "mon_addrs"]
        # get volume metadata
        vol_info = json.loads(self._get_volume_info(self.volname))
        for md in vol_fields:
            self.assertIn(md, vol_info,
                          f"'{md}' key not present in metadata of volume")
        self.assertNotIn("used_size", vol_info,
                         "'used_size' should not be present in absence of subvolumegroup")
        self.assertNotIn("pending_subvolume_deletions", vol_info,
                         "'pending_subvolume_deletions' should not be present in absence"
                         " of subvolumegroup")

    def test_volume_info_with_human_readable_flag(self):
        """
        Tests the 'fs volume info --human_readable' command
        """
        vol_fields = ["pools", "used_size", "pending_subvolume_deletions", "mon_addrs"]
        group = self._generate_random_group_name()
        # create subvolumegroup
        self._fs_cmd("subvolumegroup", "create", self.volname, group)
        # get volume metadata
        vol_info = json.loads(self._get_volume_info(self.volname, "--human_readable"))
        for md in vol_fields:
            self.assertIn(md, vol_info,
                          f"'{md}' key not present in metadata of volume")
        units = [' ', 'k', 'M', 'G', 'T', 'P', 'E']
        assert vol_info["used_size"][-1] in units, "unit suffix in used_size is absent"
        assert vol_info["pools"]["data"][0]["avail"][-1] in units, "unit suffix in avail data is absent"
        assert vol_info["pools"]["data"][0]["used"][-1] in units, "unit suffix in used data is absent"
        assert vol_info["pools"]["metadata"][0]["avail"][-1] in units, "unit suffix in avail metadata is absent"
        assert vol_info["pools"]["metadata"][0]["used"][-1] in units, "unit suffix in used metadata is absent"
        self.assertEqual(int(vol_info["used_size"]), 0,
                         "Size should be zero when volumes directory is empty")

    def test_volume_info_with_human_readable_flag_without_subvolumegroup(self):
        """
        Tests the 'fs volume info --human_readable' command without subvolume group
        """
        vol_fields = ["pools", "mon_addrs"]
        # get volume metadata
        vol_info = json.loads(self._get_volume_info(self.volname, "--human_readable"))
        for md in vol_fields:
            self.assertIn(md, vol_info,
                          f"'{md}' key not present in metadata of volume")
        units = [' ', 'k', 'M', 'G', 'T', 'P', 'E']
        assert vol_info["pools"]["data"][0]["avail"][-1] in units, "unit suffix in avail data is absent"
        assert vol_info["pools"]["data"][0]["used"][-1] in units, "unit suffix in used data is absent"
        assert vol_info["pools"]["metadata"][0]["avail"][-1] in units, "unit suffix in avail metadata is absent"
        assert vol_info["pools"]["metadata"][0]["used"][-1] in units, "unit suffix in used metadata is absent"
        self.assertNotIn("used_size", vol_info,
                         "'used_size' should not be present in absence of subvolumegroup")
        self.assertNotIn("pending_subvolume_deletions", vol_info,
                         "'pending_subvolume_deletions' should not be present in absence"
                         " of subvolumegroup")


class TestSubvolumeGroups(TestVolumesHelper):
    """Tests for FS subvolume group operations."""
    def test_default_uid_gid_subvolume_group(self):
        group = self._generate_random_group_name()
        expected_uid = 0
        expected_gid = 0

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)
        group_path = self._get_subvolume_group_path(self.volname, group)

        # check group's uid and gid
        stat = self.mount_a.stat(group_path)
        self.assertEqual(stat['st_uid'], expected_uid)
        self.assertEqual(stat['st_gid'], expected_gid)

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_nonexistent_subvolume_group_create(self):
        subvolume = self._generate_random_subvolume_name()
        group = "non_existent_group"

        # try, creating subvolume in a nonexistent group
        try:
            self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOENT:
                raise
        else:
            raise RuntimeError("expected the 'fs subvolume create' command to fail")

    def test_nonexistent_subvolume_group_rm(self):
        group = "non_existent_group"

        # try, remove subvolume group
        try:
            self._fs_cmd("subvolumegroup", "rm", self.volname, group)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOENT:
                raise
        else:
            raise RuntimeError("expected the 'fs subvolumegroup rm' command to fail")

    def test_subvolume_group_create_with_auto_cleanup_on_fail(self):
        group = self._generate_random_group_name()
        data_pool = "invalid_pool"
        # create group with invalid data pool layout
        with self.assertRaises(CommandFailedError):
            self._fs_cmd("subvolumegroup", "create", self.volname, group, "--pool_layout", data_pool)

        # check whether group path is cleaned up
        try:
            self._fs_cmd("subvolumegroup", "getpath", self.volname, group)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOENT:
                raise
        else:
            raise RuntimeError("expected the 'fs subvolumegroup getpath' command to fail")

    def test_subvolume_group_create_with_desired_data_pool_layout(self):
        group1, group2 = self._generate_random_group_name(2)

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group1)
        group1_path = self._get_subvolume_group_path(self.volname, group1)

        default_pool = self.mount_a.getfattr(group1_path, "ceph.dir.layout.pool")
        new_pool = "new_pool"
        self.assertNotEqual(default_pool, new_pool)

        # add data pool
        newid = self.fs.add_data_pool(new_pool)

        # create group specifying the new data pool as its pool layout
        self._fs_cmd("subvolumegroup", "create", self.volname, group2,
                     "--pool_layout", new_pool)
        group2_path = self._get_subvolume_group_path(self.volname, group2)

        desired_pool = self.mount_a.getfattr(group2_path, "ceph.dir.layout.pool")
        try:
            self.assertEqual(desired_pool, new_pool)
        except AssertionError:
            self.assertEqual(int(desired_pool), newid) # old kernel returns id

        self._fs_cmd("subvolumegroup", "rm", self.volname, group1)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group2)

    def test_subvolume_group_create_with_desired_mode(self):
        group1, group2 = self._generate_random_group_name(2)
        # default mode
        expected_mode1 = "755"
        # desired mode
        expected_mode2 = "777"

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group2, f"--mode={expected_mode2}")
        self._fs_cmd("subvolumegroup", "create", self.volname, group1)

        group1_path = self._get_subvolume_group_path(self.volname, group1)
        group2_path = self._get_subvolume_group_path(self.volname, group2)
        volumes_path = os.path.dirname(group1_path)

        # check group's mode
        actual_mode1 = self.mount_a.run_shell(['stat', '-c' '%a', group1_path]).stdout.getvalue().strip()
        actual_mode2 = self.mount_a.run_shell(['stat', '-c' '%a', group2_path]).stdout.getvalue().strip()
        actual_mode3 = self.mount_a.run_shell(['stat', '-c' '%a', volumes_path]).stdout.getvalue().strip()
        self.assertEqual(actual_mode1, expected_mode1)
        self.assertEqual(actual_mode2, expected_mode2)
        self.assertEqual(actual_mode3, expected_mode1)

        self._fs_cmd("subvolumegroup", "rm", self.volname, group1)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group2)

    def test_subvolume_group_create_with_desired_uid_gid(self):
        """
        That the subvolume group can be created with the desired uid and gid and its uid and gid matches the
        expected values.
        """
        uid = 1000
        gid = 1000

        # create subvolume group
        subvolgroupname = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, subvolgroupname, "--uid", str(uid), "--gid", str(gid))

        # make sure it exists
        subvolgrouppath = self._get_subvolume_group_path(self.volname, subvolgroupname)
        self.assertNotEqual(subvolgrouppath, None)

        # verify the uid and gid
        suid = int(self.mount_a.run_shell(['stat', '-c' '%u', subvolgrouppath]).stdout.getvalue().strip())
        sgid = int(self.mount_a.run_shell(['stat', '-c' '%g', subvolgrouppath]).stdout.getvalue().strip())
        self.assertEqual(uid, suid)
        self.assertEqual(gid, sgid)

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, subvolgroupname)

    def test_subvolume_group_create_with_invalid_data_pool_layout(self):
        group = self._generate_random_group_name()
        data_pool = "invalid_pool"
        # create group with invalid data pool layout
        try:
            self._fs_cmd("subvolumegroup", "create", self.volname, group, "--pool_layout", data_pool)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EINVAL:
                raise
        else:
            raise RuntimeError("expected the 'fs subvolumegroup create' command to fail")

    def test_subvolume_group_create_with_size(self):
        # create group with size -- should set quota
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group, "1000000000")

        # get group metadata
        group_info = json.loads(self._get_subvolume_group_info(self.volname, group))
        self.assertEqual(group_info["bytes_quota"], 1000000000)

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_group_info(self):
        # tests the 'fs subvolumegroup info' command

        group_md = ["atime", "bytes_pcent", "bytes_quota", "bytes_used", "created_at", "ctime",
                     "data_pool", "gid", "mode", "mon_addrs", "mtime", "uid"]

        # create group
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # get group metadata
        group_info = json.loads(self._get_subvolume_group_info(self.volname, group))
        for md in group_md:
            self.assertIn(md, group_info, "'{0}' key not present in metadata of group".format(md))

        self.assertEqual(group_info["bytes_pcent"], "undefined", "bytes_pcent should be set to undefined if quota is not set")
        self.assertEqual(group_info["bytes_quota"], "infinite", "bytes_quota should be set to infinite if quota is not set")
        self.assertEqual(group_info["uid"], 0)
        self.assertEqual(group_info["gid"], 0)

        nsize = self.DEFAULT_FILE_SIZE*1024*1024
        self._fs_cmd("subvolumegroup", "resize", self.volname, group, str(nsize))

        # get group metadata after quota set
        group_info = json.loads(self._get_subvolume_group_info(self.volname, group))
        for md in group_md:
            self.assertIn(md, group_info, "'{0}' key not present in metadata of subvolume".format(md))

        self.assertNotEqual(group_info["bytes_pcent"], "undefined", "bytes_pcent should not be set to undefined if quota is set")
        self.assertEqual(group_info["bytes_quota"], nsize, "bytes_quota should be set to '{0}'".format(nsize))

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_group_create_idempotence(self):
        # create group
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # try creating w/ same subvolume group name -- should be idempotent
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_group_create_idempotence_mode(self):
        # create group
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # try creating w/ same subvolume group name with mode -- should set mode
        self._fs_cmd("subvolumegroup", "create", self.volname, group, "--mode=766")

        group_path = self._get_subvolume_group_path(self.volname, group)

        # check subvolumegroup's  mode
        mode = self.mount_a.run_shell(['stat', '-c' '%a', group_path]).stdout.getvalue().strip()
        self.assertEqual(mode, "766")

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_group_create_idempotence_uid_gid(self):
        desired_uid = 1000
        desired_gid = 1000

        # create group
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # try creating w/ same subvolume group name with uid/gid -- should set uid/gid
        self._fs_cmd("subvolumegroup", "create", self.volname, group, "--uid", str(desired_uid), "--gid", str(desired_gid))

        group_path = self._get_subvolume_group_path(self.volname, group)

        # verify the uid and gid
        actual_uid = int(self.mount_a.run_shell(['stat', '-c' '%u', group_path]).stdout.getvalue().strip())
        actual_gid = int(self.mount_a.run_shell(['stat', '-c' '%g', group_path]).stdout.getvalue().strip())
        self.assertEqual(desired_uid, actual_uid)
        self.assertEqual(desired_gid, actual_gid)

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_group_create_idempotence_data_pool(self):
        # create group
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        group_path = self._get_subvolume_group_path(self.volname, group)

        default_pool = self.mount_a.getfattr(group_path, "ceph.dir.layout.pool")
        new_pool = "new_pool"
        self.assertNotEqual(default_pool, new_pool)

        # add data pool
        newid = self.fs.add_data_pool(new_pool)

        # try creating w/ same subvolume group name with new data pool -- should set pool
        self._fs_cmd("subvolumegroup", "create", self.volname, group, "--pool_layout", new_pool)
        desired_pool = self.mount_a.getfattr(group_path, "ceph.dir.layout.pool")
        try:
            self.assertEqual(desired_pool, new_pool)
        except AssertionError:
            self.assertEqual(int(desired_pool), newid) # old kernel returns id

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_group_create_idempotence_resize(self):
        # create group
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # try creating w/ same subvolume name with size -- should set quota
        self._fs_cmd("subvolumegroup", "create", self.volname, group, "1000000000")

        # get group metadata
        group_info = json.loads(self._get_subvolume_group_info(self.volname, group))
        self.assertEqual(group_info["bytes_quota"], 1000000000)

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_group_quota_mds_path_restriction_to_group_path(self):
        """
        Tests subvolumegroup quota enforcement with mds path restriction set to group.
        For quota to be enforced, read permission needs to be provided to the parent
        of the directory on which quota is set. Please see the tracker comment [1]
        [1] https://tracker.ceph.com/issues/55090#note-8
        """
        osize = self.DEFAULT_FILE_SIZE*1024*1024*100
        # create group with 100MB quota
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group,
                     "--size", str(osize), "--mode=777")

        # make sure it exists
        grouppath = self._get_subvolume_group_path(self.volname, group)
        self.assertNotEqual(grouppath, None)

        # create subvolume under the group
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname,
                     "--group_name", group, "--mode=777")

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname, group_name=group)
        self.assertNotEqual(subvolpath, None)

        # Create auth_id
        authid = "client.guest1"
        user = json.loads(self.fs.mon_manager.raw_cluster_cmd(
            "auth", "get-or-create", authid,
            "mds", "allow rw path=/volumes",
            "mgr", "allow rw",
            "osd", "allow rw tag cephfs *=*",
            "mon", "allow r",
            "--format=json-pretty"
            ))

        # Prepare guest_mount with new authid
        guest_mount = self.mount_b
        guest_mount.umount_wait()

        # configure credentials for guest client
        self._configure_guest_auth(guest_mount, "guest1", user[0]["key"])

        # mount the subvolume
        mount_path = os.path.join("/", subvolpath)
        guest_mount.mount_wait(cephfs_mntpt=mount_path)

        # create 99 files of 1MB
        guest_mount.run_shell_payload("mkdir -p dir1")
        for i in range(99):
            filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, i)
            guest_mount.write_n_mb(os.path.join("dir1", filename), self.DEFAULT_FILE_SIZE)
        try:
            # write two files of 1MB file to exceed the quota
            guest_mount.run_shell_payload("mkdir -p dir2")
            for i in range(2):
                filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, i)
                guest_mount.write_n_mb(os.path.join("dir2", filename), self.DEFAULT_FILE_SIZE)
            # For quota to be enforced
            time.sleep(60)
            # create 400 files of 1MB to exceed quota
            for i in range(400):
                filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, i)
                guest_mount.write_n_mb(os.path.join("dir2", filename), self.DEFAULT_FILE_SIZE)
                # Sometimes quota enforcement takes time.
                if i == 200:
                    time.sleep(60)
        except CommandFailedError:
            pass
        else:
            self.fail(f"expected filling subvolume {subvolname} with 400 files of size 1MB to fail")

        # clean up
        guest_mount.umount_wait()

        # Delete the subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, "--group_name", group)

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_group_quota_mds_path_restriction_to_subvolume_path(self):
        """
        Tests subvolumegroup quota enforcement with mds path restriction set to subvolume path
        The quota should not be enforced because of the fourth limitation mentioned at
        https://docs.ceph.com/en/latest/cephfs/quota/#limitations
        """
        osize = self.DEFAULT_FILE_SIZE*1024*1024*100
        # create group with 100MB quota
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group,
                     "--size", str(osize), "--mode=777")

        # make sure it exists
        grouppath = self._get_subvolume_group_path(self.volname, group)
        self.assertNotEqual(grouppath, None)

        # create subvolume under the group
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname,
                     "--group_name", group, "--mode=777")

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname, group_name=group)
        self.assertNotEqual(subvolpath, None)

        mount_path = os.path.join("/", subvolpath)

        # Create auth_id
        authid = "client.guest1"
        user = json.loads(self.fs.mon_manager.raw_cluster_cmd(
            "auth", "get-or-create", authid,
            "mds", f"allow rw path={mount_path}",
            "mgr", "allow rw",
            "osd", "allow rw tag cephfs *=*",
            "mon", "allow r",
            "--format=json-pretty"
            ))

        # Prepare guest_mount with new authid
        guest_mount = self.mount_b
        guest_mount.umount_wait()

        # configure credentials for guest client
        self._configure_guest_auth(guest_mount, "guest1", user[0]["key"])

        # mount the subvolume
        guest_mount.mount_wait(cephfs_mntpt=mount_path)

        # create 99 files of 1MB to exceed quota
        guest_mount.run_shell_payload("mkdir -p dir1")
        for i in range(99):
            filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, i)
            guest_mount.write_n_mb(os.path.join("dir1", filename), self.DEFAULT_FILE_SIZE)
        try:
            # write two files of 1MB file to exceed the quota
            guest_mount.run_shell_payload("mkdir -p dir2")
            for i in range(2):
                filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, i)
                guest_mount.write_n_mb(os.path.join("dir2", filename), self.DEFAULT_FILE_SIZE)
            # For quota to be enforced
            time.sleep(60)
            # create 400 files of 1MB to exceed quota
            for i in range(400):
                filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, i)
                guest_mount.write_n_mb(os.path.join("dir2", filename), self.DEFAULT_FILE_SIZE)
                # Sometimes quota enforcement takes time.
                if i == 200:
                    time.sleep(60)
        except CommandFailedError:
            self.fail(f"Quota should not be enforced, expected filling subvolume {subvolname} with 400 files of size 1MB to succeed")

        # clean up
        guest_mount.umount_wait()

        # Delete the subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, "--group_name", group)

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_group_quota_exceeded_subvolume_removal(self):
        """
        Tests subvolume removal if it's group quota is exceeded
        """
        osize = self.DEFAULT_FILE_SIZE*1024*1024*100
        # create group with 100MB quota
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group,
                     "--size", str(osize), "--mode=777")

        # make sure it exists
        grouppath = self._get_subvolume_group_path(self.volname, group)
        self.assertNotEqual(grouppath, None)

        # create subvolume under the group
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname,
                     "--group_name", group, "--mode=777")

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname, group_name=group)
        self.assertNotEqual(subvolpath, None)

        # create 99 files of 1MB to exceed quota
        self._do_subvolume_io(subvolname, subvolume_group=group, number_of_files=99)

        try:
            # write two files of 1MB file to exceed the quota
            self._do_subvolume_io(subvolname, subvolume_group=group, create_dir='dir1', number_of_files=2)
            # For quota to be enforced
            time.sleep(20)
            # create 400 files of 1MB to exceed quota
            self._do_subvolume_io(subvolname, subvolume_group=group, create_dir='dir1', number_of_files=400)
        except CommandFailedError:
            # Delete subvolume when group quota is exceeded
            self._fs_cmd("subvolume", "rm", self.volname, subvolname, "--group_name", group)
        else:
            self.fail(f"expected filling subvolume {subvolname} with 400 files of size 1MB to fail")

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_group_quota_exceeded_subvolume_removal_retained_snaps(self):
        """
        Tests retained snapshot subvolume removal if it's group quota is exceeded
        """
        group = self._generate_random_group_name()
        subvolname = self._generate_random_subvolume_name()
        snapshot1, snapshot2 = self._generate_random_snapshot_name(2)

        osize = self.DEFAULT_FILE_SIZE*1024*1024*100
        # create group with 100MB quota
        self._fs_cmd("subvolumegroup", "create", self.volname, group,
                     "--size", str(osize), "--mode=777")

        # make sure it exists
        grouppath = self._get_subvolume_group_path(self.volname, group)
        self.assertNotEqual(grouppath, None)

        # create subvolume under the group
        self._fs_cmd("subvolume", "create", self.volname, subvolname,
                     "--group_name", group, "--mode=777")

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname, group_name=group)
        self.assertNotEqual(subvolpath, None)

        # create 99 files of 1MB to exceed quota
        self._do_subvolume_io(subvolname, subvolume_group=group, number_of_files=99)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot1, "--group_name", group)
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot2, "--group_name", group)

        try:
            # write two files of 1MB file to exceed the quota
            self._do_subvolume_io(subvolname, subvolume_group=group, create_dir='dir1', number_of_files=2)
            # For quota to be enforced
            time.sleep(20)
            # create 400 files of 1MB to exceed quota
            self._do_subvolume_io(subvolname, subvolume_group=group, number_of_files=400)
        except CommandFailedError:
            # remove with snapshot retention
            self._fs_cmd("subvolume", "rm", self.volname, subvolname, "--group_name", group, "--retain-snapshots")
            # remove snapshot1
            self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot1, "--group_name", group)
            # remove snapshot2 (should remove volume)
            self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot2, "--group_name", group)
            # verify subvolume trash is clean
            self._wait_for_subvol_trash_empty(subvolname, group=group)
        else:
            self.fail(f"expected filling subvolume {subvolname} with 400 files of size 1MB to fail")

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_group_quota_subvolume_removal(self):
        """
        Tests subvolume removal if it's group quota is set.
        """
        # create group with size -- should set quota
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group, "1000000000")

        # create subvolume under the group
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", group)

        # remove subvolume
        try:
            self._fs_cmd("subvolume", "rm", self.volname, subvolname, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume rm' command to succeed if group quota is set")

        # remove subvolumegroup
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_group_quota_legacy_subvolume_removal(self):
        """
        Tests legacy subvolume removal if it's group quota is set.
        """
        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # emulate a old-fashioned subvolume -- in a custom group
        createpath1 = os.path.join(".", "volumes", group, subvolume)
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', createpath1], omit_sudo=False)

        # this would auto-upgrade on access without anyone noticing
        subvolpath1 = self._fs_cmd("subvolume", "getpath", self.volname, subvolume, "--group-name", group)
        self.assertNotEqual(subvolpath1, None)
        subvolpath1 = subvolpath1.rstrip() # remove "/" prefix and any trailing newline

        # and... the subvolume path returned should be what we created behind the scene
        self.assertEqual(createpath1[1:], subvolpath1)

        # Set subvolumegroup quota on idempotent subvolumegroup creation
        self._fs_cmd("subvolumegroup", "create", self.volname, group, "1000000000")

        # remove subvolume
        try:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume rm' command to succeed if group quota is set")

        # remove subvolumegroup
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_group_quota_v1_subvolume_removal(self):
        """
        Tests v1 subvolume removal if it's group quota is set.
        """
        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # emulate a v1 subvolume -- in a custom group
        self._create_v1_subvolume(subvolume, subvol_group=group, has_snapshot=False)

        # Set subvolumegroup quota on idempotent subvolumegroup creation
        self._fs_cmd("subvolumegroup", "create", self.volname, group, "1000000000")

        # remove subvolume
        try:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume rm' command to succeed if group quota is set")

        # remove subvolumegroup
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_group_resize_fail_invalid_size(self):
        """
        That a subvolume group cannot be resized to an invalid size and the quota did not change
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024
        # create group with 1MB quota
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group, "--size", str(osize))

        # make sure it exists
        grouppath = self._get_subvolume_group_path(self.volname, group)
        self.assertNotEqual(grouppath, None)

        # try to resize the subvolume with an invalid size -10
        nsize = -10
        try:
            self._fs_cmd("subvolumegroup", "resize", self.volname, group, str(nsize))
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL,
                             "invalid error code on resize of subvolume group with invalid size")
        else:
            self.fail("expected the 'fs subvolumegroup resize' command to fail")

        # verify the quota did not change
        size = int(self.mount_a.getfattr(grouppath, "ceph.quota.max_bytes"))
        self.assertEqual(size, osize)

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_group_resize_fail_zero_size(self):
        """
        That a subvolume group cannot be resized to a zero size and the quota did not change
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024
        # create group with 1MB quota
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group, "--size", str(osize))

        # make sure it exists
        grouppath = self._get_subvolume_group_path(self.volname, group)
        self.assertNotEqual(grouppath, None)

        # try to resize the subvolume group with size 0
        nsize = 0
        try:
            self._fs_cmd("subvolumegroup", "resize", self.volname, group, str(nsize))
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL,
                             "invalid error code on resize of subvolume group with invalid size")
        else:
            self.fail("expected the 'fs subvolumegroup resize' command to fail")

        # verify the quota did not change
        size = int(self.mount_a.getfattr(grouppath, "ceph.quota.max_bytes"))
        self.assertEqual(size, osize)

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_group_resize_quota_lt_used_size(self):
        """
        That a subvolume group can be resized to a size smaller than the current used size
        and the resulting quota matches the expected size.
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024*20
        # create group with 20MB quota
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group,
                     "--size", str(osize), "--mode=777")

        # make sure it exists
        grouppath = self._get_subvolume_group_path(self.volname, group)
        self.assertNotEqual(grouppath, None)

        # create subvolume under the group
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname,
                     "--group_name", group, "--mode=777")

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname, group_name=group)
        self.assertNotEqual(subvolpath, None)

        # create one file of 10MB
        file_size=self.DEFAULT_FILE_SIZE*10
        number_of_files=1
        log.debug("filling subvolume {0} with {1} file of size {2}MB".format(subvolname,
                                                                             number_of_files,
                                                                             file_size))
        filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, self.DEFAULT_NUMBER_OF_FILES+1)
        self.mount_a.write_n_mb(os.path.join(subvolpath, filename), file_size)

        usedsize = int(self.mount_a.getfattr(subvolpath, "ceph.dir.rbytes"))

        # shrink the subvolume group
        nsize = usedsize // 2
        try:
            self._fs_cmd("subvolumegroup", "resize", self.volname, group, str(nsize))
        except CommandFailedError:
            self.fail("expected the 'fs subvolumegroup resize' command to succeed")

        # verify the quota
        size = int(self.mount_a.getfattr(grouppath, "ceph.quota.max_bytes"))
        self.assertEqual(size, nsize)

        # remove subvolume and group
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_group_resize_fail_quota_lt_used_size_no_shrink(self):
        """
        That a subvolume group cannot be resized to a size smaller than the current used size
        when --no_shrink is given and the quota did not change.
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024*20
        # create group with 20MB quota
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group,
                     "--size", str(osize), "--mode=777")

        # make sure it exists
        grouppath = self._get_subvolume_group_path(self.volname, group)
        self.assertNotEqual(grouppath, None)

        # create subvolume under the group
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname,
                     "--group_name", group, "--mode=777")

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname, group_name=group)
        self.assertNotEqual(subvolpath, None)

        # create one file of 10MB
        file_size=self.DEFAULT_FILE_SIZE*10
        number_of_files=1
        log.debug("filling subvolume {0} with {1} file of size {2}MB".format(subvolname,
                                                                             number_of_files,
                                                                             file_size))
        filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, self.DEFAULT_NUMBER_OF_FILES+2)
        self.mount_a.write_n_mb(os.path.join(subvolpath, filename), file_size)

        usedsize = int(self.mount_a.getfattr(grouppath, "ceph.dir.rbytes"))

        # shrink the subvolume group
        nsize = usedsize // 2
        try:
            self._fs_cmd("subvolumegroup", "resize", self.volname, group, str(nsize), "--no_shrink")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL, "invalid error code on resize of subvolumegroup with quota less than used")
        else:
            self.fail("expected the 'fs subvolumegroup resize' command to fail")

        # verify the quota did not change
        size = int(self.mount_a.getfattr(grouppath, "ceph.quota.max_bytes"))
        self.assertEqual(size, osize)

        # remove subvolume and group
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_group_resize_expand_on_full_subvolume(self):
        """
        That the subvolume group can be expanded after it is full and future write succeed
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024*100
        # create group with 100MB quota
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group,
                     "--size", str(osize), "--mode=777")

        # make sure it exists
        grouppath = self._get_subvolume_group_path(self.volname, group)
        self.assertNotEqual(grouppath, None)

        # create subvolume under the group
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname,
                     "--group_name", group, "--mode=777")

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname, group_name=group)
        self.assertNotEqual(subvolpath, None)

        # create 99 files of 1MB
        self._do_subvolume_io(subvolname, subvolume_group=group, number_of_files=99)

        try:
            # write two files of 1MB file to exceed the quota
            self._do_subvolume_io(subvolname, subvolume_group=group, create_dir='dir1', number_of_files=2)
            # For quota to be enforced
            time.sleep(20)
            # create 500 files of 1MB
            self._do_subvolume_io(subvolname, subvolume_group=group, create_dir='dir1', number_of_files=500)
        except CommandFailedError:
            # Not able to write. So expand the subvolumegroup more and try writing the files again
            nsize = osize*7
            self._fs_cmd("subvolumegroup", "resize", self.volname, group, str(nsize))
            try:
                self._do_subvolume_io(subvolname, subvolume_group=group, create_dir='dir1', number_of_files=500)
            except CommandFailedError:
                self.fail("expected filling subvolume {0} with 500 files of size 1MB "
                          "to succeed".format(subvolname))
        else:
            self.fail("expected filling subvolume {0} with 500 files of size 1MB "
                      "to fail".format(subvolname))

        # remove subvolume and group
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_group_resize_infinite_size(self):
        """
        That a subvolume group can be resized to an infinite size by unsetting its quota.
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024
        # create group
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group,
                     "--size", str(osize))

        # make sure it exists
        grouppath = self._get_subvolume_group_path(self.volname, group)
        self.assertNotEqual(grouppath, None)

        # resize inf
        self._fs_cmd("subvolumegroup", "resize", self.volname, group, "inf")

        # verify that the quota is None
        size = self.mount_a.getfattr(grouppath, "ceph.quota.max_bytes")
        self.assertEqual(size, None)

        # remove subvolume group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_group_resize_infinite_size_future_writes(self):
        """
        That a subvolume group can be resized to an infinite size and the future writes succeed.
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024*5
        # create group with 5MB quota
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group,
                     "--size", str(osize), "--mode=777")

        # make sure it exists
        grouppath = self._get_subvolume_group_path(self.volname, group)
        self.assertNotEqual(grouppath, None)

        # create subvolume under the group
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname,
                     "--group_name", group, "--mode=777")

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname, group_name=group)
        self.assertNotEqual(subvolpath, None)

        # create 4 files of 1MB
        self._do_subvolume_io(subvolname, subvolume_group=group, number_of_files=4)

        try:
            # write two files of 1MB file to exceed the quota
            self._do_subvolume_io(subvolname, subvolume_group=group, create_dir='dir1', number_of_files=2)
            # For quota to be enforced
            time.sleep(20)
            # create 500 files of 1MB
            self._do_subvolume_io(subvolname, subvolume_group=group, create_dir='dir1', number_of_files=500)
        except CommandFailedError:
            # Not able to write. So resize subvolumegroup to 'inf' and try writing the files again
            # resize inf
            self._fs_cmd("subvolumegroup", "resize", self.volname, group, "inf")
            try:
                self._do_subvolume_io(subvolname, subvolume_group=group, create_dir='dir1', number_of_files=500)
            except CommandFailedError:
                self.fail("expected filling subvolume {0} with 500 files of size 1MB "
                          "to succeed".format(subvolname))
        else:
            self.fail("expected filling subvolume {0} with 500 files of size 1MB "
                      "to fail".format(subvolname))


        # verify that the quota is None
        size = self.mount_a.getfattr(grouppath, "ceph.quota.max_bytes")
        self.assertEqual(size, None)

        # remove subvolume and group
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_group_ls(self):
        # tests the 'fs subvolumegroup ls' command

        subvolumegroups = []

        #create subvolumegroups
        subvolumegroups = self._generate_random_group_name(3)
        for groupname in subvolumegroups:
            self._fs_cmd("subvolumegroup", "create", self.volname, groupname)

        subvolumegroupls = json.loads(self._fs_cmd('subvolumegroup', 'ls', self.volname))
        if len(subvolumegroupls) == 0:
            raise RuntimeError("Expected the 'fs subvolumegroup ls' command to list the created subvolume groups")
        else:
            subvolgroupnames = [subvolumegroup['name'] for subvolumegroup in subvolumegroupls]
            if collections.Counter(subvolgroupnames) != collections.Counter(subvolumegroups):
                raise RuntimeError("Error creating or listing subvolume groups")

    def test_subvolume_group_ls_filter(self):
        # tests the 'fs subvolumegroup ls' command filters '_deleting' directory

        subvolumegroups = []

        #create subvolumegroup
        subvolumegroups = self._generate_random_group_name(3)
        for groupname in subvolumegroups:
            self._fs_cmd("subvolumegroup", "create", self.volname, groupname)

        # create subvolume and remove. This creates '_deleting' directory.
        subvolume = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        subvolumegroupls = json.loads(self._fs_cmd('subvolumegroup', 'ls', self.volname))
        subvolgroupnames = [subvolumegroup['name'] for subvolumegroup in subvolumegroupls]
        if "_deleting" in subvolgroupnames:
            self.fail("Listing subvolume groups listed '_deleting' directory")

    def test_subvolume_group_ls_filter_internal_directories(self):
        # tests the 'fs subvolumegroup ls' command filters internal directories
        # eg: '_deleting', '_nogroup', '_index', "_legacy"

        subvolumegroups = self._generate_random_group_name(3)
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        #create subvolumegroups
        for groupname in subvolumegroups:
            self._fs_cmd("subvolumegroup", "create", self.volname, groupname)

        # create subvolume which will create '_nogroup' directory
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # create snapshot
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # clone snapshot which will create '_index' directory
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # wait for clone to complete
        self._wait_for_clone_to_complete(clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolume which will create '_deleting' directory
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # list subvolumegroups
        ret = json.loads(self._fs_cmd('subvolumegroup', 'ls', self.volname))
        self.assertEqual(len(ret), len(subvolumegroups))

        ret_list = [subvolumegroup['name'] for subvolumegroup in ret]
        self.assertEqual(len(ret_list), len(subvolumegroups))

        self.assertEqual(all(elem in subvolumegroups for elem in ret_list), True)

        # cleanup
        self._fs_cmd("subvolume", "rm", self.volname, clone)
        for groupname in subvolumegroups:
            self._fs_cmd("subvolumegroup", "rm", self.volname, groupname)

    def test_subvolume_group_ls_for_nonexistent_volume(self):
        # tests the 'fs subvolumegroup ls' command when /volume doesn't exist
        # prerequisite: we expect that the test volume is created and a subvolumegroup is NOT created

        # list subvolume groups
        subvolumegroupls = json.loads(self._fs_cmd('subvolumegroup', 'ls', self.volname))
        if len(subvolumegroupls) > 0:
            raise RuntimeError("Expected the 'fs subvolumegroup ls' command to output an empty list")

    def test_subvolumegroup_pin_distributed(self):
        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()
        self.config_set('mds', 'mds_export_ephemeral_distributed', True)

        group = "pinme"
        self._fs_cmd("subvolumegroup", "create", self.volname, group)
        self._fs_cmd("subvolumegroup", "pin", self.volname, group, "distributed", "True")
        subvolumes = self._generate_random_subvolume_name(50)
        for subvolume in subvolumes:
            self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)
        self._wait_distributed_subtrees(2 * 2, status=status, rank="all")

        # remove subvolumes
        for subvolume in subvolumes:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_group_rm_force(self):
        # test removing non-existing subvolume group with --force
        group = self._generate_random_group_name()
        try:
            self._fs_cmd("subvolumegroup", "rm", self.volname, group, "--force")
        except CommandFailedError:
            raise RuntimeError("expected the 'fs subvolumegroup rm --force' command to succeed")

    def test_subvolume_group_exists_with_subvolumegroup_and_no_subvolume(self):
        """Test the presence of any subvolumegroup when only subvolumegroup is present"""

        group = self._generate_random_group_name()
        # create subvolumegroup
        self._fs_cmd("subvolumegroup", "create", self.volname, group)
        ret = self._fs_cmd("subvolumegroup", "exist", self.volname)
        self.assertEqual(ret.strip('\n'), "subvolumegroup exists")
        # delete subvolumegroup
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)
        ret = self._fs_cmd("subvolumegroup", "exist", self.volname)
        self.assertEqual(ret.strip('\n'), "no subvolumegroup exists")

    def test_subvolume_group_exists_with_no_subvolumegroup_and_subvolume(self):
        """Test the presence of any subvolumegroup when no subvolumegroup is present"""

        ret = self._fs_cmd("subvolumegroup", "exist", self.volname)
        self.assertEqual(ret.strip('\n'), "no subvolumegroup exists")

    def test_subvolume_group_exists_with_subvolumegroup_and_subvolume(self):
        """Test the presence of any subvolume when subvolumegroup
            and subvolume both are present"""

        group = self._generate_random_group_name()
        subvolume = self._generate_random_subvolume_name(2)
        # create subvolumegroup
        self._fs_cmd("subvolumegroup", "create", self.volname, group)
        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume[0], "--group_name", group)
        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume[1])
        ret = self._fs_cmd("subvolumegroup", "exist", self.volname)
        self.assertEqual(ret.strip('\n'), "subvolumegroup exists")
        # delete subvolume in group
        self._fs_cmd("subvolume", "rm", self.volname, subvolume[0], "--group_name", group)
        ret = self._fs_cmd("subvolumegroup", "exist", self.volname)
        self.assertEqual(ret.strip('\n'), "subvolumegroup exists")
        # delete subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume[1])
        ret = self._fs_cmd("subvolumegroup", "exist", self.volname)
        self.assertEqual(ret.strip('\n'), "subvolumegroup exists")
        # delete subvolumegroup
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)
        ret = self._fs_cmd("subvolumegroup", "exist", self.volname)
        self.assertEqual(ret.strip('\n'), "no subvolumegroup exists")

    def test_subvolume_group_exists_without_subvolumegroup_and_with_subvolume(self):
        """Test the presence of any subvolume when subvolume is present
            but no subvolumegroup is present"""

        subvolume = self._generate_random_subvolume_name()
        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)
        ret = self._fs_cmd("subvolumegroup", "exist", self.volname)
        self.assertEqual(ret.strip('\n'), "no subvolumegroup exists")
        # delete subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        ret = self._fs_cmd("subvolumegroup", "exist", self.volname)
        self.assertEqual(ret.strip('\n'), "no subvolumegroup exists")

    def test_subvolume_group_rm_when_its_not_empty(self):
        group = self._generate_random_group_name()
        subvolume = self._generate_random_subvolume_name()

        # create subvolumegroup
        self._fs_cmd("subvolumegroup", "create", self.volname, group)
        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)
        # try, remove subvolume group
        try:
            self._fs_cmd("subvolumegroup", "rm", self.volname, group)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOTEMPTY, "invalid error code on deleting "
                             "subvolumegroup when it is not empty")
        else:
            self.fail("expected the 'fs subvolumegroup rm' command to fail")
        
        # delete subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--group_name", group)

        # delete subvolumegroup
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()


class TestSubvolumes(TestVolumesHelper):
    """Tests for FS subvolume operations, except snapshot and snapshot clone."""
    def test_async_subvolume_rm(self):
        subvolumes = self._generate_random_subvolume_name(100)

        # create subvolumes
        for subvolume in subvolumes:
            self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")
            self._do_subvolume_io(subvolume, number_of_files=10)

        self.mount_a.umount_wait()

        # remove subvolumes
        for subvolume in subvolumes:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        self.mount_a.mount_wait()

        # verify trash dir is clean
        self._wait_for_trash_empty(timeout=300)

    def test_default_uid_gid_subvolume(self):
        subvolume = self._generate_random_subvolume_name()
        expected_uid = 0
        expected_gid = 0

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)
        subvol_path = self._get_subvolume_path(self.volname, subvolume)

        # check subvolume's uid and gid
        stat = self.mount_a.stat(subvol_path)
        self.assertEqual(stat['st_uid'], expected_uid)
        self.assertEqual(stat['st_gid'], expected_gid)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_nonexistent_subvolume_rm(self):
        # remove non-existing subvolume
        subvolume = "non_existent_subvolume"

        # try, remove subvolume
        try:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOENT:
                raise
        else:
            raise RuntimeError("expected the 'fs subvolume rm' command to fail")

    def test_subvolume_create_and_rm(self):
        # create subvolume
        subvolume = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # make sure it exists
        subvolpath = self._fs_cmd("subvolume", "getpath", self.volname, subvolume)
        self.assertNotEqual(subvolpath, None)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        # make sure its gone
        try:
            self._fs_cmd("subvolume", "getpath", self.volname, subvolume)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOENT:
                raise
        else:
            raise RuntimeError("expected the 'fs subvolume getpath' command to fail. Subvolume not removed.")

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_create_and_rm_in_group(self):
        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_create_idempotence(self):
        # create subvolume
        subvolume = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # try creating w/ same subvolume name -- should be idempotent
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_create_idempotence_resize(self):
        # create subvolume
        subvolume = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # try creating w/ same subvolume name with size -- should set quota
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "1000000000")

        # get subvolume metadata
        subvol_info = json.loads(self._get_subvolume_info(self.volname, subvolume))
        self.assertEqual(subvol_info["bytes_quota"], 1000000000)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_create_idempotence_mode(self):
        # default mode
        default_mode = "755"

        # create subvolume
        subvolume = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        subvol_path = self._get_subvolume_path(self.volname, subvolume)

        actual_mode_1 = self.mount_a.run_shell(['stat', '-c' '%a', subvol_path]).stdout.getvalue().strip()
        self.assertEqual(actual_mode_1, default_mode)

        # try creating w/ same subvolume name with --mode 777
        new_mode = "777"
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode", new_mode)

        actual_mode_2 = self.mount_a.run_shell(['stat', '-c' '%a', subvol_path]).stdout.getvalue().strip()
        self.assertEqual(actual_mode_2, new_mode)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_create_idempotence_without_passing_mode(self):
        # create subvolume
        desired_mode = "777"
        subvolume = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode", desired_mode)

        subvol_path = self._get_subvolume_path(self.volname, subvolume)

        actual_mode_1 = self.mount_a.run_shell(['stat', '-c' '%a', subvol_path]).stdout.getvalue().strip()
        self.assertEqual(actual_mode_1, desired_mode)

        # default mode
        default_mode = "755"

        # try creating w/ same subvolume name without passing --mode argument
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        actual_mode_2 = self.mount_a.run_shell(['stat', '-c' '%a', subvol_path]).stdout.getvalue().strip()
        self.assertEqual(actual_mode_2, default_mode)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_create_isolated_namespace(self):
        """
        Create subvolume in separate rados namespace
        """

        # create subvolume
        subvolume = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--namespace-isolated")

        # get subvolume metadata
        subvol_info = json.loads(self._get_subvolume_info(self.volname, subvolume))
        self.assertNotEqual(len(subvol_info), 0)
        self.assertEqual(subvol_info["pool_namespace"], "fsvolumens_" + subvolume)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_create_with_auto_cleanup_on_fail(self):
        subvolume = self._generate_random_subvolume_name()
        data_pool = "invalid_pool"
        # create subvolume with invalid data pool layout fails
        with self.assertRaises(CommandFailedError):
            self._fs_cmd("subvolume", "create", self.volname, subvolume, "--pool_layout", data_pool)

        # check whether subvol path is cleaned up
        try:
            self._fs_cmd("subvolume", "getpath", self.volname, subvolume)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOENT, "invalid error code on getpath of non-existent subvolume")
        else:
            self.fail("expected the 'fs subvolume getpath' command to fail")

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_create_with_desired_data_pool_layout_in_group(self):
        subvol1, subvol2 = self._generate_random_subvolume_name(2)
        group = self._generate_random_group_name()

        # create group. this also helps set default pool layout for subvolumes
        # created within the group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvol1, "--group_name", group)
        subvol1_path = self._get_subvolume_path(self.volname, subvol1, group_name=group)

        default_pool = self.mount_a.getfattr(subvol1_path, "ceph.dir.layout.pool")
        new_pool = "new_pool"
        self.assertNotEqual(default_pool, new_pool)

        # add data pool
        newid = self.fs.add_data_pool(new_pool)

        # create subvolume specifying the new data pool as its pool layout
        self._fs_cmd("subvolume", "create", self.volname, subvol2, "--group_name", group,
                     "--pool_layout", new_pool)
        subvol2_path = self._get_subvolume_path(self.volname, subvol2, group_name=group)

        desired_pool = self.mount_a.getfattr(subvol2_path, "ceph.dir.layout.pool")
        try:
            self.assertEqual(desired_pool, new_pool)
        except AssertionError:
            self.assertEqual(int(desired_pool), newid) # old kernel returns id

        self._fs_cmd("subvolume", "rm", self.volname, subvol2, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvol1, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_create_with_desired_mode(self):
        subvol1 = self._generate_random_subvolume_name()

        # default mode
        default_mode = "755"
        # desired mode
        desired_mode = "777"

        self._fs_cmd("subvolume", "create", self.volname, subvol1,  "--mode", "777")

        subvol1_path = self._get_subvolume_path(self.volname, subvol1)

        # check subvolumegroup's mode
        subvol_par_path = os.path.dirname(subvol1_path)
        group_path = os.path.dirname(subvol_par_path)
        actual_mode1 = self.mount_a.run_shell(['stat', '-c' '%a', group_path]).stdout.getvalue().strip()
        self.assertEqual(actual_mode1, default_mode)
        # check /volumes mode
        volumes_path = os.path.dirname(group_path)
        actual_mode2 = self.mount_a.run_shell(['stat', '-c' '%a', volumes_path]).stdout.getvalue().strip()
        self.assertEqual(actual_mode2, default_mode)
        # check subvolume's  mode
        actual_mode3 = self.mount_a.run_shell(['stat', '-c' '%a', subvol1_path]).stdout.getvalue().strip()
        self.assertEqual(actual_mode3, desired_mode)

        self._fs_cmd("subvolume", "rm", self.volname, subvol1)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_create_with_desired_mode_in_group(self):
        subvol1, subvol2, subvol3 = self._generate_random_subvolume_name(3)

        group = self._generate_random_group_name()
        # default mode
        expected_mode1 = "755"
        # desired mode
        expected_mode2 = "777"

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvol1, "--group_name", group)
        self._fs_cmd("subvolume", "create", self.volname, subvol2, "--group_name", group, "--mode", "777")
        # check whether mode 0777 also works
        self._fs_cmd("subvolume", "create", self.volname, subvol3, "--group_name", group, "--mode", "0777")

        subvol1_path = self._get_subvolume_path(self.volname, subvol1, group_name=group)
        subvol2_path = self._get_subvolume_path(self.volname, subvol2, group_name=group)
        subvol3_path = self._get_subvolume_path(self.volname, subvol3, group_name=group)

        # check subvolume's  mode
        actual_mode1 = self.mount_a.run_shell(['stat', '-c' '%a', subvol1_path]).stdout.getvalue().strip()
        actual_mode2 = self.mount_a.run_shell(['stat', '-c' '%a', subvol2_path]).stdout.getvalue().strip()
        actual_mode3 = self.mount_a.run_shell(['stat', '-c' '%a', subvol3_path]).stdout.getvalue().strip()
        self.assertEqual(actual_mode1, expected_mode1)
        self.assertEqual(actual_mode2, expected_mode2)
        self.assertEqual(actual_mode3, expected_mode2)

        self._fs_cmd("subvolume", "rm", self.volname, subvol1, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvol2, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvol3, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_create_with_desired_uid_gid(self):
        """
        That the subvolume can be created with the desired uid and gid and its uid and gid matches the
        expected values.
        """
        uid = 1000
        gid = 1000

        # create subvolume
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--uid", str(uid), "--gid", str(gid))

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname)
        self.assertNotEqual(subvolpath, None)

        # verify the uid and gid
        suid = int(self.mount_a.run_shell(['stat', '-c' '%u', subvolpath]).stdout.getvalue().strip())
        sgid = int(self.mount_a.run_shell(['stat', '-c' '%g', subvolpath]).stdout.getvalue().strip())
        self.assertEqual(uid, suid)
        self.assertEqual(gid, sgid)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolname)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_create_with_invalid_data_pool_layout(self):
        subvolume = self._generate_random_subvolume_name()
        data_pool = "invalid_pool"
        # create subvolume with invalid data pool layout
        try:
            self._fs_cmd("subvolume", "create", self.volname, subvolume, "--pool_layout", data_pool)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL, "invalid error code on create of subvolume with invalid pool layout")
        else:
            self.fail("expected the 'fs subvolume create' command to fail")

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_create_with_invalid_size(self):
        # create subvolume with an invalid size -1
        subvolume = self._generate_random_subvolume_name()
        try:
            self._fs_cmd("subvolume", "create", self.volname, subvolume, "--size", "-1")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL, "invalid error code on create of subvolume with invalid size")
        else:
            self.fail("expected the 'fs subvolume create' command to fail")

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_create_and_ls_providing_group_as_nogroup(self):
        """
        That a 'subvolume create' and 'subvolume ls' should throw
        permission denied error if option --group=_nogroup is provided.
        """

        subvolname = self._generate_random_subvolume_name()

        # try to create subvolume providing --group_name=_nogroup option
        try:
            self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", "_nogroup")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EPERM)
        else:
            self.fail("expected the 'fs subvolume create' command to fail")

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolname)

        # try to list subvolumes providing --group_name=_nogroup option
        try:
            self._fs_cmd("subvolume", "ls", self.volname, "--group_name", "_nogroup")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EPERM)
        else:
            self.fail("expected the 'fs subvolume ls' command to fail")

        # list subvolumes
        self._fs_cmd("subvolume", "ls", self.volname)

        self._fs_cmd("subvolume", "rm", self.volname, subvolname)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_expand(self):
        """
        That a subvolume can be expanded in size and its quota matches the expected size.
        """

        # create subvolume
        subvolname = self._generate_random_subvolume_name()
        osize = self.DEFAULT_FILE_SIZE*1024*1024
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--size", str(osize))

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname)
        self.assertNotEqual(subvolpath, None)

        # expand the subvolume
        nsize = osize*2
        self._fs_cmd("subvolume", "resize", self.volname, subvolname, str(nsize))

        # verify the quota
        size = int(self.mount_a.getfattr(subvolpath, "ceph.quota.max_bytes"))
        self.assertEqual(size, nsize)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolname)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_info(self):
        # tests the 'fs subvolume info' command

        subvol_md = ["atime", "bytes_pcent", "bytes_quota", "bytes_used", "created_at", "ctime",
                     "data_pool", "gid", "mode", "mon_addrs", "mtime", "path", "pool_namespace",
                     "type", "uid", "features", "state"]

        # create subvolume
        subvolume = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # get subvolume metadata
        subvol_info = json.loads(self._get_subvolume_info(self.volname, subvolume))
        for md in subvol_md:
            self.assertIn(md, subvol_info, "'{0}' key not present in metadata of subvolume".format(md))

        self.assertEqual(subvol_info["bytes_pcent"], "undefined", "bytes_pcent should be set to undefined if quota is not set")
        self.assertEqual(subvol_info["bytes_quota"], "infinite", "bytes_quota should be set to infinite if quota is not set")
        self.assertEqual(subvol_info["pool_namespace"], "", "expected pool namespace to be empty")
        self.assertEqual(subvol_info["state"], "complete", "expected state to be complete")

        self.assertEqual(len(subvol_info["features"]), 3,
                         msg="expected 3 features, found '{0}' ({1})".format(len(subvol_info["features"]), subvol_info["features"]))
        for feature in ['snapshot-clone', 'snapshot-autoprotect', 'snapshot-retention']:
            self.assertIn(feature, subvol_info["features"], msg="expected feature '{0}' in subvolume".format(feature))

        nsize = self.DEFAULT_FILE_SIZE*1024*1024
        self._fs_cmd("subvolume", "resize", self.volname, subvolume, str(nsize))

        # get subvolume metadata after quota set
        subvol_info = json.loads(self._get_subvolume_info(self.volname, subvolume))
        for md in subvol_md:
            self.assertIn(md, subvol_info, "'{0}' key not present in metadata of subvolume".format(md))

        self.assertNotEqual(subvol_info["bytes_pcent"], "undefined", "bytes_pcent should not be set to undefined if quota is not set")
        self.assertEqual(subvol_info["bytes_quota"], nsize, "bytes_quota should be set to '{0}'".format(nsize))
        self.assertEqual(subvol_info["type"], "subvolume", "type should be set to subvolume")
        self.assertEqual(subvol_info["state"], "complete", "expected state to be complete")

        self.assertEqual(len(subvol_info["features"]), 3,
                         msg="expected 3 features, found '{0}' ({1})".format(len(subvol_info["features"]), subvol_info["features"]))
        for feature in ['snapshot-clone', 'snapshot-autoprotect', 'snapshot-retention']:
            self.assertIn(feature, subvol_info["features"], msg="expected feature '{0}' in subvolume".format(feature))

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_ls(self):
        # tests the 'fs subvolume ls' command

        subvolumes = []

        # create subvolumes
        subvolumes = self._generate_random_subvolume_name(3)
        for subvolume in subvolumes:
            self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # list subvolumes
        subvolumels = json.loads(self._fs_cmd('subvolume', 'ls', self.volname))
        if len(subvolumels) == 0:
            self.fail("Expected the 'fs subvolume ls' command to list the created subvolumes.")
        else:
            subvolnames = [subvolume['name'] for subvolume in subvolumels]
            if collections.Counter(subvolnames) != collections.Counter(subvolumes):
                self.fail("Error creating or listing subvolumes")

        # remove subvolume
        for subvolume in subvolumes:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_ls_with_groupname_as_internal_directory(self):
        # tests the 'fs subvolume ls' command when the default groupname as internal directories
        # Eg: '_nogroup', '_legacy', '_deleting', '_index'.
        # Expecting 'fs subvolume ls' will be fail with errno EINVAL for '_legacy', '_deleting', '_index'
        # Expecting 'fs subvolume ls' will be fail with errno EPERM for '_nogroup'

        # try to list subvolumes providing --group_name=_nogroup option
        try:
            self._fs_cmd("subvolume", "ls", self.volname, "--group_name", "_nogroup")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EPERM)
        else:
            self.fail("expected the 'fs subvolume ls' command to fail with error 'EPERM' for _nogroup")

        # try to list subvolumes providing --group_name=_legacy option
        try:
            self._fs_cmd("subvolume", "ls", self.volname, "--group_name", "_legacy")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL)
        else:
            self.fail("expected the 'fs subvolume ls' command to fail with error 'EINVAL' for _legacy")

        # try to list subvolumes providing --group_name=_deleting option
        try:
            self._fs_cmd("subvolume", "ls", self.volname, "--group_name", "_deleting")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL)
        else:
            self.fail("expected the 'fs subvolume ls' command to fail with error 'EINVAL' for _deleting")

        # try to list subvolumes providing --group_name=_index option
        try:
            self._fs_cmd("subvolume", "ls", self.volname, "--group_name", "_index")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL)
        else:
            self.fail("expected the 'fs subvolume ls' command to fail with error 'EINVAL' for _index")

    def test_subvolume_ls_for_notexistent_default_group(self):
        # tests the 'fs subvolume ls' command when the default group '_nogroup' doesn't exist
        # prerequisite: we expect that the volume is created and the default group _nogroup is
        # NOT created (i.e. a subvolume without group is not created)

        # list subvolumes
        subvolumels = json.loads(self._fs_cmd('subvolume', 'ls', self.volname))
        if len(subvolumels) > 0:
            raise RuntimeError("Expected the 'fs subvolume ls' command to output an empty list.")

    def test_subvolume_marked(self):
        """
        ensure a subvolume is marked with the ceph.dir.subvolume xattr
        """
        subvolume = self._generate_random_subvolume_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # getpath
        subvolpath = self._get_subvolume_path(self.volname, subvolume)

        # subdirectory of a subvolume cannot be moved outside the subvolume once marked with
        # the xattr ceph.dir.subvolume, hence test by attempting to rename subvol path (incarnation)
        # outside the subvolume
        dstpath = os.path.join(self.mount_a.mountpoint, 'volumes', '_nogroup', 'new_subvol_location')
        srcpath = os.path.join(self.mount_a.mountpoint, subvolpath)
        rename_script = dedent("""
            import os
            import errno
            try:
                os.rename("{src}", "{dst}")
            except OSError as e:
                if e.errno != errno.EXDEV:
                    raise RuntimeError("invalid error code on renaming subvolume incarnation out of subvolume directory")
            else:
                raise RuntimeError("expected renaming subvolume incarnation out of subvolume directory to fail")
            """)
        self.mount_a.run_python(rename_script.format(src=srcpath, dst=dstpath), sudo=True)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_pin_export(self):
        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()

        subvolume = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume)
        self._fs_cmd("subvolume", "pin", self.volname, subvolume, "export", "1")
        path = self._fs_cmd("subvolume", "getpath", self.volname, subvolume)
        path = os.path.dirname(path) # get subvolume path

        self._get_subtrees(status=status, rank=1)
        self._wait_subtrees([(path, 1)], status=status)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    ### authorize operations

    def test_authorize_deauthorize_legacy_subvolume(self):
        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        authid = "alice"

        guest_mount = self.mount_b
        guest_mount.umount_wait()

        # emulate a old-fashioned subvolume in a custom group
        createpath = os.path.join(".", "volumes", group, subvolume)
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', createpath], omit_sudo=False)

        # add required xattrs to subvolume
        default_pool = self.mount_a.getfattr(".", "ceph.dir.layout.pool")
        self.mount_a.setfattr(createpath, 'ceph.dir.layout.pool', default_pool, sudo=True)

        mount_path = os.path.join("/", "volumes", group, subvolume)

        # authorize guest authID read-write access to subvolume
        key = self._fs_cmd("subvolume", "authorize", self.volname, subvolume, authid,
                           "--group_name", group, "--tenant_id", "tenant_id")

        # guest authID should exist
        existing_ids = [a['entity'] for a in self.auth_list()]
        self.assertIn("client.{0}".format(authid), existing_ids)

        # configure credentials for guest client
        self._configure_guest_auth(guest_mount, authid, key)

        # mount the subvolume, and write to it
        guest_mount.mount_wait(cephfs_mntpt=mount_path)
        guest_mount.write_n_mb("data.bin", 1)

        # authorize guest authID read access to subvolume
        key = self._fs_cmd("subvolume", "authorize", self.volname, subvolume, authid,
                           "--group_name", group, "--tenant_id", "tenant_id", "--access_level", "r")

        # guest client sees the change in access level to read only after a
        # remount of the subvolume.
        guest_mount.umount_wait()
        guest_mount.mount_wait(cephfs_mntpt=mount_path)

        # read existing content of the subvolume
        self.assertListEqual(guest_mount.ls(guest_mount.mountpoint), ["data.bin"])
        # cannot write into read-only subvolume
        with self.assertRaises(CommandFailedError):
            guest_mount.write_n_mb("rogue.bin", 1)

        # cleanup
        guest_mount.umount_wait()
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume, authid,
                     "--group_name", group)
        # guest authID should no longer exist
        existing_ids = [a['entity'] for a in self.auth_list()]
        self.assertNotIn("client.{0}".format(authid), existing_ids)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_authorize_deauthorize_subvolume(self):
        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        authid = "alice"

        guest_mount = self.mount_b
        guest_mount.umount_wait()

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group, "--mode=777")

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)
        mount_path = self._fs_cmd("subvolume", "getpath", self.volname, subvolume,
                                  "--group_name", group).rstrip()

        # authorize guest authID read-write access to subvolume
        key = self._fs_cmd("subvolume", "authorize", self.volname, subvolume, authid,
                           "--group_name", group, "--tenant_id", "tenant_id")

        # guest authID should exist
        existing_ids = [a['entity'] for a in self.auth_list()]
        self.assertIn("client.{0}".format(authid), existing_ids)

        # configure credentials for guest client
        self._configure_guest_auth(guest_mount, authid, key)

        # mount the subvolume, and write to it
        guest_mount.mount_wait(cephfs_mntpt=mount_path)
        guest_mount.write_n_mb("data.bin", 1)

        # authorize guest authID read access to subvolume
        key = self._fs_cmd("subvolume", "authorize", self.volname, subvolume, authid,
                           "--group_name", group, "--tenant_id", "tenant_id", "--access_level", "r")

        # guest client sees the change in access level to read only after a
        # remount of the subvolume.
        guest_mount.umount_wait()
        guest_mount.mount_wait(cephfs_mntpt=mount_path)

        # read existing content of the subvolume
        self.assertListEqual(guest_mount.ls(guest_mount.mountpoint), ["data.bin"])
        # cannot write into read-only subvolume
        with self.assertRaises(CommandFailedError):
            guest_mount.write_n_mb("rogue.bin", 1)

        # cleanup
        guest_mount.umount_wait()
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume, authid,
                     "--group_name", group)
        # guest authID should no longer exist
        existing_ids = [a['entity'] for a in self.auth_list()]
        self.assertNotIn("client.{0}".format(authid), existing_ids)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_multitenant_subvolumes(self):
        """
        That subvolume access can be restricted to a tenant.

        That metadata used to enforce tenant isolation of
        subvolumes is stored as a two-way mapping between auth
        IDs and subvolumes that they're authorized to access.
        """
        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        guest_mount = self.mount_b

        # Guest clients belonging to different tenants, but using the same
        # auth ID.
        auth_id = "alice"
        guestclient_1 = {
            "auth_id": auth_id,
            "tenant_id": "tenant1",
        }
        guestclient_2 = {
            "auth_id": auth_id,
            "tenant_id": "tenant2",
        }

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # Check that subvolume metadata file is created on subvolume creation.
        subvol_metadata_filename = "_{0}:{1}.meta".format(group, subvolume)
        self.assertIn(subvol_metadata_filename, guest_mount.ls("volumes"))

        # Authorize 'guestclient_1', using auth ID 'alice' and belonging to
        # 'tenant1', with 'rw' access to the volume.
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume, guestclient_1["auth_id"],
                     "--group_name", group, "--tenant_id", guestclient_1["tenant_id"])

        # Check that auth metadata file for auth ID 'alice', is
        # created on authorizing 'alice' access to the subvolume.
        auth_metadata_filename = "${0}.meta".format(guestclient_1["auth_id"])
        self.assertIn(auth_metadata_filename, guest_mount.ls("volumes"))

        # Verify that the auth metadata file stores the tenant ID that the
        # auth ID belongs to, the auth ID's authorized access levels
        # for different subvolumes, versioning details, etc.
        expected_auth_metadata = {
            "version": 5,
            "compat_version": 6,
            "dirty": False,
            "tenant_id": "tenant1",
            "subvolumes": {
                "{0}/{1}".format(group,subvolume): {
                    "dirty": False,
                    "access_level": "rw"
                }
            }
        }

        auth_metadata = self._auth_metadata_get(guest_mount.read_file("volumes/{0}".format(auth_metadata_filename)))
        self.assertGreaterEqual(auth_metadata["version"], expected_auth_metadata["version"])
        del expected_auth_metadata["version"]
        del auth_metadata["version"]
        self.assertEqual(expected_auth_metadata, auth_metadata)

        # Verify that the subvolume metadata file stores info about auth IDs
        # and their access levels to the subvolume, versioning details, etc.
        expected_subvol_metadata = {
            "version": 1,
            "compat_version": 1,
            "auths": {
                "alice": {
                    "dirty": False,
                    "access_level": "rw"
                }
            }
        }
        subvol_metadata = self._auth_metadata_get(guest_mount.read_file("volumes/{0}".format(subvol_metadata_filename)))

        self.assertGreaterEqual(subvol_metadata["version"], expected_subvol_metadata["version"])
        del expected_subvol_metadata["version"]
        del subvol_metadata["version"]
        self.assertEqual(expected_subvol_metadata, subvol_metadata)

        # Cannot authorize 'guestclient_2' to access the volume.
        # It uses auth ID 'alice', which has already been used by a
        # 'guestclient_1' belonging to an another tenant for accessing
        # the volume.

        try:
            self._fs_cmd("subvolume", "authorize", self.volname, subvolume, guestclient_2["auth_id"],
                         "--group_name", group, "--tenant_id", guestclient_2["tenant_id"])
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EPERM,
                             "Invalid error code returned on authorize of subvolume with same auth_id but different tenant_id")
        else:
            self.fail("expected the 'fs subvolume authorize' command to fail")

        # Check that auth metadata file is cleaned up on removing
        # auth ID's only access to a volume.

        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume, auth_id,
                     "--group_name", group)
        self.assertNotIn(auth_metadata_filename, guest_mount.ls("volumes"))

        # Check that subvolume metadata file is cleaned up on subvolume deletion.
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--group_name", group)
        self.assertNotIn(subvol_metadata_filename, guest_mount.ls("volumes"))

        # clean up
        guest_mount.umount_wait()
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_authorized_list(self):
        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        authid1 = "alice"
        authid2 = "guest1"
        authid3 = "guest2"

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # authorize alice authID read-write access to subvolume
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume, authid1,
                     "--group_name", group)
        # authorize guest1 authID read-write access to subvolume
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume, authid2,
                     "--group_name", group)
        # authorize guest2 authID read access to subvolume
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume, authid3,
                     "--group_name", group, "--access_level", "r")

        # list authorized-ids of the subvolume
        expected_auth_list = [{'alice': 'rw'}, {'guest1': 'rw'}, {'guest2': 'r'}]
        auth_list = json.loads(self._fs_cmd('subvolume', 'authorized_list', self.volname, subvolume, "--group_name", group))
        self.assertCountEqual(expected_auth_list, auth_list)

        # cleanup
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume, authid1,
                     "--group_name", group)
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume, authid2,
                     "--group_name", group)
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume, authid3,
                     "--group_name", group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_authorize_auth_id_not_created_by_mgr_volumes(self):
        """
        If the auth_id already exists and is not created by mgr plugin,
        it's not allowed to authorize the auth-id by default.
        """

        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # Create auth_id
        self.fs.mon_manager.raw_cluster_cmd(
            "auth", "get-or-create", "client.guest1",
            "mds", "allow *",
            "osd", "allow rw",
            "mon", "allow *"
        )

        auth_id = "guest1"
        guestclient_1 = {
            "auth_id": auth_id,
            "tenant_id": "tenant1",
        }

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        try:
            self._fs_cmd("subvolume", "authorize", self.volname, subvolume, guestclient_1["auth_id"],
                         "--group_name", group, "--tenant_id", guestclient_1["tenant_id"])
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EPERM,
                             "Invalid error code returned on authorize of subvolume for auth_id created out of band")
        else:
            self.fail("expected the 'fs subvolume authorize' command to fail")

        # clean up
        self.fs.mon_manager.raw_cluster_cmd("auth", "rm", "client.guest1")
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_authorize_allow_existing_id_option(self):
        """
        If the auth_id already exists and is not created by mgr volumes,
        it's not allowed to authorize the auth-id by default but is
        allowed with option allow_existing_id.
        """

        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # Create auth_id
        self.fs.mon_manager.raw_cluster_cmd(
            "auth", "get-or-create", "client.guest1",
            "mds", "allow *",
            "osd", "allow rw",
            "mon", "allow *"
        )

        auth_id = "guest1"
        guestclient_1 = {
            "auth_id": auth_id,
            "tenant_id": "tenant1",
        }

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # Cannot authorize 'guestclient_1' to access the volume by default,
        # which already exists and not created by mgr volumes but is allowed
        # with option 'allow_existing_id'.
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume, guestclient_1["auth_id"],
                     "--group_name", group, "--tenant_id", guestclient_1["tenant_id"], "--allow-existing-id")

        # clean up
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume, auth_id,
                     "--group_name", group)
        self.fs.mon_manager.raw_cluster_cmd("auth", "rm", "client.guest1")
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_deauthorize_auth_id_after_out_of_band_update(self):
        """
        If the auth_id authorized by mgr/volumes plugin is updated
        out of band, the auth_id should not be deleted after a
        deauthorize. It should only remove caps associated with it.
        """

        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        auth_id = "guest1"
        guestclient_1 = {
            "auth_id": auth_id,
            "tenant_id": "tenant1",
        }

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # Authorize 'guestclient_1' to access the subvolume.
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume, guestclient_1["auth_id"],
                     "--group_name", group, "--tenant_id", guestclient_1["tenant_id"])

        subvol_path = self._fs_cmd("subvolume", "getpath", self.volname, subvolume,
                                  "--group_name", group).rstrip()

        # Update caps for guestclient_1 out of band
        out = self.fs.mon_manager.raw_cluster_cmd(
            "auth", "caps", "client.guest1",
            "mds", "allow rw path=/volumes/{0}, allow rw path={1}".format(group, subvol_path),
            "osd", "allow rw pool=cephfs_data",
            "mon", "allow r",
            "mgr", "allow *"
        )

        # Deauthorize guestclient_1
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume, auth_id, "--group_name", group)

        # Validate the caps of guestclient_1 after deauthorize. It should not have deleted
        # guestclient_1. The mgr and mds caps should be present which was updated out of band.
        out = json.loads(self.fs.mon_manager.raw_cluster_cmd("auth", "get", "client.guest1", "--format=json-pretty"))

        self.assertEqual("client.guest1", out[0]["entity"])
        self.assertEqual("allow rw path=/volumes/{0}".format(group), out[0]["caps"]["mds"])
        self.assertEqual("allow *", out[0]["caps"]["mgr"])
        self.assertNotIn("osd", out[0]["caps"])

        # clean up
        out = self.fs.mon_manager.raw_cluster_cmd("auth", "rm", "client.guest1")
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_recover_auth_metadata_during_authorize(self):
        """
        That auth metadata manager can recover from partial auth updates using
        metadata files, which store auth info and its update status info. This
        test validates the recovery during authorize.
        """

        guest_mount = self.mount_b

        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        auth_id = "guest1"
        guestclient_1 = {
            "auth_id": auth_id,
            "tenant_id": "tenant1",
        }

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # Authorize 'guestclient_1' to access the subvolume.
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume, guestclient_1["auth_id"],
                     "--group_name", group, "--tenant_id", guestclient_1["tenant_id"])

        # Check that auth metadata file for auth ID 'guest1', is
        # created on authorizing 'guest1' access to the subvolume.
        auth_metadata_filename = "${0}.meta".format(guestclient_1["auth_id"])
        self.assertIn(auth_metadata_filename, guest_mount.ls("volumes"))
        expected_auth_metadata_content = self._auth_metadata_get(self.mount_a.read_file("volumes/{0}".format(auth_metadata_filename)))

        # Induce partial auth update state by modifying the auth metadata file,
        # and then run authorize again.
        guest_mount.run_shell(['sudo', 'sed', '-i', 's/false/true/g', 'volumes/{0}'.format(auth_metadata_filename)], omit_sudo=False)

        # Authorize 'guestclient_1' to access the subvolume.
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume, guestclient_1["auth_id"],
                     "--group_name", group, "--tenant_id", guestclient_1["tenant_id"])

        auth_metadata_content = self._auth_metadata_get(self.mount_a.read_file("volumes/{0}".format(auth_metadata_filename)))
        self.assertEqual(auth_metadata_content, expected_auth_metadata_content)

        # clean up
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume, auth_id, "--group_name", group)
        guest_mount.umount_wait()
        self.fs.mon_manager.raw_cluster_cmd("auth", "rm", "client.guest1")
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_recover_auth_metadata_during_deauthorize(self):
        """
        That auth metadata manager can recover from partial auth updates using
        metadata files, which store auth info and its update status info. This
        test validates the recovery during deauthorize.
        """

        guest_mount = self.mount_b

        subvolume1, subvolume2 = self._generate_random_subvolume_name(2)
        group = self._generate_random_group_name()

        guestclient_1 = {
            "auth_id": "guest1",
            "tenant_id": "tenant1",
        }

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolumes in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume1, "--group_name", group)
        self._fs_cmd("subvolume", "create", self.volname, subvolume2, "--group_name", group)

        # Authorize 'guestclient_1' to access the subvolume1.
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume1, guestclient_1["auth_id"],
                     "--group_name", group, "--tenant_id", guestclient_1["tenant_id"])

        # Check that auth metadata file for auth ID 'guest1', is
        # created on authorizing 'guest1' access to the subvolume1.
        auth_metadata_filename = "${0}.meta".format(guestclient_1["auth_id"])
        self.assertIn(auth_metadata_filename, guest_mount.ls("volumes"))
        expected_auth_metadata_content = self._auth_metadata_get(self.mount_a.read_file("volumes/{0}".format(auth_metadata_filename)))

        # Authorize 'guestclient_1' to access the subvolume2.
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume2, guestclient_1["auth_id"],
                     "--group_name", group, "--tenant_id", guestclient_1["tenant_id"])

        # Induce partial auth update state by modifying the auth metadata file,
        # and then run de-authorize.
        guest_mount.run_shell(['sudo', 'sed', '-i', 's/false/true/g', 'volumes/{0}'.format(auth_metadata_filename)], omit_sudo=False)

        # Deauthorize 'guestclient_1' to access the subvolume2.
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume2, guestclient_1["auth_id"],
                     "--group_name", group)

        auth_metadata_content = self._auth_metadata_get(self.mount_a.read_file("volumes/{0}".format(auth_metadata_filename)))
        self.assertEqual(auth_metadata_content, expected_auth_metadata_content)

        # clean up
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume1, "guest1", "--group_name", group)
        guest_mount.umount_wait()
        self.fs.mon_manager.raw_cluster_cmd("auth", "rm", "client.guest1")
        self._fs_cmd("subvolume", "rm", self.volname, subvolume1, "--group_name", group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume2, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_update_old_style_auth_metadata_to_new_during_authorize(self):
        """
        CephVolumeClient stores the subvolume data in auth metadata file with
        'volumes' key as there was no subvolume namespace. It doesn't makes sense
        with mgr/volumes. This test validates the transparent update of 'volumes'
        key to 'subvolumes' key in auth metadata file during authorize.
        """

        guest_mount = self.mount_b

        subvolume1, subvolume2 = self._generate_random_subvolume_name(2)
        group = self._generate_random_group_name()

        auth_id = "guest1"
        guestclient_1 = {
            "auth_id": auth_id,
            "tenant_id": "tenant1",
        }

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolumes in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume1, "--group_name", group)
        self._fs_cmd("subvolume", "create", self.volname, subvolume2, "--group_name", group)

        # Authorize 'guestclient_1' to access the subvolume1.
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume1, guestclient_1["auth_id"],
                     "--group_name", group, "--tenant_id", guestclient_1["tenant_id"])

        # Check that auth metadata file for auth ID 'guest1', is
        # created on authorizing 'guest1' access to the subvolume1.
        auth_metadata_filename = "${0}.meta".format(guestclient_1["auth_id"])
        self.assertIn(auth_metadata_filename, guest_mount.ls("volumes"))

        # Replace 'subvolumes' to 'volumes', old style auth-metadata file
        guest_mount.run_shell(['sudo', 'sed', '-i', 's/subvolumes/volumes/g', 'volumes/{0}'.format(auth_metadata_filename)], omit_sudo=False)

        # Authorize 'guestclient_1' to access the subvolume2. This should transparently update 'volumes' to 'subvolumes'
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume2, guestclient_1["auth_id"],
                     "--group_name", group, "--tenant_id", guestclient_1["tenant_id"])

        expected_auth_metadata = {
            "version": 5,
            "compat_version": 6,
            "dirty": False,
            "tenant_id": "tenant1",
            "subvolumes": {
                "{0}/{1}".format(group,subvolume1): {
                    "dirty": False,
                    "access_level": "rw"
                },
                "{0}/{1}".format(group,subvolume2): {
                    "dirty": False,
                    "access_level": "rw"
                }
            }
        }

        auth_metadata = self._auth_metadata_get(guest_mount.read_file("volumes/{0}".format(auth_metadata_filename)))

        self.assertGreaterEqual(auth_metadata["version"], expected_auth_metadata["version"])
        del expected_auth_metadata["version"]
        del auth_metadata["version"]
        self.assertEqual(expected_auth_metadata, auth_metadata)

        # clean up
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume1, auth_id, "--group_name", group)
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume2, auth_id, "--group_name", group)
        guest_mount.umount_wait()
        self.fs.mon_manager.raw_cluster_cmd("auth", "rm", "client.guest1")
        self._fs_cmd("subvolume", "rm", self.volname, subvolume1, "--group_name", group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume2, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_update_old_style_auth_metadata_to_new_during_deauthorize(self):
        """
        CephVolumeClient stores the subvolume data in auth metadata file with
        'volumes' key as there was no subvolume namespace. It doesn't makes sense
        with mgr/volumes. This test validates the transparent update of 'volumes'
        key to 'subvolumes' key in auth metadata file during deauthorize.
        """

        guest_mount = self.mount_b

        subvolume1, subvolume2 = self._generate_random_subvolume_name(2)
        group = self._generate_random_group_name()

        auth_id = "guest1"
        guestclient_1 = {
            "auth_id": auth_id,
            "tenant_id": "tenant1",
        }

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolumes in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume1, "--group_name", group)
        self._fs_cmd("subvolume", "create", self.volname, subvolume2, "--group_name", group)

        # Authorize 'guestclient_1' to access the subvolume1.
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume1, guestclient_1["auth_id"],
                     "--group_name", group, "--tenant_id", guestclient_1["tenant_id"])

        # Authorize 'guestclient_1' to access the subvolume2.
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume2, guestclient_1["auth_id"],
                     "--group_name", group, "--tenant_id", guestclient_1["tenant_id"])

        # Check that auth metadata file for auth ID 'guest1', is created.
        auth_metadata_filename = "${0}.meta".format(guestclient_1["auth_id"])
        self.assertIn(auth_metadata_filename, guest_mount.ls("volumes"))

        # Replace 'subvolumes' to 'volumes', old style auth-metadata file
        guest_mount.run_shell(['sudo', 'sed', '-i', 's/subvolumes/volumes/g', 'volumes/{0}'.format(auth_metadata_filename)], omit_sudo=False)

        # Deauthorize 'guestclient_1' to access the subvolume2. This should update 'volumes' to subvolumes'
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume2, auth_id, "--group_name", group)

        expected_auth_metadata = {
            "version": 5,
            "compat_version": 6,
            "dirty": False,
            "tenant_id": "tenant1",
            "subvolumes": {
                "{0}/{1}".format(group,subvolume1): {
                    "dirty": False,
                    "access_level": "rw"
                }
            }
        }

        auth_metadata = self._auth_metadata_get(guest_mount.read_file("volumes/{0}".format(auth_metadata_filename)))

        self.assertGreaterEqual(auth_metadata["version"], expected_auth_metadata["version"])
        del expected_auth_metadata["version"]
        del auth_metadata["version"]
        self.assertEqual(expected_auth_metadata, auth_metadata)

        # clean up
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume1, auth_id, "--group_name", group)
        guest_mount.umount_wait()
        self.fs.mon_manager.raw_cluster_cmd("auth", "rm", "client.guest1")
        self._fs_cmd("subvolume", "rm", self.volname, subvolume1, "--group_name", group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume2, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_evict_client(self):
        """
        That a subvolume client can be evicted based on the auth ID
        """

        subvolumes = self._generate_random_subvolume_name(2)
        group = self._generate_random_group_name()

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # mounts[0] and mounts[1] would be used as guests to mount the volumes/shares.
        for i in range(0, 2):
            self.mounts[i].umount_wait()
        guest_mounts = (self.mounts[0], self.mounts[1])
        auth_id = "guest"
        guestclient_1 = {
            "auth_id": auth_id,
            "tenant_id": "tenant1",
        }

        # Create two subvolumes. Authorize 'guest' auth ID to mount the two
        # subvolumes. Mount the two subvolumes. Write data to the volumes.
        for i in range(2):
            # Create subvolume.
            self._fs_cmd("subvolume", "create", self.volname, subvolumes[i], "--group_name", group, "--mode=777")

            # authorize guest authID read-write access to subvolume
            key = self._fs_cmd("subvolume", "authorize", self.volname, subvolumes[i], guestclient_1["auth_id"],
                               "--group_name", group, "--tenant_id", guestclient_1["tenant_id"])

            mount_path = self._fs_cmd("subvolume", "getpath", self.volname, subvolumes[i],
                                      "--group_name", group).rstrip()
            # configure credentials for guest client
            self._configure_guest_auth(guest_mounts[i], auth_id, key)

            # mount the subvolume, and write to it
            guest_mounts[i].mount_wait(cephfs_mntpt=mount_path)
            guest_mounts[i].write_n_mb("data.bin", 1)

        # Evict client, guest_mounts[0], using auth ID 'guest' and has mounted
        # one volume.
        self._fs_cmd("subvolume", "evict", self.volname, subvolumes[0], auth_id, "--group_name", group)

        # Evicted guest client, guest_mounts[0], should not be able to do
        # anymore metadata ops.  It should start failing all operations
        # when it sees that its own address is in the blocklist.
        try:
            guest_mounts[0].write_n_mb("rogue.bin", 1)
        except CommandFailedError:
            pass
        else:
            raise RuntimeError("post-eviction write should have failed!")

        # The blocklisted guest client should now be unmountable
        guest_mounts[0].umount_wait()

        # Guest client, guest_mounts[1], using the same auth ID 'guest', but
        # has mounted the other volume, should be able to use its volume
        # unaffected.
        guest_mounts[1].write_n_mb("data.bin.1", 1)

        # Cleanup.
        guest_mounts[1].umount_wait()
        for i in range(2):
            self._fs_cmd("subvolume", "deauthorize", self.volname, subvolumes[i], auth_id, "--group_name", group)
            self._fs_cmd("subvolume", "rm", self.volname, subvolumes[i], "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_pin_random(self):
        self.fs.set_max_mds(2)
        self.fs.wait_for_daemons()
        self.config_set('mds', 'mds_export_ephemeral_random', True)

        subvolume = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume)
        self._fs_cmd("subvolume", "pin", self.volname, subvolume, "random", ".01")
        # no verification

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_resize_fail_invalid_size(self):
        """
        That a subvolume cannot be resized to an invalid size and the quota did not change
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024
        # create subvolume
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--size", str(osize))

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname)
        self.assertNotEqual(subvolpath, None)

        # try to resize the subvolume with an invalid size -10
        nsize = -10
        try:
            self._fs_cmd("subvolume", "resize", self.volname, subvolname, str(nsize))
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL, "invalid error code on resize of subvolume with invalid size")
        else:
            self.fail("expected the 'fs subvolume resize' command to fail")

        # verify the quota did not change
        size = int(self.mount_a.getfattr(subvolpath, "ceph.quota.max_bytes"))
        self.assertEqual(size, osize)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolname)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_resize_fail_zero_size(self):
        """
        That a subvolume cannot be resized to a zero size and the quota did not change
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024
        # create subvolume
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--size", str(osize))

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname)
        self.assertNotEqual(subvolpath, None)

        # try to resize the subvolume with size 0
        nsize = 0
        try:
            self._fs_cmd("subvolume", "resize", self.volname, subvolname, str(nsize))
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL, "invalid error code on resize of subvolume with invalid size")
        else:
            self.fail("expected the 'fs subvolume resize' command to fail")

        # verify the quota did not change
        size = int(self.mount_a.getfattr(subvolpath, "ceph.quota.max_bytes"))
        self.assertEqual(size, osize)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolname)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_resize_quota_lt_used_size(self):
        """
        That a subvolume can be resized to a size smaller than the current used size
        and the resulting quota matches the expected size.
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024*20
        # create subvolume
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--size", str(osize), "--mode=777")

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname)
        self.assertNotEqual(subvolpath, None)

        # create one file of 10MB
        file_size=self.DEFAULT_FILE_SIZE*10
        number_of_files=1
        log.debug("filling subvolume {0} with {1} file of size {2}MB".format(subvolname,
                                                                             number_of_files,
                                                                             file_size))
        filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, self.DEFAULT_NUMBER_OF_FILES+1)
        self.mount_a.write_n_mb(os.path.join(subvolpath, filename), file_size)

        usedsize = int(self.mount_a.getfattr(subvolpath, "ceph.dir.rbytes"))
        susedsize = int(self.mount_a.run_shell(['stat', '-c' '%s', subvolpath]).stdout.getvalue().strip())
        if isinstance(self.mount_a, FuseMount):
            # kclient dir does not have size==rbytes
            self.assertEqual(usedsize, susedsize)

        # shrink the subvolume
        nsize = usedsize // 2
        try:
            self._fs_cmd("subvolume", "resize", self.volname, subvolname, str(nsize))
        except CommandFailedError:
            self.fail("expected the 'fs subvolume resize' command to succeed")

        # verify the quota
        size = int(self.mount_a.getfattr(subvolpath, "ceph.quota.max_bytes"))
        self.assertEqual(size, nsize)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolname)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_resize_fail_quota_lt_used_size_no_shrink(self):
        """
        That a subvolume cannot be resized to a size smaller than the current used size
        when --no_shrink is given and the quota did not change.
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024*20
        # create subvolume
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--size", str(osize), "--mode=777")

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname)
        self.assertNotEqual(subvolpath, None)

        # create one file of 10MB
        file_size=self.DEFAULT_FILE_SIZE*10
        number_of_files=1
        log.debug("filling subvolume {0} with {1} file of size {2}MB".format(subvolname,
                                                                             number_of_files,
                                                                             file_size))
        filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, self.DEFAULT_NUMBER_OF_FILES+2)
        self.mount_a.write_n_mb(os.path.join(subvolpath, filename), file_size)

        usedsize = int(self.mount_a.getfattr(subvolpath, "ceph.dir.rbytes"))
        susedsize = int(self.mount_a.run_shell(['stat', '-c' '%s', subvolpath]).stdout.getvalue().strip())
        if isinstance(self.mount_a, FuseMount):
            # kclient dir does not have size==rbytes
            self.assertEqual(usedsize, susedsize)

        # shrink the subvolume
        nsize = usedsize // 2
        try:
            self._fs_cmd("subvolume", "resize", self.volname, subvolname, str(nsize), "--no_shrink")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL, "invalid error code on resize of subvolume with invalid size")
        else:
            self.fail("expected the 'fs subvolume resize' command to fail")

        # verify the quota did not change
        size = int(self.mount_a.getfattr(subvolpath, "ceph.quota.max_bytes"))
        self.assertEqual(size, osize)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolname)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_resize_expand_on_full_subvolume(self):
        """
        That the subvolume can be expanded from a full subvolume and future writes succeed.
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024*10
        # create subvolume of quota 10MB and make sure it exists
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--size", str(osize), "--mode=777")
        subvolpath = self._get_subvolume_path(self.volname, subvolname)
        self.assertNotEqual(subvolpath, None)

        # create one file of size 10MB and write
        file_size=self.DEFAULT_FILE_SIZE*10
        number_of_files=1
        log.debug("filling subvolume {0} with {1} file of size {2}MB".format(subvolname,
                                                                             number_of_files,
                                                                             file_size))
        filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, self.DEFAULT_NUMBER_OF_FILES+3)
        self.mount_a.write_n_mb(os.path.join(subvolpath, filename), file_size)

        # create a file of size 5MB and try write more
        file_size=file_size // 2
        number_of_files=1
        log.debug("filling subvolume {0} with {1} file of size {2}MB".format(subvolname,
                                                                             number_of_files,
                                                                             file_size))
        filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, self.DEFAULT_NUMBER_OF_FILES+4)
        try:
            self.mount_a.write_n_mb(os.path.join(subvolpath, filename), file_size)
        except CommandFailedError:
            # Not able to write. So expand the subvolume more and try writing the 5MB file again
            nsize = osize*2
            self._fs_cmd("subvolume", "resize", self.volname, subvolname, str(nsize))
            try:
                self.mount_a.write_n_mb(os.path.join(subvolpath, filename), file_size)
            except CommandFailedError:
                self.fail("expected filling subvolume {0} with {1} file of size {2}MB"
                                   "to succeed".format(subvolname, number_of_files, file_size))
        else:
            self.fail("expected filling subvolume {0} with {1} file of size {2}MB"
                               "to fail".format(subvolname, number_of_files, file_size))

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolname)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_resize_infinite_size(self):
        """
        That a subvolume can be resized to an infinite size by unsetting its quota.
        """

        # create subvolume
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--size",
                     str(self.DEFAULT_FILE_SIZE*1024*1024))

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname)
        self.assertNotEqual(subvolpath, None)

        # resize inf
        self._fs_cmd("subvolume", "resize", self.volname, subvolname, "inf")

        # verify that the quota is None
        size = self.mount_a.getfattr(subvolpath, "ceph.quota.max_bytes")
        self.assertEqual(size, None)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolname)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_resize_infinite_size_future_writes(self):
        """
        That a subvolume can be resized to an infinite size and the future writes succeed.
        """

        # create subvolume
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--size",
                     str(self.DEFAULT_FILE_SIZE*1024*1024*5), "--mode=777")

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname)
        self.assertNotEqual(subvolpath, None)

        # resize inf
        self._fs_cmd("subvolume", "resize", self.volname, subvolname, "inf")

        # verify that the quota is None
        size = self.mount_a.getfattr(subvolpath, "ceph.quota.max_bytes")
        self.assertEqual(size, None)

        # create one file of 10MB and try to write
        file_size=self.DEFAULT_FILE_SIZE*10
        number_of_files=1
        log.debug("filling subvolume {0} with {1} file of size {2}MB".format(subvolname,
                                                                             number_of_files,
                                                                             file_size))
        filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, self.DEFAULT_NUMBER_OF_FILES+5)

        try:
            self.mount_a.write_n_mb(os.path.join(subvolpath, filename), file_size)
        except CommandFailedError:
            self.fail("expected filling subvolume {0} with {1} file of size {2}MB "
                               "to succeed".format(subvolname, number_of_files, file_size))

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolname)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_rm_force(self):
        # test removing non-existing subvolume with --force
        subvolume = self._generate_random_subvolume_name()
        try:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--force")
        except CommandFailedError:
            self.fail("expected the 'fs subvolume rm --force' command to succeed")

    def test_subvolume_exists_with_subvolumegroup_and_subvolume(self):
        """Test the presence of any subvolume by specifying the name of subvolumegroup"""

        group = self._generate_random_group_name()
        subvolume1 = self._generate_random_subvolume_name()
        # create subvolumegroup
        self._fs_cmd("subvolumegroup", "create", self.volname, group)
        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume1, "--group_name", group)
        ret = self._fs_cmd("subvolume", "exist", self.volname, "--group_name", group)
        self.assertEqual(ret.strip('\n'), "subvolume exists")
        # delete subvolume in group
        self._fs_cmd("subvolume", "rm", self.volname, subvolume1, "--group_name", group)
        ret = self._fs_cmd("subvolume", "exist", self.volname, "--group_name", group)
        self.assertEqual(ret.strip('\n'), "no subvolume exists")
        # delete subvolumegroup
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_exists_with_subvolumegroup_and_no_subvolume(self):
        """Test the presence of any subvolume specifying the name
            of subvolumegroup and no subvolumes"""

        group = self._generate_random_group_name()
        # create subvolumegroup
        self._fs_cmd("subvolumegroup", "create", self.volname, group)
        ret = self._fs_cmd("subvolume", "exist", self.volname, "--group_name", group)
        self.assertEqual(ret.strip('\n'), "no subvolume exists")
        # delete subvolumegroup
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_exists_without_subvolumegroup_and_with_subvolume(self):
        """Test the presence of any subvolume without specifying the name
            of subvolumegroup"""

        subvolume1 = self._generate_random_subvolume_name()
        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume1)
        ret = self._fs_cmd("subvolume", "exist", self.volname)
        self.assertEqual(ret.strip('\n'), "subvolume exists")
        # delete subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume1)
        ret = self._fs_cmd("subvolume", "exist", self.volname)
        self.assertEqual(ret.strip('\n'), "no subvolume exists")

    def test_subvolume_exists_without_subvolumegroup_and_without_subvolume(self):
        """Test the presence of any subvolume without any subvolumegroup
            and without any subvolume"""

        ret = self._fs_cmd("subvolume", "exist", self.volname)
        self.assertEqual(ret.strip('\n'), "no subvolume exists")

    def test_subvolume_shrink(self):
        """
        That a subvolume can be shrinked in size and its quota matches the expected size.
        """

        # create subvolume
        subvolname = self._generate_random_subvolume_name()
        osize = self.DEFAULT_FILE_SIZE*1024*1024
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--size", str(osize))

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname)
        self.assertNotEqual(subvolpath, None)

        # shrink the subvolume
        nsize = osize // 2
        self._fs_cmd("subvolume", "resize", self.volname, subvolname, str(nsize))

        # verify the quota
        size = int(self.mount_a.getfattr(subvolpath, "ceph.quota.max_bytes"))
        self.assertEqual(size, nsize)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolname)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_retain_snapshot_rm_idempotency(self):
        """
        ensure subvolume deletion of a subvolume which is already deleted with retain snapshots option passes.
        After subvolume deletion with retain snapshots, the subvolume exists until the trash directory (resides inside subvolume)
        is cleaned up. The subvolume deletion issued while the trash directory is not empty, should pass and should
        not error out with EAGAIN.
        """
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=256)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # remove with snapshot retention
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--retain-snapshots")

        # remove snapshots (removes retained volume)
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolume (check idempotency)
        try:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOENT:
                self.fail(f"expected subvolume rm to pass with error: {os.strerror(ce.exitstatus)}")

        # verify trash dir is clean
        self._wait_for_trash_empty()


    def test_subvolume_user_metadata_set(self):
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", group)

        # set metadata for subvolume.
        key = "key"
        value = "value"
        try:
            self._fs_cmd("subvolume", "metadata", "set", self.volname, subvolname, key, value, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata set' command to succeed")

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_user_metadata_set_idempotence(self):
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", group)

        # set metadata for subvolume.
        key = "key"
        value = "value"
        try:
            self._fs_cmd("subvolume", "metadata", "set", self.volname, subvolname, key, value, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata set' command to succeed")

        # set same metadata again for subvolume.
        try:
            self._fs_cmd("subvolume", "metadata", "set", self.volname, subvolname, key, value, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata set' command to succeed because it is idempotent operation")

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_user_metadata_get(self):
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", group)

        # set metadata for subvolume.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "metadata", "set", self.volname, subvolname, key, value, "--group_name", group)

        # get value for specified key.
        try:
            ret = self._fs_cmd("subvolume", "metadata", "get", self.volname, subvolname, key, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata get' command to succeed")

        # remove '\n' from returned value.
        ret = ret.strip('\n')

        # match received value with expected value.
        self.assertEqual(value, ret)

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_user_metadata_get_for_nonexisting_key(self):
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", group)

        # set metadata for subvolume.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "metadata", "set", self.volname, subvolname, key, value, "--group_name", group)

        # try to get value for nonexisting key
        # Expecting ENOENT exit status because key does not exist
        try:
            self._fs_cmd("subvolume", "metadata", "get", self.volname, subvolname, "key_nonexist", "--group_name", group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because 'key_nonexist' does not exist")

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_user_metadata_get_for_nonexisting_section(self):
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", group)

        # try to get value for nonexisting key (as section does not exist)
        # Expecting ENOENT exit status because key does not exist
        try:
            self._fs_cmd("subvolume", "metadata", "get", self.volname, subvolname, "key", "--group_name", group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because section does not exist")

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_user_metadata_update(self):
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", group)

        # set metadata for subvolume.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "metadata", "set", self.volname, subvolname, key, value, "--group_name", group)

        # update metadata against key.
        new_value = "new_value"
        self._fs_cmd("subvolume", "metadata", "set", self.volname, subvolname, key, new_value, "--group_name", group)

        # get metadata for specified key of subvolume.
        try:
            ret = self._fs_cmd("subvolume", "metadata", "get", self.volname, subvolname, key, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata get' command to succeed")

        # remove '\n' from returned value.
        ret = ret.strip('\n')

        # match received value with expected value.
        self.assertEqual(new_value, ret)

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_user_metadata_list(self):
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", group)

        # set metadata for subvolume.
        input_metadata_dict =  {f'key_{i}' : f'value_{i}' for i in range(3)}

        for k, v in input_metadata_dict.items():
            self._fs_cmd("subvolume", "metadata", "set", self.volname, subvolname, k, v, "--group_name", group)

        # list metadata
        try:
            ret = self._fs_cmd("subvolume", "metadata", "ls", self.volname, subvolname, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata ls' command to succeed")

        ret_dict = json.loads(ret)

        # compare output with expected output
        self.assertDictEqual(input_metadata_dict, ret_dict)

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_user_metadata_list_if_no_metadata_set(self):
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", group)

        # list metadata
        try:
            ret = self._fs_cmd("subvolume", "metadata", "ls", self.volname, subvolname, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata ls' command to succeed")

        # remove '\n' from returned value.
        ret = ret.strip('\n')

        # compare output with expected output
        # expecting empty json/dictionary
        self.assertEqual(ret, "{}")

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_user_metadata_remove(self):
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", group)

        # set metadata for subvolume.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "metadata", "set", self.volname, subvolname, key, value, "--group_name", group)

        # remove metadata against specified key.
        try:
            self._fs_cmd("subvolume", "metadata", "rm", self.volname, subvolname, key, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata rm' command to succeed")

        # confirm key is removed by again fetching metadata
        try:
            self._fs_cmd("subvolume", "metadata", "get", self.volname, subvolname, key, "--group_name", group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because key does not exist")

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_user_metadata_remove_for_nonexisting_key(self):
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", group)

        # set metadata for subvolume.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "metadata", "set", self.volname, subvolname, key, value, "--group_name", group)

        # try to remove value for nonexisting key
        # Expecting ENOENT exit status because key does not exist
        try:
            self._fs_cmd("subvolume", "metadata", "rm", self.volname, subvolname, "key_nonexist", "--group_name", group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because 'key_nonexist' does not exist")

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_user_metadata_remove_for_nonexisting_section(self):
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", group)

        # try to remove value for nonexisting key (as section does not exist)
        # Expecting ENOENT exit status because key does not exist
        try:
            self._fs_cmd("subvolume", "metadata", "rm", self.volname, subvolname, "key", "--group_name", group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because section does not exist")

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_user_metadata_remove_force(self):
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", group)

        # set metadata for subvolume.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "metadata", "set", self.volname, subvolname, key, value, "--group_name", group)

        # remove metadata against specified key with --force option.
        try:
            self._fs_cmd("subvolume", "metadata", "rm", self.volname, subvolname, key, "--group_name", group, "--force")
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata rm' command to succeed")

        # confirm key is removed by again fetching metadata
        try:
            self._fs_cmd("subvolume", "metadata", "get", self.volname, subvolname, key, "--group_name", group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because key does not exist")

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_user_metadata_remove_force_for_nonexisting_key(self):
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", group)

        # set metadata for subvolume.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "metadata", "set", self.volname, subvolname, key, value, "--group_name", group)

        # remove metadata against specified key.
        try:
            self._fs_cmd("subvolume", "metadata", "rm", self.volname, subvolname, key, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata rm' command to succeed")

        # confirm key is removed by again fetching metadata
        try:
            self._fs_cmd("subvolume", "metadata", "get", self.volname, subvolname, key, "--group_name", group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because key does not exist")

        # again remove metadata against already removed key with --force option.
        try:
            self._fs_cmd("subvolume", "metadata", "rm", self.volname, subvolname, key, "--group_name", group, "--force")
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata rm' (with --force) command to succeed")

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_user_metadata_set_and_get_for_legacy_subvolume(self):
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # emulate a old-fashioned subvolume in a custom group
        createpath = os.path.join(".", "volumes", group, subvolname)
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', createpath], omit_sudo=False)

        # set metadata for subvolume.
        key = "key"
        value = "value"
        try:
            self._fs_cmd("subvolume", "metadata", "set", self.volname, subvolname, key, value, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata set' command to succeed")

        # get value for specified key.
        try:
            ret = self._fs_cmd("subvolume", "metadata", "get", self.volname, subvolname, key, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata get' command to succeed")

        # remove '\n' from returned value.
        ret = ret.strip('\n')

        # match received value with expected value.
        self.assertEqual(value, ret)

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_user_metadata_list_and_remove_for_legacy_subvolume(self):
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # emulate a old-fashioned subvolume in a custom group
        createpath = os.path.join(".", "volumes", group, subvolname)
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', createpath], omit_sudo=False)

        # set metadata for subvolume.
        input_metadata_dict =  {f'key_{i}' : f'value_{i}' for i in range(3)}

        for k, v in input_metadata_dict.items():
            self._fs_cmd("subvolume", "metadata", "set", self.volname, subvolname, k, v, "--group_name", group)

        # list metadata
        try:
            ret = self._fs_cmd("subvolume", "metadata", "ls", self.volname, subvolname, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata ls' command to succeed")

        ret_dict = json.loads(ret)

        # compare output with expected output
        self.assertDictEqual(input_metadata_dict, ret_dict)

        # remove metadata against specified key.
        try:
            self._fs_cmd("subvolume", "metadata", "rm", self.volname, subvolname, "key_1", "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata rm' command to succeed")

        # confirm key is removed by again fetching metadata
        try:
            self._fs_cmd("subvolume", "metadata", "get", self.volname, subvolname, "key_1", "--group_name", group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because key_1 does not exist")

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

class TestSubvolumeGroupSnapshots(TestVolumesHelper):
    """Tests for FS subvolume group snapshot operations."""
    @unittest.skip("skipping subvolumegroup snapshot tests")
    def test_nonexistent_subvolume_group_snapshot_rm(self):
        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # snapshot group
        self._fs_cmd("subvolumegroup", "snapshot", "create", self.volname, group, snapshot)

        # remove snapshot
        self._fs_cmd("subvolumegroup", "snapshot", "rm", self.volname, group, snapshot)

        # remove snapshot
        try:
            self._fs_cmd("subvolumegroup", "snapshot", "rm", self.volname, group, snapshot)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOENT:
                raise
        else:
            raise RuntimeError("expected the 'fs subvolumegroup snapshot rm' command to fail")

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    @unittest.skip("skipping subvolumegroup snapshot tests")
    def test_subvolume_group_snapshot_create_and_rm(self):
        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # snapshot group
        self._fs_cmd("subvolumegroup", "snapshot", "create", self.volname, group, snapshot)

        # remove snapshot
        self._fs_cmd("subvolumegroup", "snapshot", "rm", self.volname, group, snapshot)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    @unittest.skip("skipping subvolumegroup snapshot tests")
    def test_subvolume_group_snapshot_idempotence(self):
        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # snapshot group
        self._fs_cmd("subvolumegroup", "snapshot", "create", self.volname, group, snapshot)

        # try creating snapshot w/ same snapshot name -- shoule be idempotent
        self._fs_cmd("subvolumegroup", "snapshot", "create", self.volname, group, snapshot)

        # remove snapshot
        self._fs_cmd("subvolumegroup", "snapshot", "rm", self.volname, group, snapshot)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    @unittest.skip("skipping subvolumegroup snapshot tests")
    def test_subvolume_group_snapshot_ls(self):
        # tests the 'fs subvolumegroup snapshot ls' command

        snapshots = []

        # create group
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolumegroup snapshots
        snapshots = self._generate_random_snapshot_name(3)
        for snapshot in snapshots:
            self._fs_cmd("subvolumegroup", "snapshot", "create", self.volname, group, snapshot)

        subvolgrpsnapshotls = json.loads(self._fs_cmd('subvolumegroup', 'snapshot', 'ls', self.volname, group))
        if len(subvolgrpsnapshotls) == 0:
            raise RuntimeError("Expected the 'fs subvolumegroup snapshot ls' command to list the created subvolume group snapshots")
        else:
            snapshotnames = [snapshot['name'] for snapshot in subvolgrpsnapshotls]
            if collections.Counter(snapshotnames) != collections.Counter(snapshots):
                raise RuntimeError("Error creating or listing subvolume group snapshots")

    @unittest.skip("skipping subvolumegroup snapshot tests")
    def test_subvolume_group_snapshot_rm_force(self):
        # test removing non-existing subvolume group snapshot with --force
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()
        # remove snapshot
        try:
            self._fs_cmd("subvolumegroup", "snapshot", "rm", self.volname, group, snapshot, "--force")
        except CommandFailedError:
            raise RuntimeError("expected the 'fs subvolumegroup snapshot rm --force' command to succeed")

    def test_subvolume_group_snapshot_unsupported_status(self):
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # snapshot group
        try:
            self._fs_cmd("subvolumegroup", "snapshot", "create", self.volname, group, snapshot)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOSYS, "invalid error code on subvolumegroup snapshot create")
        else:
            self.fail("expected subvolumegroup snapshot create command to fail")

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)


class TestSubvolumeSnapshots(TestVolumesHelper):
    """Tests for FS subvolume snapshot operations."""
    def test_nonexistent_subvolume_snapshot_rm(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove snapshot again
        try:
            self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOENT:
                raise
        else:
            raise RuntimeError("expected the 'fs subvolume snapshot rm' command to fail")

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_create_and_rm(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_create_idempotence(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # try creating w/ same subvolume snapshot name -- should be idempotent
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_info(self):

        """
        tests the 'fs subvolume snapshot info' command
        """

        snap_md = ["created_at", "data_pool", "has_pending_clones"]

        subvolume = self._generate_random_subvolume_name()
        snapshot, snap_missing = self._generate_random_snapshot_name(2)

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=1)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        snap_info = json.loads(self._get_subvolume_snapshot_info(self.volname, subvolume, snapshot))
        for md in snap_md:
            self.assertIn(md, snap_info, "'{0}' key not present in metadata of snapshot".format(md))
        self.assertEqual(snap_info["has_pending_clones"], "no")

        # snapshot info for non-existent snapshot
        try:
            self._get_subvolume_snapshot_info(self.volname, subvolume, snap_missing)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOENT, "invalid error code on snapshot info of non-existent snapshot")
        else:
            self.fail("expected snapshot info of non-existent snapshot to fail")

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_in_group(self):
        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # snapshot subvolume in group
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot, group)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot, group)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_snapshot_ls(self):
        # tests the 'fs subvolume snapshot ls' command

        snapshots = []

        # create subvolume
        subvolume = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # create subvolume snapshots
        snapshots = self._generate_random_snapshot_name(3)
        for snapshot in snapshots:
            self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        subvolsnapshotls = json.loads(self._fs_cmd('subvolume', 'snapshot', 'ls', self.volname, subvolume))
        if len(subvolsnapshotls) == 0:
            self.fail("Expected the 'fs subvolume snapshot ls' command to list the created subvolume snapshots")
        else:
            snapshotnames = [snapshot['name'] for snapshot in subvolsnapshotls]
            if collections.Counter(snapshotnames) != collections.Counter(snapshots):
                self.fail("Error creating or listing subvolume snapshots")

        # remove snapshot
        for snapshot in snapshots:
            self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_inherited_snapshot_ls(self):
        # tests the scenario where 'fs subvolume snapshot ls' command
        # should not list inherited snapshots created as part of snapshot
        # at ancestral level

        snapshots = []
        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snap_count = 3

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # create subvolume snapshots
        snapshots = self._generate_random_snapshot_name(snap_count)
        for snapshot in snapshots:
            self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot, group)

        # Create snapshot at ancestral level
        ancestral_snappath1 = os.path.join(".", "volumes", group, ".snap", "ancestral_snap_1")
        ancestral_snappath2 = os.path.join(".", "volumes", group, ".snap", "ancestral_snap_2")
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', ancestral_snappath1, ancestral_snappath2], omit_sudo=False)

        subvolsnapshotls = json.loads(self._fs_cmd('subvolume', 'snapshot', 'ls', self.volname, subvolume, group))
        self.assertEqual(len(subvolsnapshotls), snap_count)

        # remove ancestral snapshots
        self.mount_a.run_shell(['sudo', 'rmdir', ancestral_snappath1, ancestral_snappath2], omit_sudo=False)

        # remove snapshot
        for snapshot in snapshots:
            self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot, group)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_inherited_snapshot_info(self):
        """
        tests the scenario where 'fs subvolume snapshot info' command
        should fail for inherited snapshots created as part of snapshot
        at ancestral level
        """

        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # Create snapshot at ancestral level
        ancestral_snap_name = "ancestral_snap_1"
        ancestral_snappath1 = os.path.join(".", "volumes", group, ".snap", ancestral_snap_name)
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', ancestral_snappath1], omit_sudo=False)

        # Validate existence of inherited snapshot
        group_path = os.path.join(".", "volumes", group)
        inode_number_group_dir = int(self.mount_a.run_shell(['stat', '-c' '%i', group_path]).stdout.getvalue().strip())
        inherited_snap = "_{0}_{1}".format(ancestral_snap_name, inode_number_group_dir)
        inherited_snappath = os.path.join(".", "volumes", group, subvolume,".snap", inherited_snap)
        self.mount_a.run_shell(['ls', inherited_snappath])

        # snapshot info on inherited snapshot
        try:
            self._get_subvolume_snapshot_info(self.volname, subvolume, inherited_snap, group)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL, "invalid error code on snapshot info of inherited snapshot")
        else:
            self.fail("expected snapshot info of inherited snapshot to fail")

        # remove ancestral snapshots
        self.mount_a.run_shell(['sudo', 'rmdir', ancestral_snappath1], omit_sudo=False)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--group_name", group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_inherited_snapshot_rm(self):
        """
        tests the scenario where 'fs subvolume snapshot rm' command
        should fail for inherited snapshots created as part of snapshot
        at ancestral level
        """

        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # Create snapshot at ancestral level
        ancestral_snap_name = "ancestral_snap_1"
        ancestral_snappath1 = os.path.join(".", "volumes", group, ".snap", ancestral_snap_name)
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', ancestral_snappath1], omit_sudo=False)

        # Validate existence of inherited snap
        group_path = os.path.join(".", "volumes", group)
        inode_number_group_dir = int(self.mount_a.run_shell(['stat', '-c' '%i', group_path]).stdout.getvalue().strip())
        inherited_snap = "_{0}_{1}".format(ancestral_snap_name, inode_number_group_dir)
        inherited_snappath = os.path.join(".", "volumes", group, subvolume,".snap", inherited_snap)
        self.mount_a.run_shell(['ls', inherited_snappath])

        # inherited snapshot should not be deletable
        try:
            self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, inherited_snap, "--group_name", group)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL, msg="invalid error code when removing inherited snapshot")
        else:
            self.fail("expected removing inheirted snapshot to fail")

        # remove ancestral snapshots
        self.mount_a.run_shell(['sudo', 'rmdir', ancestral_snappath1], omit_sudo=False)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_subvolumegroup_snapshot_name_conflict(self):
        """
        tests the scenario where creation of subvolume snapshot name
        with same name as it's subvolumegroup snapshot name. This should
        fail.
        """

        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        group_snapshot = self._generate_random_snapshot_name()

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # Create subvolumegroup snapshot
        group_snapshot_path = os.path.join(".", "volumes", group, ".snap", group_snapshot)
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', group_snapshot_path], omit_sudo=False)

        # Validate existence of subvolumegroup snapshot
        self.mount_a.run_shell(['ls', group_snapshot_path])

        # Creation of subvolume snapshot with it's subvolumegroup snapshot name should fail
        try:
            self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, group_snapshot, "--group_name", group)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL, msg="invalid error code when creating subvolume snapshot with same name as subvolume group snapshot")
        else:
            self.fail("expected subvolume snapshot creation with same name as subvolumegroup snapshot to fail")

        # remove subvolumegroup snapshot
        self.mount_a.run_shell(['sudo', 'rmdir', group_snapshot_path], omit_sudo=False)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_retain_snapshot_invalid_recreate(self):
        """
        ensure retained subvolume recreate does not leave any incarnations in the subvolume and trash
        """
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # remove with snapshot retention
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--retain-snapshots")

        # recreate subvolume with an invalid pool
        data_pool = "invalid_pool"
        try:
            self._fs_cmd("subvolume", "create", self.volname, subvolume, "--pool_layout", data_pool)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL, "invalid error code on recreate of subvolume with invalid poolname")
        else:
            self.fail("expected recreate of subvolume with invalid poolname to fail")

        # fetch info
        subvol_info = json.loads(self._fs_cmd("subvolume", "info", self.volname, subvolume))
        self.assertEqual(subvol_info["state"], "snapshot-retained",
                         msg="expected state to be 'snapshot-retained', found '{0}".format(subvol_info["state"]))

        # getpath
        try:
            self._fs_cmd("subvolume", "getpath", self.volname, subvolume)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOENT, "invalid error code on getpath of subvolume with retained snapshots")
        else:
            self.fail("expected getpath of subvolume with retained snapshots to fail")

        # remove snapshot (should remove volume)
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_retain_snapshot_recreate_subvolume(self):
        """
        ensure a retained subvolume can be recreated and further snapshotted
        """
        snap_md = ["created_at", "data_pool", "has_pending_clones"]

        subvolume = self._generate_random_subvolume_name()
        snapshot1, snapshot2 = self._generate_random_snapshot_name(2)

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot1)

        # remove with snapshot retention
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--retain-snapshots")

        # fetch info
        subvol_info = json.loads(self._fs_cmd("subvolume", "info", self.volname, subvolume))
        self.assertEqual(subvol_info["state"], "snapshot-retained",
                         msg="expected state to be 'snapshot-retained', found '{0}".format(subvol_info["state"]))

        # recreate retained subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # fetch info
        subvol_info = json.loads(self._fs_cmd("subvolume", "info", self.volname, subvolume))
        self.assertEqual(subvol_info["state"], "complete",
                         msg="expected state to be 'snapshot-retained', found '{0}".format(subvol_info["state"]))

        # snapshot info (older snapshot)
        snap_info = json.loads(self._get_subvolume_snapshot_info(self.volname, subvolume, snapshot1))
        for md in snap_md:
            self.assertIn(md, snap_info, "'{0}' key not present in metadata of snapshot".format(md))
        self.assertEqual(snap_info["has_pending_clones"], "no")

        # snap-create (new snapshot)
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot2)

        # remove with retain snapshots
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--retain-snapshots")

        # list snapshots
        subvolsnapshotls = json.loads(self._fs_cmd('subvolume', 'snapshot', 'ls', self.volname, subvolume))
        self.assertEqual(len(subvolsnapshotls), 2, "Expected the 'fs subvolume snapshot ls' command to list the"
                         " created subvolume snapshots")
        snapshotnames = [snapshot['name'] for snapshot in subvolsnapshotls]
        for snap in [snapshot1, snapshot2]:
            self.assertIn(snap, snapshotnames, "Missing snapshot '{0}' in snapshot list".format(snap))

        # remove snapshots (should remove volume)
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot1)
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot2)

        # verify list subvolumes returns an empty list
        subvolumels = json.loads(self._fs_cmd('subvolume', 'ls', self.volname))
        self.assertEqual(len(subvolumels), 0)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_retain_snapshot_with_snapshots(self):
        """
        ensure retain snapshots based delete of a subvolume with snapshots retains the subvolume
        also test allowed and dis-allowed operations on a retained subvolume
        """
        snap_md = ["created_at", "data_pool", "has_pending_clones"]

        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # remove subvolume -- should fail with ENOTEMPTY since it has snapshots
        try:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOTEMPTY, "invalid error code on rm of retained subvolume with snapshots")
        else:
            self.fail("expected rm of subvolume with retained snapshots to fail")

        # remove with snapshot retention
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--retain-snapshots")

        # fetch info
        subvol_info = json.loads(self._fs_cmd("subvolume", "info", self.volname, subvolume))
        self.assertEqual(subvol_info["state"], "snapshot-retained",
                         msg="expected state to be 'snapshot-retained', found '{0}".format(subvol_info["state"]))

        ## test allowed ops in retained state
        # ls
        subvolumes = json.loads(self._fs_cmd('subvolume', 'ls', self.volname))
        self.assertEqual(len(subvolumes), 1, "subvolume ls count mismatch, expected '1', found {0}".format(len(subvolumes)))
        self.assertEqual(subvolumes[0]['name'], subvolume,
                         "subvolume name mismatch in ls output, expected '{0}', found '{1}'".format(subvolume, subvolumes[0]['name']))

        # snapshot info
        snap_info = json.loads(self._get_subvolume_snapshot_info(self.volname, subvolume, snapshot))
        for md in snap_md:
            self.assertIn(md, snap_info, "'{0}' key not present in metadata of snapshot".format(md))
        self.assertEqual(snap_info["has_pending_clones"], "no")

        # rm --force (allowed but should fail)
        try:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--force")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOTEMPTY, "invalid error code on rm of subvolume with retained snapshots")
        else:
            self.fail("expected rm of subvolume with retained snapshots to fail")

        # rm (allowed but should fail)
        try:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOTEMPTY, "invalid error code on rm of subvolume with retained snapshots")
        else:
            self.fail("expected rm of subvolume with retained snapshots to fail")

        ## test disallowed ops
        # getpath
        try:
            self._fs_cmd("subvolume", "getpath", self.volname, subvolume)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOENT, "invalid error code on getpath of subvolume with retained snapshots")
        else:
            self.fail("expected getpath of subvolume with retained snapshots to fail")

        # resize
        nsize = self.DEFAULT_FILE_SIZE*1024*1024
        try:
            self._fs_cmd("subvolume", "resize", self.volname, subvolume, str(nsize))
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOENT, "invalid error code on resize of subvolume with retained snapshots")
        else:
            self.fail("expected resize of subvolume with retained snapshots to fail")

        # snap-create
        try:
            self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, "fail")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOENT, "invalid error code on snapshot create of subvolume with retained snapshots")
        else:
            self.fail("expected snapshot create of subvolume with retained snapshots to fail")

        # remove snapshot (should remove volume)
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # verify list subvolumes returns an empty list
        subvolumels = json.loads(self._fs_cmd('subvolume', 'ls', self.volname))
        self.assertEqual(len(subvolumels), 0)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_retain_snapshot_without_snapshots(self):
        """
        ensure retain snapshots based delete of a subvolume with no snapshots, deletes the subbvolume
        """
        subvolume = self._generate_random_subvolume_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # remove with snapshot retention (should remove volume, no snapshots to retain)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--retain-snapshots")

        # verify list subvolumes returns an empty list
        subvolumels = json.loads(self._fs_cmd('subvolume', 'ls', self.volname))
        self.assertEqual(len(subvolumels), 0)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_retain_snapshot_trash_busy_recreate(self):
        """
        ensure retained subvolume recreate fails if its trash is not yet purged
        """
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # remove with snapshot retention
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--retain-snapshots")

        # fake a trash entry
        self._update_fake_trash(subvolume)

        # recreate subvolume
        try:
            self._fs_cmd("subvolume", "create", self.volname, subvolume)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EAGAIN, "invalid error code on recreate of subvolume with purge pending")
        else:
            self.fail("expected recreate of subvolume with purge pending to fail")

        # clear fake trash entry
        self._update_fake_trash(subvolume, create=False)

        # recreate subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_rm_with_snapshots(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # remove subvolume -- should fail with ENOTEMPTY since it has snapshots
        try:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOTEMPTY:
                raise RuntimeError("invalid error code returned when deleting subvolume with snapshots")
        else:
            raise RuntimeError("expected subvolume deletion to fail")

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_protect_unprotect_sanity(self):
        """
        Snapshot protect/unprotect commands are deprecated. This test exists to ensure that
        invoking the command does not cause errors, till they are removed from a subsequent release.
        """
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=64)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # now, protect snapshot
        self._fs_cmd("subvolume", "snapshot", "protect", self.volname, subvolume, snapshot)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # now, unprotect snapshot
        self._fs_cmd("subvolume", "snapshot", "unprotect", self.volname, subvolume, snapshot)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_rm_force(self):
        # test removing non existing subvolume snapshot with --force
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()

        # remove snapshot
        try:
            self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot, "--force")
        except CommandFailedError:
            raise RuntimeError("expected the 'fs subvolume snapshot rm --force' command to succeed")

    def test_subvolume_snapshot_metadata_set(self):
        """
        Set custom metadata for subvolume snapshot.
        """
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # set metadata for snapshot.
        key = "key"
        value = "value"
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, key, value, group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume snapshot metadata set' command to succeed")

        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_metadata_set_idempotence(self):
        """
        Set custom metadata for subvolume snapshot (Idempotency).
        """
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # set metadata for snapshot.
        key = "key"
        value = "value"
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, key, value, group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume snapshot metadata set' command to succeed")

        # set same metadata again for subvolume.
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, key, value, group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume snapshot metadata set' command to succeed because it is idempotent operation")

        # get value for specified key.
        try:
            ret = self._fs_cmd("subvolume", "snapshot", "metadata", "get", self.volname, subvolname, snapshot, key, group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume snapshot metadata get' command to succeed")

        # remove '\n' from returned value.
        ret = ret.strip('\n')

        # match received value with expected value.
        self.assertEqual(value, ret)

        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_metadata_get(self):
        """
        Get custom metadata for a specified key in subvolume snapshot metadata.
        """
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # set metadata for snapshot.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, key, value, group)

        # get value for specified key.
        try:
            ret = self._fs_cmd("subvolume", "snapshot", "metadata", "get", self.volname, subvolname, snapshot, key, group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume snapshot metadata get' command to succeed")

        # remove '\n' from returned value.
        ret = ret.strip('\n')

        # match received value with expected value.
        self.assertEqual(value, ret)

        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_metadata_get_for_nonexisting_key(self):
        """
        Get custom metadata for subvolume snapshot if specified key not exist in metadata.
        """
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # set metadata for snapshot.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, key, value, group)

        # try to get value for nonexisting key
        # Expecting ENOENT exit status because key does not exist
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "get", self.volname, subvolname, snapshot, "key_nonexist", group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because 'key_nonexist' does not exist")

        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_metadata_get_for_nonexisting_section(self):
        """
        Get custom metadata for subvolume snapshot if metadata is not added for subvolume snapshot.
        """
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # try to get value for nonexisting key (as section does not exist)
        # Expecting ENOENT exit status because key does not exist
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "get", self.volname, subvolname, snapshot, "key", group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because section does not exist")

        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_metadata_update(self):
        """
        Update custom metadata for a specified key in subvolume snapshot metadata.
        """
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # set metadata for snapshot.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, key, value, group)

        # update metadata against key.
        new_value = "new_value"
        self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, key, new_value, group)

        # get metadata for specified key of snapshot.
        try:
            ret = self._fs_cmd("subvolume", "snapshot", "metadata", "get", self.volname, subvolname, snapshot, key, group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume snapshot metadata get' command to succeed")

        # remove '\n' from returned value.
        ret = ret.strip('\n')

        # match received value with expected value.
        self.assertEqual(new_value, ret)

        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_metadata_list(self):
        """
        List custom metadata for subvolume snapshot.
        """
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # set metadata for subvolume.
        input_metadata_dict =  {f'key_{i}' : f'value_{i}' for i in range(3)}

        for k, v in input_metadata_dict.items():
            self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, k, v, group)

        # list metadata
        try:
            ret_dict = json.loads(self._fs_cmd("subvolume", "snapshot", "metadata", "ls", self.volname, subvolname, snapshot, group))
        except CommandFailedError:
            self.fail("expected the 'fs subvolume snapshot metadata ls' command to succeed")

        # compare output with expected output
        self.assertDictEqual(input_metadata_dict, ret_dict)

        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_metadata_list_if_no_metadata_set(self):
        """
        List custom metadata for subvolume snapshot if metadata is not added for subvolume snapshot.
        """
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # list metadata
        try:
            ret_dict = json.loads(self._fs_cmd("subvolume", "snapshot", "metadata", "ls", self.volname, subvolname, snapshot, group))
        except CommandFailedError:
            self.fail("expected the 'fs subvolume snapshot metadata ls' command to succeed")

        # compare output with expected output
        empty_dict = {}
        self.assertDictEqual(ret_dict, empty_dict)

        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_metadata_remove(self):
        """
        Remove custom metadata for a specified key in subvolume snapshot metadata.
        """
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # set metadata for snapshot.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, key, value, group)

        # remove metadata against specified key.
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "rm", self.volname, subvolname, snapshot, key, group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume snapshot metadata rm' command to succeed")

        # confirm key is removed by again fetching metadata
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "get", self.volname, subvolname, key, snapshot, group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because key does not exist")

        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_metadata_remove_for_nonexisting_key(self):
        """
        Remove custom metadata for subvolume snapshot if specified key not exist in metadata.
        """
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # set metadata for snapshot.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, key, value, group)

        # try to remove value for nonexisting key
        # Expecting ENOENT exit status because key does not exist
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "rm", self.volname, subvolname, snapshot, "key_nonexist", group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because 'key_nonexist' does not exist")

        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_metadata_remove_for_nonexisting_section(self):
        """
        Remove custom metadata for subvolume snapshot if metadata is not added for subvolume snapshot.
        """
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # try to remove value for nonexisting key (as section does not exist)
        # Expecting ENOENT exit status because key does not exist
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "rm", self.volname, subvolname, snapshot, "key", group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because section does not exist")

        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_metadata_remove_force(self):
        """
        Forcefully remove custom metadata for a specified key in subvolume snapshot metadata.
        """
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # set metadata for snapshot.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, key, value, group)

        # remove metadata against specified key with --force option.
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "rm", self.volname, subvolname, snapshot, key, group, "--force")
        except CommandFailedError:
            self.fail("expected the 'fs subvolume snapshot metadata rm' command to succeed")

        # confirm key is removed by again fetching metadata
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "get", self.volname, subvolname, snapshot, key, group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because key does not exist")

        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_metadata_remove_force_for_nonexisting_key(self):
        """
        Forcefully remove custom metadata for subvolume snapshot if specified key not exist in metadata.
        """
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # set metadata for snapshot.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, key, value, group)

        # remove metadata against specified key.
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "rm", self.volname, subvolname, snapshot, key, group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume snapshot metadata rm' command to succeed")

        # confirm key is removed by again fetching metadata
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "get", self.volname, subvolname, snapshot, key, group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because key does not exist")

        # again remove metadata against already removed key with --force option.
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "rm", self.volname, subvolname, snapshot, key, group, "--force")
        except CommandFailedError:
            self.fail("expected the 'fs subvolume snapshot metadata rm' (with --force) command to succeed")

        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_metadata_after_snapshot_remove(self):
        """
        Verify metadata removal of subvolume snapshot after snapshot removal.
        """
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # set metadata for snapshot.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, key, value, group)

        # get value for specified key.
        ret = self._fs_cmd("subvolume", "snapshot", "metadata", "get", self.volname, subvolname, snapshot, key, group)

        # remove '\n' from returned value.
        ret = ret.strip('\n')

        # match received value with expected value.
        self.assertEqual(value, ret)

        # remove subvolume snapshot.
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)

        # try to get metadata after removing snapshot.
        # Expecting error ENOENT with error message of snapshot does not exist
        cmd_ret = self.run_ceph_cmd(
            args=["fs", "subvolume", "snapshot", "metadata", "get", self.volname, subvolname, snapshot, key, group], check_status=False, stdout=StringIO(),
            stderr=StringIO())
        self.assertEqual(cmd_ret.returncode, errno.ENOENT, "Expecting ENOENT error")
        self.assertIn(f"snapshot '{snapshot}' does not exist", cmd_ret.stderr.getvalue(),
                f"Expecting message: snapshot '{snapshot}' does not exist ")

        # confirm metadata is removed by searching section name in .meta file
        meta_path = os.path.join(".", "volumes", group, subvolname, ".meta")
        section_name = "SNAP_METADATA_" + snapshot

        try:
            self.mount_a.run_shell(f"sudo grep {section_name} {meta_path}", omit_sudo=False)
        except CommandFailedError as e:
            self.assertNotEqual(e.exitstatus, 0)
        else:
            self.fail("Expected non-zero exist status because section should not exist")

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_clean_stale_subvolume_snapshot_metadata(self):
        """
        Validate cleaning of stale subvolume snapshot metadata.
        """
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # set metadata for snapshot.
        key = "key"
        value = "value"
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, key, value, group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume snapshot metadata set' command to succeed")

        # save the subvolume config file.
        meta_path = os.path.join(".", "volumes", group, subvolname, ".meta")
        tmp_meta_path = os.path.join(".", "volumes", group, subvolname, ".meta.stale_snap_section")
        self.mount_a.run_shell(['sudo', 'cp', '-p', meta_path, tmp_meta_path], omit_sudo=False)

        # Delete snapshot, this would remove user snap metadata
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)

        # Copy back saved subvolume config file. This would have stale snapshot metadata
        self.mount_a.run_shell(['sudo', 'cp', '-p', tmp_meta_path, meta_path], omit_sudo=False)

        # Verify that it has stale snapshot metadata
        section_name = "SNAP_METADATA_" + snapshot
        try:
            self.mount_a.run_shell(f"sudo grep {section_name} {meta_path}", omit_sudo=False)
        except CommandFailedError:
            self.fail("Expected grep cmd to succeed because stale snapshot metadata exist")

        # Do any subvolume operation to clean the stale snapshot metadata
        _ = json.loads(self._get_subvolume_info(self.volname, subvolname, group))

        # Verify that the stale snapshot metadata is cleaned
        try:
            self.mount_a.run_shell(f"sudo grep {section_name} {meta_path}", omit_sudo=False)
        except CommandFailedError as e:
            self.assertNotEqual(e.exitstatus, 0)
        else:
            self.fail("Expected non-zero exist status because stale snapshot metadata should not exist")

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()
        # Clean tmp config file
        self.mount_a.run_shell(['sudo', 'rm', '-f', tmp_meta_path], omit_sudo=False)


class TestSubvolumeSnapshotClones(TestVolumesHelper):
    """ Tests for FS subvolume snapshot clone operations."""
    def test_clone_subvolume_info(self):
        # tests the 'fs subvolume info' command for a clone
        subvol_md = ["atime", "bytes_pcent", "bytes_quota", "bytes_used", "created_at", "ctime",
                     "data_pool", "gid", "mode", "mon_addrs", "mtime", "path", "pool_namespace",
                     "type", "uid"]

        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=1)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        subvol_info = json.loads(self._get_subvolume_info(self.volname, clone))
        if len(subvol_info) == 0:
            raise RuntimeError("Expected the 'fs subvolume info' command to list metadata of subvolume")
        for md in subvol_md:
            if md not in subvol_info.keys():
                raise RuntimeError("%s not present in the metadata of subvolume" % md)
        if subvol_info["type"] != "clone":
            raise RuntimeError("type should be set to clone")

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_info_without_snapshot_clone(self):
        """
        Verify subvolume snapshot info output without cloning snapshot.
        If no clone is performed then path /volumes/_index/clone/{track_id}
        will not exist.
        """
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()

        # create subvolume.
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # list snapshot info
        result = json.loads(self._fs_cmd("subvolume", "snapshot", "info", self.volname, subvolume, snapshot))

        # verify snapshot info
        self.assertEqual(result['has_pending_clones'], "no")
        self.assertFalse('orphan_clones_count' in result)
        self.assertFalse('pending_clones' in result)

        # remove snapshot, subvolume, clone
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_info_if_no_clone_pending(self):
        """
        Verify subvolume snapshot info output if no clone is in pending state.
        """
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone_list =  [f'clone_{i}' for i in range(3)]

        # create subvolume.
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # schedule a clones
        for clone in clone_list:
            self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clones status
        for clone in clone_list:
            self._wait_for_clone_to_complete(clone)

        # list snapshot info
        result = json.loads(self._fs_cmd("subvolume", "snapshot", "info", self.volname, subvolume, snapshot))

        # verify snapshot info
        self.assertEqual(result['has_pending_clones'], "no")
        self.assertFalse('orphan_clones_count' in result)
        self.assertFalse('pending_clones' in result)

        # remove snapshot, subvolume, clone
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        for clone in clone_list:
            self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_info_if_clone_pending_for_no_group(self):
        """
        Verify subvolume snapshot info output if clones are in pending state.
        Clones are not specified for particular target_group. Hence target_group
        should not be in the output as we don't show _nogroup (default group)
        """
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone_list =  [f'clone_{i}' for i in range(3)]

        # create subvolume.
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # insert delay at the beginning of snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 5)

        # schedule a clones
        for clone in clone_list:
            self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # list snapshot info
        result = json.loads(self._fs_cmd("subvolume", "snapshot", "info", self.volname, subvolume, snapshot))

        # verify snapshot info
        expected_clone_list = []
        for clone in clone_list:
            expected_clone_list.append({"name": clone})
        self.assertEqual(result['has_pending_clones'], "yes")
        self.assertFalse('orphan_clones_count' in result)
        self.assertListEqual(result['pending_clones'], expected_clone_list)
        self.assertEqual(len(result['pending_clones']), 3)

        # check clones status
        for clone in clone_list:
            self._wait_for_clone_to_complete(clone)

        # remove snapshot, subvolume, clone
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        for clone in clone_list:
            self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_info_if_clone_pending_for_target_group(self):
        """
        Verify subvolume snapshot info output if clones are in pending state.
        Clones are not specified for target_group.
        """
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()
        group = self._generate_random_group_name()
        target_group = self._generate_random_group_name()

        # create groups
        self._fs_cmd("subvolumegroup", "create", self.volname, group)
        self._fs_cmd("subvolumegroup", "create", self.volname, target_group)

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, group, "--mode=777")

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot, group)

        # insert delay at the beginning of snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 5)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone,
                     "--group_name", group, "--target_group_name", target_group)

        # list snapshot info
        result = json.loads(self._fs_cmd("subvolume", "snapshot", "info", self.volname, subvolume, snapshot, "--group_name", group))

        # verify snapshot info
        expected_clone_list = [{"name": clone, "target_group": target_group}]
        self.assertEqual(result['has_pending_clones'], "yes")
        self.assertFalse('orphan_clones_count' in result)
        self.assertListEqual(result['pending_clones'], expected_clone_list)
        self.assertEqual(len(result['pending_clones']), 1)

        # check clone status
        self._wait_for_clone_to_complete(clone, clone_group=target_group)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot, group)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, group)
        self._fs_cmd("subvolume", "rm", self.volname, clone, target_group)

        # remove groups
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, target_group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_info_if_orphan_clone(self):
        """
        Verify subvolume snapshot info output if orphan clones exists.
        Orphan clones should not list under pending clones.
        orphan_clones_count should display correct count of orphan clones'
        """
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone_list =  [f'clone_{i}' for i in range(3)]

        # create subvolume.
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # insert delay at the beginning of snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 15)

        # schedule a clones
        for clone in clone_list:
            self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # remove track file for third clone to make it orphan
        meta_path = os.path.join(".", "volumes", "_nogroup", subvolume, ".meta")
        pending_clones_result = self.mount_a.run_shell(['sudo', 'grep', 'clone snaps', '-A3', meta_path], omit_sudo=False, stdout=StringIO(), stderr=StringIO())
        third_clone_track_id = pending_clones_result.stdout.getvalue().splitlines()[3].split(" = ")[0]
        third_clone_track_path = os.path.join(".", "volumes", "_index", "clone", third_clone_track_id)
        self.mount_a.run_shell(f"sudo rm -f {third_clone_track_path}", omit_sudo=False)

        # list snapshot info
        result = json.loads(self._fs_cmd("subvolume", "snapshot", "info", self.volname, subvolume, snapshot))

        # verify snapshot info
        expected_clone_list = []
        for i in range(len(clone_list)-1):
            expected_clone_list.append({"name": clone_list[i]})
        self.assertEqual(result['has_pending_clones'], "yes")
        self.assertEqual(result['orphan_clones_count'], 1)
        self.assertListEqual(result['pending_clones'], expected_clone_list)
        self.assertEqual(len(result['pending_clones']), 2)

        # check clones status
        for i in range(len(clone_list)-1):
            self._wait_for_clone_to_complete(clone_list[i])

        # list snapshot info after cloning completion
        res = json.loads(self._fs_cmd("subvolume", "snapshot", "info", self.volname, subvolume, snapshot))

        # verify snapshot info (has_pending_clones should be no)
        self.assertEqual(res['has_pending_clones'], "no")

    def test_non_clone_status(self):
        subvolume = self._generate_random_subvolume_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        try:
            self._fs_cmd("clone", "status", self.volname, subvolume)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOTSUP:
                raise RuntimeError("invalid error code when fetching status of a non cloned subvolume")
        else:
            raise RuntimeError("expected fetching of clone status of a subvolume to fail")

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_clone_inherit_snapshot_namespace_and_size(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()
        osize = self.DEFAULT_FILE_SIZE*1024*1024*12

        # create subvolume, in an isolated namespace with a specified size
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--namespace-isolated", "--size", str(osize), "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=8)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # create a pool different from current subvolume pool
        subvol_path = self._get_subvolume_path(self.volname, subvolume)
        default_pool = self.mount_a.getfattr(subvol_path, "ceph.dir.layout.pool")
        new_pool = "new_pool"
        self.assertNotEqual(default_pool, new_pool)
        self.fs.add_data_pool(new_pool)

        # update source subvolume pool
        self._do_subvolume_pool_and_namespace_update(subvolume, pool=new_pool, pool_namespace="")

        # schedule a clone, with NO --pool specification
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_clone_inherit_quota_attrs(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()
        osize = self.DEFAULT_FILE_SIZE*1024*1024*12

        # create subvolume with a specified size
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777", "--size", str(osize))

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=8)

        # get subvolume path
        subvolpath = self._get_subvolume_path(self.volname, subvolume)

        # set quota on number of files
        self.mount_a.setfattr(subvolpath, 'ceph.quota.max_files', "20", sudo=True)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone)

        # get subvolume path
        clonepath = self._get_subvolume_path(self.volname, clone)

        # verify quota max_files is inherited from source snapshot
        subvol_quota = self.mount_a.getfattr(subvolpath, "ceph.quota.max_files")
        clone_quota = self.mount_a.getfattr(clonepath, "ceph.quota.max_files")
        self.assertEqual(subvol_quota, clone_quota)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_clone_in_progress_getpath(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=64)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # Insert delay at the beginning of snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 2)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # clone should not be accessible right now
        try:
            self._get_subvolume_path(self.volname, clone)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EAGAIN:
                raise RuntimeError("invalid error code when fetching path of an pending clone")
        else:
            raise RuntimeError("expected fetching path of an pending clone to fail")

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # clone should be accessible now
        subvolpath = self._get_subvolume_path(self.volname, clone)
        self.assertNotEqual(subvolpath, None)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_clone_in_progress_snapshot_rm(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=64)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # Insert delay at the beginning of snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 2)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # snapshot should not be deletable now
        try:
            self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EAGAIN, msg="invalid error code when removing source snapshot of a clone")
        else:
            self.fail("expected removing source snapshot of a clone to fail")

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # clone should be accessible now
        subvolpath = self._get_subvolume_path(self.volname, clone)
        self.assertNotEqual(subvolpath, None)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_clone_in_progress_source(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=64)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # Insert delay at the beginning of snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 2)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # verify clone source
        result = json.loads(self._fs_cmd("clone", "status", self.volname, clone))
        source = result['status']['source']
        self.assertEqual(source['volume'], self.volname)
        self.assertEqual(source['subvolume'], subvolume)
        self.assertEqual(source.get('group', None), None)
        self.assertEqual(source['snapshot'], snapshot)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # clone should be accessible now
        subvolpath = self._get_subvolume_path(self.volname, clone)
        self.assertNotEqual(subvolpath, None)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_clone_retain_snapshot_with_snapshots(self):
        """
        retain snapshots of a cloned subvolume and check disallowed operations
        """
        subvolume = self._generate_random_subvolume_name()
        snapshot1, snapshot2 = self._generate_random_snapshot_name(2)
        clone = self._generate_random_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # store path for clone verification
        subvol1_path = self._get_subvolume_path(self.volname, subvolume)

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=16)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot1)

        # remove with snapshot retention
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--retain-snapshots")

        # clone retained subvolume snapshot
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot1, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume, snapshot1, clone, subvol_path=subvol1_path)

        # create a snapshot on the clone
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, clone, snapshot2)

        # retain a clone
        self._fs_cmd("subvolume", "rm", self.volname, clone, "--retain-snapshots")

        # list snapshots
        clonesnapshotls = json.loads(self._fs_cmd('subvolume', 'snapshot', 'ls', self.volname, clone))
        self.assertEqual(len(clonesnapshotls), 1, "Expected the 'fs subvolume snapshot ls' command to list the"
                         " created subvolume snapshots")
        snapshotnames = [snapshot['name'] for snapshot in clonesnapshotls]
        for snap in [snapshot2]:
            self.assertIn(snap, snapshotnames, "Missing snapshot '{0}' in snapshot list".format(snap))

        ## check disallowed operations on retained clone
        # clone-status
        try:
            self._fs_cmd("clone", "status", self.volname, clone)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOENT, "invalid error code on clone status of clone with retained snapshots")
        else:
            self.fail("expected clone status of clone with retained snapshots to fail")

        # clone-cancel
        try:
            self._fs_cmd("clone", "cancel", self.volname, clone)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOENT, "invalid error code on clone cancel of clone with retained snapshots")
        else:
            self.fail("expected clone cancel of clone with retained snapshots to fail")

        # remove snapshots (removes subvolumes as all are in retained state)
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot1)
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, clone, snapshot2)

        # verify list subvolumes returns an empty list
        subvolumels = json.loads(self._fs_cmd('subvolume', 'ls', self.volname))
        self.assertEqual(len(subvolumels), 0)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_retain_snapshot_clone(self):
        """
        clone a snapshot from a snapshot retained subvolume
        """
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # store path for clone verification
        subvol_path = self._get_subvolume_path(self.volname, subvolume)

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=16)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # remove with snapshot retention
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--retain-snapshots")

        # clone retained subvolume snapshot
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone, subvol_path=subvol_path)

        # remove snapshots (removes retained volume)
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify list subvolumes returns an empty list
        subvolumels = json.loads(self._fs_cmd('subvolume', 'ls', self.volname))
        self.assertEqual(len(subvolumels), 0)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_retain_snapshot_clone_from_newer_snapshot(self):
        """
        clone a subvolume from recreated subvolume's latest snapshot
        """
        subvolume = self._generate_random_subvolume_name()
        snapshot1, snapshot2 = self._generate_random_snapshot_name(2)
        clone = self._generate_random_clone_name(1)

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=16)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot1)

        # remove with snapshot retention
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--retain-snapshots")

        # recreate subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # get and store path for clone verification
        subvol2_path = self._get_subvolume_path(self.volname, subvolume)

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=16)

        # snapshot newer subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot2)

        # remove with snapshot retention
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--retain-snapshots")

        # clone retained subvolume's newer snapshot
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot2, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume, snapshot2, clone, subvol_path=subvol2_path)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot1)
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot2)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify list subvolumes returns an empty list
        subvolumels = json.loads(self._fs_cmd('subvolume', 'ls', self.volname))
        self.assertEqual(len(subvolumels), 0)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_retain_snapshot_recreate(self):
        """
        recreate a subvolume from one of its retained snapshots
        """
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # store path for clone verification
        subvol_path = self._get_subvolume_path(self.volname, subvolume)

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=16)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # remove with snapshot retention
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--retain-snapshots")

        # recreate retained subvolume using its own snapshot to clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, subvolume)

        # check clone status
        self._wait_for_clone_to_complete(subvolume)

        # verify clone
        self._verify_clone(subvolume, snapshot, subvolume, subvol_path=subvol_path)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify list subvolumes returns an empty list
        subvolumels = json.loads(self._fs_cmd('subvolume', 'ls', self.volname))
        self.assertEqual(len(subvolumels), 0)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_retain_snapshot_trash_busy_recreate_clone(self):
        """
        ensure retained clone recreate fails if its trash is not yet purged
        """
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # clone subvolume snapshot
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # snapshot clone
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, clone, snapshot)

        # remove clone with snapshot retention
        self._fs_cmd("subvolume", "rm", self.volname, clone, "--retain-snapshots")

        # fake a trash entry
        self._update_fake_trash(clone)

        # clone subvolume snapshot (recreate)
        try:
            self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EAGAIN, "invalid error code on recreate of clone with purge pending")
        else:
            self.fail("expected recreate of clone with purge pending to fail")

        # clear fake trash entry
        self._update_fake_trash(clone, create=False)

        # recreate subvolume
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, clone, snapshot)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_attr_clone(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io_mixed(subvolume)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_clone_failure_status_pending_in_progress_complete(self):
        """
        ensure failure status is not shown when clone is not in failed/cancelled state
        """
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone1 = self._generate_random_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=200)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # Insert delay at the beginning of snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 5)

        # schedule a clone1
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone1)

        # pending clone shouldn't show failure status
        clone1_result = self._get_clone_status(clone1)
        try:
            clone1_result["status"]["failure"]["errno"]
        except KeyError as e:
            self.assertEqual(str(e), "'failure'")
        else:
            self.fail("clone status shouldn't show failure for pending clone")

        # check clone1 to be in-progress
        self._wait_for_clone_to_be_in_progress(clone1)

        # in-progress clone1 shouldn't show failure status
        clone1_result = self._get_clone_status(clone1)
        try:
            clone1_result["status"]["failure"]["errno"]
        except KeyError as e:
            self.assertEqual(str(e), "'failure'")
        else:
            self.fail("clone status shouldn't show failure for in-progress clone")

        # wait for clone1 to complete
        self._wait_for_clone_to_complete(clone1)

        # complete clone1 shouldn't show failure status
        clone1_result = self._get_clone_status(clone1)
        try:
            clone1_result["status"]["failure"]["errno"]
        except KeyError as e:
            self.assertEqual(str(e), "'failure'")
        else:
            self.fail("clone status shouldn't show failure for complete clone")

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone1)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_clone_failure_status_failed(self):
        """
        ensure failure status is shown when clone is in failed state and validate the reason
        """
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone1 = self._generate_random_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=200)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # Insert delay at the beginning of snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 5)

        # schedule a clone1
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone1)

        # remove snapshot from backend to force the clone failure.
        snappath = os.path.join(".", "volumes", "_nogroup", subvolume, ".snap", snapshot)
        self.mount_a.run_shell(['sudo', 'rmdir', snappath], omit_sudo=False)

        # wait for clone1 to fail.
        self._wait_for_clone_to_fail(clone1)

        # check clone1 status
        clone1_result = self._get_clone_status(clone1)
        self.assertEqual(clone1_result["status"]["state"], "failed")
        self.assertEqual(clone1_result["status"]["failure"]["errno"], "2")
        self.assertEqual(clone1_result["status"]["failure"]["error_msg"], "snapshot '{0}' does not exist".format(snapshot))

        # clone removal should succeed after failure, remove clone1
        self._fs_cmd("subvolume", "rm", self.volname, clone1, "--force")

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_clone_failure_status_pending_cancelled(self):
        """
        ensure failure status is shown when clone is cancelled during pending state and validate the reason
        """
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone1 = self._generate_random_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=200)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # Insert delay at the beginning of snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 5)

        # schedule a clone1
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone1)

        # cancel pending clone1
        self._fs_cmd("clone", "cancel", self.volname, clone1)

        # check clone1 status
        clone1_result = self._get_clone_status(clone1)
        self.assertEqual(clone1_result["status"]["state"], "canceled")
        self.assertEqual(clone1_result["status"]["failure"]["errno"], "4")
        self.assertEqual(clone1_result["status"]["failure"]["error_msg"], "user interrupted clone operation")

        # clone removal should succeed with force after cancelled, remove clone1
        self._fs_cmd("subvolume", "rm", self.volname, clone1, "--force")

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_clone_failure_status_in_progress_cancelled(self):
        """
        ensure failure status is shown when clone is cancelled during in-progress state and validate the reason
        """
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone1 = self._generate_random_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=200)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # Insert delay at the beginning of snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 5)

        # schedule a clone1
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone1)

        # wait for clone1 to be in-progress
        self._wait_for_clone_to_be_in_progress(clone1)

        # cancel in-progess clone1
        self._fs_cmd("clone", "cancel", self.volname, clone1)

        # check clone1 status
        clone1_result = self._get_clone_status(clone1)
        self.assertEqual(clone1_result["status"]["state"], "canceled")
        self.assertEqual(clone1_result["status"]["failure"]["errno"], "4")
        self.assertEqual(clone1_result["status"]["failure"]["error_msg"], "user interrupted clone operation")

        # clone removal should succeed with force after cancelled, remove clone1
        self._fs_cmd("subvolume", "rm", self.volname, clone1, "--force")

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=64)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone_quota_exceeded(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        # create subvolume with 20MB quota
        osize = self.DEFAULT_FILE_SIZE*1024*1024*20
        self._fs_cmd("subvolume", "create", self.volname, subvolume,"--mode=777", "--size", str(osize))

        # do IO, write 50 files of 1MB each to exceed quota. This mostly succeeds as quota enforcement takes time.
        try:
            self._do_subvolume_io(subvolume, number_of_files=50)
        except CommandFailedError:
            # ignore quota enforcement error.
            pass

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_in_complete_clone_rm(self):
        """
        Validates the removal of clone when it is not in 'complete|cancelled|failed' state.
        The forceful removl of subvolume clone succeeds only if it's in any of the
        'complete|cancelled|failed' states. It fails with EAGAIN in any other states.
        """

        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=64)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # Insert delay at the beginning of snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 2)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # Use --force since clone is not complete. Returns EAGAIN as clone is not either complete or cancelled.
        try:
            self._fs_cmd("subvolume", "rm", self.volname, clone, "--force")
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EAGAIN:
                raise RuntimeError("invalid error code when trying to remove failed clone")
        else:
            raise RuntimeError("expected error when removing a failed clone")

        # cancel on-going clone
        self._fs_cmd("clone", "cancel", self.volname, clone)

        # verify canceled state
        self._check_clone_canceled(clone)

        # clone removal should succeed after cancel
        self._fs_cmd("subvolume", "rm", self.volname, clone, "--force")

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone_retain_suid_guid(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # Create a file with suid, guid bits set along with executable bit.
        args = ["subvolume", "getpath", self.volname, subvolume]
        args = tuple(args)
        subvolpath = self._fs_cmd(*args)
        self.assertNotEqual(subvolpath, None)
        subvolpath = subvolpath[1:].rstrip() # remove "/" prefix and any trailing newline

        file_path = subvolpath
        file_path = os.path.join(subvolpath, "test_suid_file")
        self.mount_a.run_shell(["touch", file_path])
        self.mount_a.run_shell(["chmod", "u+sx,g+sx", file_path])

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone_and_reclone(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone1, clone2 = self._generate_random_clone_name(2)

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=32)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone1)

        # check clone status
        self._wait_for_clone_to_complete(clone1)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone1)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # now the clone is just like a normal subvolume -- snapshot the clone and fork
        # another clone. before that do some IO so it's can be differentiated.
        self._do_subvolume_io(clone1, create_dir="data", number_of_files=32)

        # snapshot clone -- use same snap name
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, clone1, snapshot)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, clone1, snapshot, clone2)

        # check clone status
        self._wait_for_clone_to_complete(clone2)

        # verify clone
        self._verify_clone(clone1, snapshot, clone2)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, clone1, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone1)
        self._fs_cmd("subvolume", "rm", self.volname, clone2)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone_cancel_in_progress(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=128)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # Insert delay at the beginning of snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 2)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # cancel on-going clone
        self._fs_cmd("clone", "cancel", self.volname, clone)

        # verify canceled state
        self._check_clone_canceled(clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone, "--force")

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone_cancel_pending(self):
        """
        this test is a bit more involved compared to canceling an in-progress clone.
        we'd need to ensure that a to-be canceled clone has still not been picked up
        by cloner threads. exploit the fact that clones are picked up in an FCFS
        fashion and there are four (4) cloner threads by default. When the number of
        cloner threads increase, this test _may_ start tripping -- so, the number of
        clone operations would need to be jacked up.
        """
        # default number of clone threads
        NR_THREADS = 4
        # good enough for 4 threads
        NR_CLONES = 5
        # yeh, 1gig -- we need the clone to run for sometime
        FILE_SIZE_MB = 1024

        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clones = self._generate_random_clone_name(NR_CLONES)

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=4, file_size=FILE_SIZE_MB)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # schedule clones
        for clone in clones:
            self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        to_wait = clones[0:NR_THREADS]
        to_cancel = clones[NR_THREADS:]

        # cancel pending clones and verify
        for clone in to_cancel:
            status = json.loads(self._fs_cmd("clone", "status", self.volname, clone))
            self.assertEqual(status["status"]["state"], "pending")
            self._fs_cmd("clone", "cancel", self.volname, clone)
            self._check_clone_canceled(clone)

        # let's cancel on-going clones. handle the case where some of the clones
        # _just_ complete
        for clone in list(to_wait):
            try:
                self._fs_cmd("clone", "cancel", self.volname, clone)
                to_cancel.append(clone)
                to_wait.remove(clone)
            except CommandFailedError as ce:
                if ce.exitstatus != errno.EINVAL:
                    raise RuntimeError("invalid error code when cancelling on-going clone")

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        for clone in to_wait:
            self._fs_cmd("subvolume", "rm", self.volname, clone)
        for clone in to_cancel:
            self._fs_cmd("subvolume", "rm", self.volname, clone, "--force")

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone_different_groups(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()
        s_group, c_group = self._generate_random_group_name(2)

        # create groups
        self._fs_cmd("subvolumegroup", "create", self.volname, s_group)
        self._fs_cmd("subvolumegroup", "create", self.volname, c_group)

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, s_group, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, subvolume_group=s_group, number_of_files=32)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot, s_group)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone,
                     '--group_name', s_group, '--target_group_name', c_group)

        # check clone status
        self._wait_for_clone_to_complete(clone, clone_group=c_group)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone, source_group=s_group, clone_group=c_group)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot, s_group)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, s_group)
        self._fs_cmd("subvolume", "rm", self.volname, clone, c_group)

        # remove groups
        self._fs_cmd("subvolumegroup", "rm", self.volname, s_group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, c_group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone_fail_with_remove(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone1, clone2 = self._generate_random_clone_name(2)

        pool_capacity = 32 * 1024 * 1024
        # number of files required to fill up 99% of the pool
        nr_files = int((pool_capacity * 0.99) / (TestVolumes.DEFAULT_FILE_SIZE * 1024 * 1024))

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=nr_files)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # add data pool
        new_pool = "new_pool"
        self.fs.add_data_pool(new_pool)

        self.fs.mon_manager.raw_cluster_cmd("osd", "pool", "set-quota", new_pool,
                                            "max_bytes", "{0}".format(pool_capacity // 4))

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone1, "--pool_layout", new_pool)

        # check clone status -- this should dramatically overshoot the pool quota
        self._wait_for_clone_to_complete(clone1)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone1, clone_pool=new_pool)

        # wait a bit so that subsequent I/O will give pool full error
        time.sleep(120)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone2, "--pool_layout", new_pool)

        # check clone status
        self._wait_for_clone_to_fail(clone2)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone1)
        try:
            self._fs_cmd("subvolume", "rm", self.volname, clone2)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EAGAIN:
                raise RuntimeError("invalid error code when trying to remove failed clone")
        else:
            raise RuntimeError("expected error when removing a failed clone")

        #  ... and with force, failed clone can be removed
        self._fs_cmd("subvolume", "rm", self.volname, clone2, "--force")

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone_on_existing_subvolumes(self):
        subvolume1, subvolume2 = self._generate_random_subvolume_name(2)
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        # create subvolumes
        self._fs_cmd("subvolume", "create", self.volname, subvolume1, "--mode=777")
        self._fs_cmd("subvolume", "create", self.volname, subvolume2, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume1, number_of_files=32)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume1, snapshot)

        # schedule a clone with target as subvolume2
        try:
            self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume1, snapshot, subvolume2)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EEXIST:
                raise RuntimeError("invalid error code when cloning to existing subvolume")
        else:
            raise RuntimeError("expected cloning to fail if the target is an existing subvolume")

        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume1, snapshot, clone)

        # schedule a clone with target as clone
        try:
            self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume1, snapshot, clone)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EEXIST:
                raise RuntimeError("invalid error code when cloning to existing clone")
        else:
            raise RuntimeError("expected cloning to fail if the target is an existing clone")

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume1, snapshot, clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume1, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume1)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume2)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone_pool_layout(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        # add data pool
        new_pool = "new_pool"
        newid = self.fs.add_data_pool(new_pool)

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=32)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone, "--pool_layout", new_pool)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone, clone_pool=new_pool)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        subvol_path = self._get_subvolume_path(self.volname, clone)
        desired_pool = self.mount_a.getfattr(subvol_path, "ceph.dir.layout.pool")
        try:
            self.assertEqual(desired_pool, new_pool)
        except AssertionError:
            self.assertEqual(int(desired_pool), newid) # old kernel returns id

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone_under_group(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()
        group = self._generate_random_group_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=32)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone, '--target_group_name', group)

        # check clone status
        self._wait_for_clone_to_complete(clone, clone_group=group)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone, clone_group=group)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone, group)

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone_with_attrs(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        mode = "777"
        uid  = "1000"
        gid  = "1000"
        new_uid  = "1001"
        new_gid  = "1001"
        new_mode = "700"

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode", mode, "--uid", uid, "--gid", gid)

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=32)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # change subvolume attrs (to ensure clone picks up snapshot attrs)
        self._do_subvolume_attr_update(subvolume, new_uid, new_gid, new_mode)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone_with_upgrade(self):
        """
        yet another poor man's upgrade test -- rather than going through a full
        upgrade cycle, emulate old types subvolumes by going through the wormhole
        and verify clone operation.
        further ensure that a legacy volume is not updated to v2, but clone is.
        """
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        # emulate a old-fashioned subvolume
        createpath = os.path.join(".", "volumes", "_nogroup", subvolume)
        self.mount_a.run_shell_payload(f"sudo mkdir -p -m 777 {createpath}", omit_sudo=False)

        # add required xattrs to subvolume
        default_pool = self.mount_a.getfattr(".", "ceph.dir.layout.pool")
        self.mount_a.setfattr(createpath, 'ceph.dir.layout.pool', default_pool, sudo=True)

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=64)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # ensure metadata file is in legacy location, with required version v1
        self._assert_meta_location_and_version(self.volname, subvolume, version=1, legacy=True)

        # Insert delay at the beginning of snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 2)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # snapshot should not be deletable now
        try:
            self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EAGAIN, msg="invalid error code when removing source snapshot of a clone")
        else:
            self.fail("expected removing source snapshot of a clone to fail")

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone, source_version=1)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # ensure metadata file is in v2 location, with required version v2
        self._assert_meta_location_and_version(self.volname, clone)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_reconf_max_concurrent_clones(self):
        """
        Validate 'max_concurrent_clones' config option
        """

        # get the default number of cloner threads
        default_max_concurrent_clones = int(self.config_get('mgr', 'mgr/volumes/max_concurrent_clones'))
        self.assertEqual(default_max_concurrent_clones, 4)

        # Increase number of cloner threads
        self.config_set('mgr', 'mgr/volumes/max_concurrent_clones', 6)
        max_concurrent_clones = int(self.config_get('mgr', 'mgr/volumes/max_concurrent_clones'))
        self.assertEqual(max_concurrent_clones, 6)

        # Decrease number of cloner threads
        self.config_set('mgr', 'mgr/volumes/max_concurrent_clones', 2)
        max_concurrent_clones = int(self.config_get('mgr', 'mgr/volumes/max_concurrent_clones'))
        self.assertEqual(max_concurrent_clones, 2)

    def test_subvolume_snapshot_config_snapshot_clone_delay(self):
        """
        Validate 'snapshot_clone_delay' config option
        """

        # get the default delay before starting the clone
        default_timeout = int(self.config_get('mgr', 'mgr/volumes/snapshot_clone_delay'))
        self.assertEqual(default_timeout, 0)

        # Insert delay of 2 seconds at the beginning of the snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 2)
        default_timeout = int(self.config_get('mgr', 'mgr/volumes/snapshot_clone_delay'))
        self.assertEqual(default_timeout, 2)

        # Decrease number of cloner threads
        self.config_set('mgr', 'mgr/volumes/max_concurrent_clones', 2)
        max_concurrent_clones = int(self.config_get('mgr', 'mgr/volumes/max_concurrent_clones'))
        self.assertEqual(max_concurrent_clones, 2)

    def test_subvolume_under_group_snapshot_clone(self):
        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, group, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, subvolume_group=group, number_of_files=32)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot, group)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone, '--group_name', group)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone, source_group=group)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot, group)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, group)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()


class TestMisc(TestVolumesHelper):
    """Miscellaneous tests related to FS volume, subvolume group, and subvolume operations."""
    def test_connection_expiration(self):
        # unmount any cephfs mounts
        for i in range(0, self.CLIENTS_REQUIRED):
            self.mounts[i].umount_wait()
        sessions = self._session_list()
        self.assertLessEqual(len(sessions), 1) # maybe mgr is already mounted

        # Get the mgr to definitely mount cephfs
        subvolume = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume)
        sessions = self._session_list()
        self.assertEqual(len(sessions), 1)

        # Now wait for the mgr to expire the connection:
        self.wait_until_evicted(sessions[0]['id'], timeout=90)

    def test_mgr_eviction(self):
        # unmount any cephfs mounts
        for i in range(0, self.CLIENTS_REQUIRED):
            self.mounts[i].umount_wait()
        sessions = self._session_list()
        self.assertLessEqual(len(sessions), 1) # maybe mgr is already mounted

        # Get the mgr to definitely mount cephfs
        subvolume = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume)
        sessions = self._session_list()
        self.assertEqual(len(sessions), 1)

        # Now fail the mgr, check the session was evicted
        mgr = self.mgr_cluster.get_active_id()
        self.mgr_cluster.mgr_fail(mgr)
        self.wait_until_evicted(sessions[0]['id'])

    def test_names_can_only_be_goodchars(self):
        """
        Test the creating vols, subvols subvolgroups fails when their names uses
        characters beyond [a-zA-Z0-9 -_.].
        """
        volname, badname = 'testvol', 'abcd@#'

        with self.assertRaises(CommandFailedError):
            self._fs_cmd('volume', 'create', badname)
        self._fs_cmd('volume', 'create', volname)

        with self.assertRaises(CommandFailedError):
            self._fs_cmd('subvolumegroup', 'create', volname, badname)

        with self.assertRaises(CommandFailedError):
            self._fs_cmd('subvolume', 'create', volname, badname)
        self._fs_cmd('volume', 'rm', volname, '--yes-i-really-mean-it')

    def test_subvolume_ops_on_nonexistent_vol(self):
        # tests the fs subvolume operations on non existing volume

        volname = "non_existent_subvolume"

        # try subvolume operations
        for op in ("create", "rm", "getpath", "info", "resize", "pin", "ls"):
            try:
                if op == "resize":
                    self._fs_cmd("subvolume", "resize", volname, "subvolname_1", "inf")
                elif op == "pin":
                    self._fs_cmd("subvolume", "pin", volname, "subvolname_1", "export", "1")
                elif op == "ls":
                    self._fs_cmd("subvolume", "ls", volname)
                else:
                    self._fs_cmd("subvolume", op, volname, "subvolume_1")
            except CommandFailedError as ce:
                self.assertEqual(ce.exitstatus, errno.ENOENT)
            else:
                self.fail("expected the 'fs subvolume {0}' command to fail".format(op))

        # try subvolume snapshot operations and clone create
        for op in ("create", "rm", "info", "protect", "unprotect", "ls", "clone"):
            try:
                if op == "ls":
                    self._fs_cmd("subvolume", "snapshot", op, volname, "subvolume_1")
                elif op == "clone":
                    self._fs_cmd("subvolume", "snapshot", op, volname, "subvolume_1", "snapshot_1", "clone_1")
                else:
                    self._fs_cmd("subvolume", "snapshot", op, volname, "subvolume_1", "snapshot_1")
            except CommandFailedError as ce:
                self.assertEqual(ce.exitstatus, errno.ENOENT)
            else:
                self.fail("expected the 'fs subvolume snapshot {0}' command to fail".format(op))

        # try, clone status
        try:
            self._fs_cmd("clone", "status", volname, "clone_1")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOENT)
        else:
            self.fail("expected the 'fs clone status' command to fail")

        # try subvolumegroup operations
        for op in ("create", "rm", "getpath", "pin", "ls"):
            try:
                if op == "pin":
                    self._fs_cmd("subvolumegroup", "pin", volname, "group_1", "export", "0")
                elif op == "ls":
                    self._fs_cmd("subvolumegroup", op, volname)
                else:
                    self._fs_cmd("subvolumegroup", op, volname, "group_1")
            except CommandFailedError as ce:
                self.assertEqual(ce.exitstatus, errno.ENOENT)
            else:
                self.fail("expected the 'fs subvolumegroup {0}' command to fail".format(op))

        # try subvolumegroup snapshot operations
        for op in ("create", "rm", "ls"):
            try:
                if op == "ls":
                    self._fs_cmd("subvolumegroup", "snapshot", op, volname, "group_1")
                else:
                    self._fs_cmd("subvolumegroup", "snapshot", op, volname, "group_1", "snapshot_1")
            except CommandFailedError as ce:
                self.assertEqual(ce.exitstatus, errno.ENOENT)
            else:
                self.fail("expected the 'fs subvolumegroup snapshot {0}' command to fail".format(op))

    def test_subvolume_upgrade_legacy_to_v1(self):
        """
        poor man's upgrade test -- rather than going through a full upgrade cycle,
        emulate subvolumes by going through the wormhole and verify if they are
        accessible.
        further ensure that a legacy volume is not updated to v2.
        """
        subvolume1, subvolume2 = self._generate_random_subvolume_name(2)
        group = self._generate_random_group_name()

        # emulate a old-fashioned subvolume -- one in the default group and
        # the other in a custom group
        createpath1 = os.path.join(".", "volumes", "_nogroup", subvolume1)
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', createpath1], omit_sudo=False)

        # create group
        createpath2 = os.path.join(".", "volumes", group, subvolume2)
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', createpath2], omit_sudo=False)

        # this would auto-upgrade on access without anyone noticing
        subvolpath1 = self._fs_cmd("subvolume", "getpath", self.volname, subvolume1)
        self.assertNotEqual(subvolpath1, None)
        subvolpath1 = subvolpath1.rstrip() # remove "/" prefix and any trailing newline

        subvolpath2 = self._fs_cmd("subvolume", "getpath", self.volname, subvolume2, group)
        self.assertNotEqual(subvolpath2, None)
        subvolpath2 = subvolpath2.rstrip() # remove "/" prefix and any trailing newline

        # and... the subvolume path returned should be what we created behind the scene
        self.assertEqual(createpath1[1:], subvolpath1)
        self.assertEqual(createpath2[1:], subvolpath2)

        # ensure metadata file is in legacy location, with required version v1
        self._assert_meta_location_and_version(self.volname, subvolume1, version=1, legacy=True)
        self._assert_meta_location_and_version(self.volname, subvolume2, subvol_group=group, version=1, legacy=True)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume1)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume2, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_no_upgrade_v1_sanity(self):
        """
        poor man's upgrade test -- theme continues...

        This test is to ensure v1 subvolumes are retained as is, due to a snapshot being present, and runs through
        a series of operations on the v1 subvolume to ensure they work as expected.
        """
        subvol_md = ["atime", "bytes_pcent", "bytes_quota", "bytes_used", "created_at", "ctime",
                     "data_pool", "gid", "mode", "mon_addrs", "mtime", "path", "pool_namespace",
                     "type", "uid", "features", "state"]
        snap_md = ["created_at", "data_pool", "has_pending_clones"]

        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone1, clone2 = self._generate_random_clone_name(2)
        mode = "777"
        uid  = "1000"
        gid  = "1000"

        # emulate a v1 subvolume -- in the default group
        subvolume_path = self._create_v1_subvolume(subvolume)

        # getpath
        subvolpath = self._get_subvolume_path(self.volname, subvolume)
        self.assertEqual(subvolpath, subvolume_path)

        # ls
        subvolumes = json.loads(self._fs_cmd('subvolume', 'ls', self.volname))
        self.assertEqual(len(subvolumes), 1, "subvolume ls count mismatch, expected '1', found {0}".format(len(subvolumes)))
        self.assertEqual(subvolumes[0]['name'], subvolume,
                         "subvolume name mismatch in ls output, expected '{0}', found '{1}'".format(subvolume, subvolumes[0]['name']))

        # info
        subvol_info = json.loads(self._get_subvolume_info(self.volname, subvolume))
        for md in subvol_md:
            self.assertIn(md, subvol_info, "'{0}' key not present in metadata of subvolume".format(md))

        self.assertEqual(subvol_info["state"], "complete",
                         msg="expected state to be 'complete', found '{0}".format(subvol_info["state"]))
        self.assertEqual(len(subvol_info["features"]), 2,
                         msg="expected 1 feature, found '{0}' ({1})".format(len(subvol_info["features"]), subvol_info["features"]))
        for feature in ['snapshot-clone', 'snapshot-autoprotect']:
            self.assertIn(feature, subvol_info["features"], msg="expected feature '{0}' in subvolume".format(feature))

        # resize
        nsize = self.DEFAULT_FILE_SIZE*1024*1024*10
        self._fs_cmd("subvolume", "resize", self.volname, subvolume, str(nsize))
        subvol_info = json.loads(self._get_subvolume_info(self.volname, subvolume))
        for md in subvol_md:
            self.assertIn(md, subvol_info, "'{0}' key not present in metadata of subvolume".format(md))
        self.assertEqual(subvol_info["bytes_quota"], nsize, "bytes_quota should be set to '{0}'".format(nsize))

        # create (idempotent) (change some attrs, to ensure attrs are preserved from the snapshot on clone)
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode", mode, "--uid", uid, "--gid", gid)

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=8)

        # snap-create
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone1)

        # check clone status
        self._wait_for_clone_to_complete(clone1)

        # ensure clone is v2
        self._assert_meta_location_and_version(self.volname, clone1, version=2)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone1, source_version=1)

        # clone (older snapshot)
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, 'fake', clone2)

        # check clone status
        self._wait_for_clone_to_complete(clone2)

        # ensure clone is v2
        self._assert_meta_location_and_version(self.volname, clone2, version=2)

        # verify clone
        # TODO: rentries will mismatch till this is fixed https://tracker.ceph.com/issues/46747
        #self._verify_clone(subvolume, 'fake', clone2, source_version=1)

        # snap-info
        snap_info = json.loads(self._get_subvolume_snapshot_info(self.volname, subvolume, snapshot))
        for md in snap_md:
            self.assertIn(md, snap_info, "'{0}' key not present in metadata of snapshot".format(md))
        self.assertEqual(snap_info["has_pending_clones"], "no")

        # snap-ls
        subvol_snapshots = json.loads(self._fs_cmd('subvolume', 'snapshot', 'ls', self.volname, subvolume))
        self.assertEqual(len(subvol_snapshots), 2, "subvolume ls count mismatch, expected 2', found {0}".format(len(subvol_snapshots)))
        snapshotnames = [snapshot['name'] for snapshot in subvol_snapshots]
        for name in [snapshot, 'fake']:
            self.assertIn(name, snapshotnames, msg="expected snapshot '{0}' in subvolume snapshot ls".format(name))

        # snap-rm
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, "fake")

        # ensure volume is still at version 1
        self._assert_meta_location_and_version(self.volname, subvolume, version=1)

        # rm
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone1)
        self._fs_cmd("subvolume", "rm", self.volname, clone2)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_no_upgrade_v1_to_v2(self):
        """
        poor man's upgrade test -- theme continues...
        ensure v1 to v2 upgrades are not done automatically due to various states of v1
        """
        subvolume1, subvolume2, subvolume3 = self._generate_random_subvolume_name(3)
        group = self._generate_random_group_name()

        # emulate a v1 subvolume -- in the default group
        subvol1_path = self._create_v1_subvolume(subvolume1)

        # emulate a v1 subvolume -- in a custom group
        subvol2_path = self._create_v1_subvolume(subvolume2, subvol_group=group)

        # emulate a v1 subvolume -- in a clone pending state
        self._create_v1_subvolume(subvolume3, subvol_type='clone', has_snapshot=False, state='pending')

        # this would attempt auto-upgrade on access, but fail to do so as snapshots exist
        subvolpath1 = self._get_subvolume_path(self.volname, subvolume1)
        self.assertEqual(subvolpath1, subvol1_path)

        subvolpath2 = self._get_subvolume_path(self.volname, subvolume2, group_name=group)
        self.assertEqual(subvolpath2, subvol2_path)

        # this would attempt auto-upgrade on access, but fail to do so as volume is not complete
        # use clone status, as only certain operations are allowed in pending state
        status = json.loads(self._fs_cmd("clone", "status", self.volname, subvolume3))
        self.assertEqual(status["status"]["state"], "pending")

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume1, "fake")
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume2, "fake", group)

        # ensure metadata file is in v1 location, with version retained as v1
        self._assert_meta_location_and_version(self.volname, subvolume1, version=1)
        self._assert_meta_location_and_version(self.volname, subvolume2, subvol_group=group, version=1)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume1)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume2, group)
        try:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume3)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EAGAIN, "invalid error code on rm of subvolume undergoing clone")
        else:
            self.fail("expected rm of subvolume undergoing clone to fail")

        # ensure metadata file is in v1 location, with version retained as v1
        self._assert_meta_location_and_version(self.volname, subvolume3, version=1)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume3, "--force")

        # verify list subvolumes returns an empty list
        subvolumels = json.loads(self._fs_cmd('subvolume', 'ls', self.volname))
        self.assertEqual(len(subvolumels), 0)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_upgrade_v1_to_v2(self):
        """
        poor man's upgrade test -- theme continues...
        ensure v1 to v2 upgrades work
        """
        subvolume1, subvolume2 = self._generate_random_subvolume_name(2)
        group = self._generate_random_group_name()

        # emulate a v1 subvolume -- in the default group
        subvol1_path = self._create_v1_subvolume(subvolume1, has_snapshot=False)

        # emulate a v1 subvolume -- in a custom group
        subvol2_path = self._create_v1_subvolume(subvolume2, subvol_group=group, has_snapshot=False)

        # this would attempt auto-upgrade on access
        subvolpath1 = self._get_subvolume_path(self.volname, subvolume1)
        self.assertEqual(subvolpath1, subvol1_path)

        subvolpath2 = self._get_subvolume_path(self.volname, subvolume2, group_name=group)
        self.assertEqual(subvolpath2, subvol2_path)

        # ensure metadata file is in v2 location, with version retained as v2
        self._assert_meta_location_and_version(self.volname, subvolume1, version=2)
        self._assert_meta_location_and_version(self.volname, subvolume2, subvol_group=group, version=2)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume1)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume2, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_malicious_metafile_on_legacy_to_v1_upgrade(self):
        """
        Validate handcrafted .meta file on legacy subvol root doesn't break the system
        on legacy subvol upgrade to v1
        poor man's upgrade test -- theme continues...
        """
        subvol1, subvol2 = self._generate_random_subvolume_name(2)

        # emulate a old-fashioned subvolume in the default group
        createpath1 = os.path.join(".", "volumes", "_nogroup", subvol1)
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', createpath1], omit_sudo=False)

        # add required xattrs to subvolume
        default_pool = self.mount_a.getfattr(".", "ceph.dir.layout.pool")
        self.mount_a.setfattr(createpath1, 'ceph.dir.layout.pool', default_pool, sudo=True)

        # create v2 subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvol2)

        # Create malicious .meta file in legacy subvolume root. Copy v2 subvolume
        # .meta into legacy subvol1's root
        subvol2_metapath = os.path.join(".", "volumes", "_nogroup", subvol2, ".meta")
        self.mount_a.run_shell(['sudo', 'cp', subvol2_metapath, createpath1], omit_sudo=False)

        # Upgrade legacy subvol1 to v1
        subvolpath1 = self._fs_cmd("subvolume", "getpath", self.volname, subvol1)
        self.assertNotEqual(subvolpath1, None)
        subvolpath1 = subvolpath1.rstrip()

        # the subvolume path returned should not be of subvol2 from handcrafted
        # .meta file
        self.assertEqual(createpath1[1:], subvolpath1)

        # ensure metadata file is in legacy location, with required version v1
        self._assert_meta_location_and_version(self.volname, subvol1, version=1, legacy=True)

        # Authorize alice authID read-write access to subvol1. Verify it authorizes subvol1 path and not subvol2
        # path whose '.meta' file is copied to subvol1 root
        authid1 = "alice"
        self._fs_cmd("subvolume", "authorize", self.volname, subvol1, authid1)

        # Validate that the mds path added is of subvol1 and not of subvol2
        out = json.loads(self.fs.mon_manager.raw_cluster_cmd("auth", "get", "client.alice", "--format=json-pretty"))
        self.assertEqual("client.alice", out[0]["entity"])
        self.assertEqual("allow rw path={0}".format(createpath1[1:]), out[0]["caps"]["mds"])

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvol1)
        self._fs_cmd("subvolume", "rm", self.volname, subvol2)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_binary_metafile_on_legacy_to_v1_upgrade(self):
        """
        Validate binary .meta file on legacy subvol root doesn't break the system
        on legacy subvol upgrade to v1
        poor man's upgrade test -- theme continues...
        """
        subvol = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # emulate a old-fashioned subvolume -- in a custom group
        createpath = os.path.join(".", "volumes", group, subvol)
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', createpath], omit_sudo=False)

        # add required xattrs to subvolume
        default_pool = self.mount_a.getfattr(".", "ceph.dir.layout.pool")
        self.mount_a.setfattr(createpath, 'ceph.dir.layout.pool', default_pool, sudo=True)

        # Create unparseable binary .meta file on legacy subvol's root
        meta_contents = os.urandom(4096)
        meta_filepath = os.path.join(self.mount_a.mountpoint, createpath, ".meta")
        self.mount_a.client_remote.write_file(meta_filepath, meta_contents, sudo=True)

        # Upgrade legacy subvol to v1
        subvolpath = self._fs_cmd("subvolume", "getpath", self.volname, subvol, group)
        self.assertNotEqual(subvolpath, None)
        subvolpath = subvolpath.rstrip()

        # The legacy subvolume path should be returned for subvol.
        # Should ignore unparseable binary .meta file in subvol's root
        self.assertEqual(createpath[1:], subvolpath)

        # ensure metadata file is in legacy location, with required version v1
        self._assert_meta_location_and_version(self.volname, subvol, subvol_group=group, version=1, legacy=True)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvol, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_unparseable_metafile_on_legacy_to_v1_upgrade(self):
        """
        Validate unparseable text .meta file on legacy subvol root doesn't break the system
        on legacy subvol upgrade to v1
        poor man's upgrade test -- theme continues...
        """
        subvol = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # emulate a old-fashioned subvolume -- in a custom group
        createpath = os.path.join(".", "volumes", group, subvol)
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', createpath], omit_sudo=False)

        # add required xattrs to subvolume
        default_pool = self.mount_a.getfattr(".", "ceph.dir.layout.pool")
        self.mount_a.setfattr(createpath, 'ceph.dir.layout.pool', default_pool, sudo=True)

        # Create unparseable text .meta file on legacy subvol's root
        meta_contents = "unparseable config\nfile ...\nunparseable config\nfile ...\n"
        meta_filepath = os.path.join(self.mount_a.mountpoint, createpath, ".meta")
        self.mount_a.client_remote.write_file(meta_filepath, meta_contents, sudo=True)

        # Upgrade legacy subvol to v1
        subvolpath = self._fs_cmd("subvolume", "getpath", self.volname, subvol, group)
        self.assertNotEqual(subvolpath, None)
        subvolpath = subvolpath.rstrip()

        # The legacy subvolume path should be returned for subvol.
        # Should ignore unparseable binary .meta file in subvol's root
        self.assertEqual(createpath[1:], subvolpath)

        # ensure metadata file is in legacy location, with required version v1
        self._assert_meta_location_and_version(self.volname, subvol, subvol_group=group, version=1, legacy=True)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvol, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

class TestPerModuleFinsherThread(TestVolumesHelper):
    """
    Per module finisher thread tests related to mgr/volume cmds.
    This is used in conjuction with check_counter with min val being 4
    as four subvolume cmds are run
    """
    def test_volumes_module_finisher_thread(self):
        subvol1, subvol2, subvol3 = self._generate_random_subvolume_name(3)
        group = self._generate_random_group_name()

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolumes in group
        self._fs_cmd("subvolume", "create", self.volname, subvol1, "--group_name", group)
        self._fs_cmd("subvolume", "create", self.volname, subvol2, "--group_name", group)
        self._fs_cmd("subvolume", "create", self.volname, subvol3, "--group_name", group)

        self._fs_cmd("subvolume", "rm", self.volname, subvol1, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvol2, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvol3, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()
