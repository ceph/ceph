import os
import json
import time
import errno
import random
import logging
import collections

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)

class TestVolumes(CephFSTestCase):
    TEST_VOLUME_PREFIX = "volume"
    TEST_SUBVOLUME_PREFIX="subvolume"
    TEST_GROUP_PREFIX="group"
    TEST_SNAPSHOT_PREFIX="snapshot"
    TEST_CLONE_PREFIX="clone"
    TEST_FILE_NAME_PREFIX="subvolume_file"

    # for filling subvolume with data
    CLIENTS_REQUIRED = 1
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

    def _wait_for_clone_to_complete(self, clone, clone_group=None, timo=120):
        self.__check_clone_state("complete", clone, clone_group, timo)

    def _wait_for_clone_to_fail(self, clone, clone_group=None, timo=120):
        self.__check_clone_state("failed", clone, clone_group, timo)

    def _check_clone_canceled(self, clone, clone_group=None):
        self.__check_clone_state("canceled", clone, clone_group, timo=1)

    def _verify_clone_attrs(self, subvolume, clone, source_group=None, clone_group=None):
        path1 = self._get_subvolume_path(self.volname, subvolume, group_name=source_group)
        path2 = self._get_subvolume_path(self.volname, clone, group_name=clone_group)

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
            sval = int(self.mount_a.run_shell(['stat', '-c' '%X', source_path]).stdout.getvalue().strip())
            cval = int(self.mount_a.run_shell(['stat', '-c' '%X', sink_path]).stdout.getvalue().strip())
            self.assertEqual(sval, cval)

            sval = int(self.mount_a.run_shell(['stat', '-c' '%Y', source_path]).stdout.getvalue().strip())
            cval = int(self.mount_a.run_shell(['stat', '-c' '%Y', sink_path]).stdout.getvalue().strip())
            self.assertEqual(sval, cval)

    def _verify_clone(self, subvolume, clone, source_group=None, clone_group=None, timo=120):
        path1 = self._get_subvolume_path(self.volname, subvolume, group_name=source_group)
        path2 = self._get_subvolume_path(self.volname, clone, group_name=clone_group)

        check = 0
        while check < timo:
            val1 = int(self.mount_a.getfattr(path1, "ceph.dir.rentries"))
            val2 = int(self.mount_a.getfattr(path2, "ceph.dir.rentries"))
            if val1 == val2:
                break
            check += 1
            time.sleep(1)
        self.assertTrue(check < timo)

        self._verify_clone_attrs(subvolume, clone, source_group=source_group, clone_group=clone_group)

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

    def  _get_subvolume_group_path(self, vol_name, group_name):
        args = ("subvolumegroup", "getpath", vol_name, group_name)
        path = self._fs_cmd(*args)
        # remove the leading '/', and trailing whitespaces
        return path[1:].rstrip()

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
            self.mount_a.run_shell(["mkdir", "-p", io_path])

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

        self.mount_a.run_shell(["sudo", "mkdir", dir_path], omit_sudo=False)
        self.mount_a.run_shell(["sudo", "ln", "-s", "./{}".format(reg_file), sym_path1], omit_sudo=False)
        self.mount_a.run_shell(["sudo", "ln", "-s", "./{}".format(reg_file), sym_path2], omit_sudo=False)
        # flip ownership to nobody. assumption: nobody's id is 65534
        self.mount_a.run_shell(["sudo", "chown", "-h", "65534:65534", sym_path2], omit_sudo=False)

    def _wait_for_trash_empty(self, timeout=30):
        # XXX: construct the trash dir path (note that there is no mgr
        # [sub]volume interface for this).
        trashdir = os.path.join("./", "volumes", "_deleting")
        self.mount_a.wait_for_dir_empty(trashdir, timeout=timeout)

    def setUp(self):
        super(TestVolumes, self).setUp()
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
        super(TestVolumes, self).tearDown()

    def test_connection_expiration(self):
        # unmount any cephfs mounts
        self.mount_a.umount_wait()
        sessions = self._session_list()
        self.assertLessEqual(len(sessions), 1) # maybe mgr is already mounted

        # Get the mgr to definitely mount cephfs
        subvolume = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume)
        sessions = self._session_list()
        self.assertEqual(len(sessions), 1)

        # Now wait for the mgr to expire the connection:
        self.wait_until_evicted(sessions[0]['id'], timeout=90)

    def test_volume_create(self):
        """
        That the volume can be created and then cleans up
        """
        volname = self._generate_random_volume_name()
        self._fs_cmd("volume", "create", volname)
        volumels = json.loads(self._fs_cmd("volume", "ls"))

        if not (volname in ([volume['name'] for volume in volumels])):
            raise RuntimeError("Error creating volume '{0}'".format(volname))
        else:
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
        volumenames = self._generate_random_volume_name(3)
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

    ### basic subvolume operations

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
            if ce.exitstatus != errno.EINVAL:
                raise
        else:
            raise RuntimeError("expected the 'fs subvolume resize' command to fail")

        # verify the quota did not change
        size = int(self.mount_a.getfattr(subvolpath, "ceph.quota.max_bytes"))
        self.assertEqual(size, osize)

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
            if ce.exitstatus != errno.EINVAL:
                raise
        else:
            raise RuntimeError("expected the 'fs subvolume resize' command to fail")

        # verify the quota did not change
        size = int(self.mount_a.getfattr(subvolpath, "ceph.quota.max_bytes"))
        self.assertEqual(size, osize)

    def test_subvolume_resize_quota_lt_used_size(self):
        """
        That a subvolume can be resized to a size smaller than the current used size
        and the resulting quota matches the expected size.
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024*20
        # create subvolume
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--size", str(osize))

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
        self.assertEqual(usedsize, susedsize)

        # shrink the subvolume
        nsize = usedsize // 2
        try:
            self._fs_cmd("subvolume", "resize", self.volname, subvolname, str(nsize))
        except CommandFailedError:
            raise RuntimeError("expected the 'fs subvolume resize' command to succeed")

        # verify the quota
        size = int(self.mount_a.getfattr(subvolpath, "ceph.quota.max_bytes"))
        self.assertEqual(size, nsize)


    def test_subvolume_resize_fail_quota_lt_used_size_no_shrink(self):
        """
        That a subvolume cannot be resized to a size smaller than the current used size
        when --no_shrink is given and the quota did not change.
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024*20
        # create subvolume
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--size", str(osize))

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
        self.assertEqual(usedsize, susedsize)

        # shrink the subvolume
        nsize = usedsize // 2
        try:
            self._fs_cmd("subvolume", "resize", self.volname, subvolname, str(nsize), "--no_shrink")
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EINVAL:
                raise
        else:
            raise RuntimeError("expected the 'fs subvolume resize' command to fail")

        # verify the quota did not change
        size = int(self.mount_a.getfattr(subvolpath, "ceph.quota.max_bytes"))
        self.assertEqual(size, osize)

    def test_subvolume_resize_expand_on_full_subvolume(self):
        """
        That the subvolume can be expanded from a full subvolume and future writes succeed.
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024*10
        # create subvolume of quota 10MB and make sure it exists
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--size", str(osize))
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
                raise RuntimeError("expected filling subvolume {0} with {1} file of size {2}MB"
                                   "to succeed".format(subvolname, number_of_files, file_size))
        else:
            raise RuntimeError("expected filling subvolume {0} with {1} file of size {2}MB"
                               "to fail".format(subvolname, number_of_files, file_size))

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

    def test_subvolumegroup_pin_distributed(self):
        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()
        self.config_set('mds', 'mds_export_ephemeral_distributed', True)

        group = "pinme"
        self._fs_cmd("subvolumegroup", "create", self.volname, group)
        self._fs_cmd("subvolumegroup", "pin", self.volname, group, "distributed", "True")
        # (no effect on distribution) pin the group directory to 0 so rank 0 has all subtree bounds visible
        self._fs_cmd("subvolumegroup", "pin", self.volname, group, "export", "0")
        subvolumes = self._generate_random_subvolume_name(10)
        for subvolume in subvolumes:
            self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)
        self._wait_distributed_subtrees(10, status=status)

    def test_subvolume_pin_random(self):
        self.fs.set_max_mds(2)
        self.fs.wait_for_daemons()
        self.config_set('mds', 'mds_export_ephemeral_random', True)

        subvolume = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume)
        self._fs_cmd("subvolume", "pin", self.volname, subvolume, "random", ".01")
        # no verification

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

    def test_subvolume_create_with_invalid_data_pool_layout(self):
        subvolume = self._generate_random_subvolume_name()
        data_pool = "invalid_pool"
        # create subvolume with invalid data pool layout
        try:
            self._fs_cmd("subvolume", "create", self.volname, subvolume, "--pool_layout", data_pool)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EINVAL:
                raise
        else:
            raise RuntimeError("expected the 'fs subvolume create' command to fail")

    def test_subvolume_rm_force(self):
        # test removing non-existing subvolume with --force
        subvolume = self._generate_random_subvolume_name()
        try:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--force")
        except CommandFailedError:
            raise RuntimeError("expected the 'fs subvolume rm --force' command to succeed")

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
            if ce.exitstatus != errno.ENOENT:
                raise
        else:
            raise RuntimeError("expected the 'fs subvolume getpath' command to fail")

    def test_subvolume_create_with_invalid_size(self):
        # create subvolume with an invalid size -1
        subvolume = self._generate_random_subvolume_name()
        try:
            self._fs_cmd("subvolume", "create", self.volname, subvolume, "--size", "-1")
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EINVAL:
                raise
        else:
            raise RuntimeError("expected the 'fs subvolume create' command to fail")

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
            raise RuntimeError("Expected the 'fs subvolume ls' command to list the created subvolumes.")
        else:
            subvolnames = [subvolume['name'] for subvolume in subvolumels]
            if collections.Counter(subvolnames) != collections.Counter(subvolumes):
                raise RuntimeError("Error creating or listing subvolumes")

    def test_subvolume_ls_for_notexistent_default_group(self):
        # tests the 'fs subvolume ls' command when the default group '_nogroup' doesn't exist
        # prerequisite: we expect that the volume is created and the default group _nogroup is
        # NOT created (i.e. a subvolume without group is not created)

        # list subvolumes
        subvolumels = json.loads(self._fs_cmd('subvolume', 'ls', self.volname))
        if len(subvolumels) > 0:
            raise RuntimeError("Expected the 'fs subvolume ls' command to output an empty list.")

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

    def test_subvolume_resize_infinite_size_future_writes(self):
        """
        That a subvolume can be resized to an infinite size and the future writes succeed.
        """

        # create subvolume
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--size",
                     str(self.DEFAULT_FILE_SIZE*1024*1024*5))

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
            raise RuntimeError("expected filling subvolume {0} with {1} file of size {2}MB "
                               "to succeed".format(subvolname, number_of_files, file_size))

    def test_subvolume_info(self):
        # tests the 'fs subvolume info' command

        subvol_md = ["atime", "bytes_pcent", "bytes_quota", "bytes_used", "created_at", "ctime",
                     "data_pool", "gid", "mode", "mon_addrs", "mtime", "path", "pool_namespace",
                     "type", "uid", "features"]

        # create subvolume
        subvolume = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # get subvolume metadata
        subvol_info = json.loads(self._get_subvolume_info(self.volname, subvolume))
        self.assertNotEqual(len(subvol_info), 0, "expected the 'fs subvolume info' command to list metadata of subvolume")
        for md in subvol_md:
            self.assertIn(md, subvol_info.keys(), "'{0}' key not present in metadata of subvolume".format(md))

        self.assertEqual(subvol_info["bytes_pcent"], "undefined", "bytes_pcent should be set to undefined if quota is not set")
        self.assertEqual(subvol_info["bytes_quota"], "infinite", "bytes_quota should be set to infinite if quota is not set")
        self.assertEqual(subvol_info["pool_namespace"], "", "expected pool namespace to be empty")

        self.assertEqual(len(subvol_info["features"]), 2,
                         msg="expected 2 features, found '{0}' ({1})".format(len(subvol_info["features"]), subvol_info["features"]))
        for feature in ['snapshot-clone', 'snapshot-autoprotect']:
            self.assertIn(feature, subvol_info["features"], msg="expected feature '{0}' in subvolume".format(feature))

        nsize = self.DEFAULT_FILE_SIZE*1024*1024
        self._fs_cmd("subvolume", "resize", self.volname, subvolume, str(nsize))

        # get subvolume metadata after quota set
        subvol_info = json.loads(self._get_subvolume_info(self.volname, subvolume))
        self.assertNotEqual(len(subvol_info), 0, "expected the 'fs subvolume info' command to list metadata of subvolume")

        self.assertNotEqual(subvol_info["bytes_pcent"], "undefined", "bytes_pcent should not be set to undefined if quota is not set")
        self.assertNotEqual(subvol_info["bytes_quota"], "infinite", "bytes_quota should not be set to infinite if quota is not set")
        self.assertEqual(subvol_info["type"], "subvolume", "type should be set to subvolume")

        self.assertEqual(len(subvol_info["features"]), 2,
                         msg="expected 2 features, found '{0}' ({1})".format(len(subvol_info["features"]), subvol_info["features"]))
        for feature in ['snapshot-clone', 'snapshot-autoprotect']:
            self.assertIn(feature, subvol_info["features"], msg="expected feature '{0}' in subvolume".format(feature))

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_clone_subvolume_info(self):

        # tests the 'fs subvolume info' command for a clone
        subvol_md = ["atime", "bytes_pcent", "bytes_quota", "bytes_used", "created_at", "ctime",
                     "data_pool", "gid", "mode", "mon_addrs", "mtime", "path", "pool_namespace",
                     "type", "uid"]

        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

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


    ### subvolume group operations

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

    def test_subvolume_group_create_with_desired_data_pool_layout(self):
        group1, group2 = self._generate_random_group_name(2)

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group1)
        group1_path = self._get_subvolume_group_path(self.volname, group1)

        default_pool = self.mount_a.getfattr(group1_path, "ceph.dir.layout.pool")
        new_pool = "new_pool"
        self.assertNotEqual(default_pool, new_pool)

        # add data pool
        self.fs.add_data_pool(new_pool)

        # create group specifying the new data pool as its pool layout
        self._fs_cmd("subvolumegroup", "create", self.volname, group2,
                     "--pool_layout", new_pool)
        group2_path = self._get_subvolume_group_path(self.volname, group2)

        desired_pool = self.mount_a.getfattr(group2_path, "ceph.dir.layout.pool")
        self.assertEqual(desired_pool, new_pool)

        self._fs_cmd("subvolumegroup", "rm", self.volname, group1)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group2)

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

    def test_subvolume_group_rm_force(self):
        # test removing non-existing subvolume group with --force
        group = self._generate_random_group_name()
        try:
            self._fs_cmd("subvolumegroup", "rm", self.volname, group, "--force")
        except CommandFailedError:
            raise RuntimeError("expected the 'fs subvolumegroup rm --force' command to succeed")

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
        self.fs.add_data_pool(new_pool)

        # create subvolume specifying the new data pool as its pool layout
        self._fs_cmd("subvolume", "create", self.volname, subvol2, "--group_name", group,
                     "--pool_layout", new_pool)
        subvol2_path = self._get_subvolume_path(self.volname, subvol2, group_name=group)

        desired_pool = self.mount_a.getfattr(subvol2_path, "ceph.dir.layout.pool")
        self.assertEqual(desired_pool, new_pool)

        self._fs_cmd("subvolume", "rm", self.volname, subvol2, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvol1, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_group_create_with_desired_mode(self):
        group1, group2 = self._generate_random_group_name(2)
        # default mode
        expected_mode1 = "755"
        # desired mode
        expected_mode2 = "777"

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group1)
        self._fs_cmd("subvolumegroup", "create", self.volname, group2, "--mode", "777")

        group1_path = self._get_subvolume_group_path(self.volname, group1)
        group2_path = self._get_subvolume_group_path(self.volname, group2)

        # check group's mode
        actual_mode1 = self.mount_a.run_shell(['stat', '-c' '%a', group1_path]).stdout.getvalue().strip()
        actual_mode2 = self.mount_a.run_shell(['stat', '-c' '%a', group2_path]).stdout.getvalue().strip()
        self.assertEqual(actual_mode1, expected_mode1)
        self.assertEqual(actual_mode2, expected_mode2)

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

    def test_subvolume_group_ls_for_nonexistent_volume(self):
        # tests the 'fs subvolumegroup ls' command when /volume doesn't exist
        # prerequisite: we expect that the test volume is created and a subvolumegroup is NOT created

        # list subvolume groups
        subvolumegroupls = json.loads(self._fs_cmd('subvolumegroup', 'ls', self.volname))
        if len(subvolumegroupls) > 0:
            raise RuntimeError("Expected the 'fs subvolumegroup ls' command to output an empty list")

    ### snapshot operations

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

    def test_subvolume_snapshot_info(self):

        """
        tests the 'fs subvolume snapshot info' command
        """

        snap_metadata = ["created_at", "data_pool", "has_pending_clones", "size"]

        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=1)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        snap_info = json.loads(self._get_subvolume_snapshot_info(self.volname, subvolume, snapshot))
        self.assertNotEqual(len(snap_info), 0)
        for md in snap_metadata:
            if md not in snap_info:
                raise RuntimeError("%s not present in the metadata of subvolume snapshot" % md)
        self.assertEqual(snap_info["has_pending_clones"], "no")

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

    def test_subvolume_snapshot_rm_force(self):
        # test removing non existing subvolume snapshot with --force
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()

        # remove snapshot
        try:
            self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot, "--force")
        except CommandFailedError:
            raise RuntimeError("expected the 'fs subvolume snapshot rm --force' command to succeed")

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
            raise RuntimeError("Expected the 'fs subvolume snapshot ls' command to list the created subvolume snapshots")
        else:
            snapshotnames = [snapshot['name'] for snapshot in subvolsnapshotls]
            if collections.Counter(snapshotnames) != collections.Counter(snapshots):
                raise RuntimeError("Error creating or listing subvolume snapshots")

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

    def test_subvolume_group_snapshot_rm_force(self):
        # test removing non-existing subvolume group snapshot with --force
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()
        # remove snapshot
        try:
            self._fs_cmd("subvolumegroup", "snapshot", "rm", self.volname, group, snapshot, "--force")
        except CommandFailedError:
            raise RuntimeError("expected the 'fs subvolumegroup snapshot rm --force' command to succeed")

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

    def test_async_subvolume_rm(self):
        subvolumes = self._generate_random_subvolume_name(100)

        # create subvolumes
        for subvolume in subvolumes:
            self._fs_cmd("subvolume", "create", self.volname, subvolume)
            self._do_subvolume_io(subvolume, number_of_files=10)

        self.mount_a.umount_wait()

        # remove subvolumes
        for subvolume in subvolumes:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        self.mount_a.mount_wait()

        # verify trash dir is clean
        self._wait_for_trash_empty(timeout=300)

    def test_mgr_eviction(self):
        # unmount any cephfs mounts
        self.mount_a.umount_wait()
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

    def test_subvolume_upgrade(self):
        """
        poor man's upgrade test -- rather than going through a full upgrade cycle,
        emulate subvolumes by going through the wormhole and verify if they are
        accessible.
        """
        subvolume1, subvolume2 = self._generate_random_subvolume_name(2)
        group = self._generate_random_group_name()

        # emulate a old-fashioned subvolume -- one in the default group and
        # the other in a custom group
        createpath1 = os.path.join(".", "volumes", "_nogroup", subvolume1)
        self.mount_a.run_shell(['mkdir', '-p', createpath1])

        # create group
        createpath2 = os.path.join(".", "volumes", group, subvolume2)
        self.mount_a.run_shell(['mkdir', '-p', createpath2])

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

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume1)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume2, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

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
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

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

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # verify clone
        self._verify_clone(subvolume, clone)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=64)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # verify clone
        self._verify_clone(subvolume, clone)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone_pool_layout(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        # add data pool
        new_pool = "new_pool"
        self.fs.add_data_pool(new_pool)

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=32)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone, "--pool_layout", new_pool)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # verify clone
        self._verify_clone(subvolume, clone)

        subvol_path = self._get_subvolume_path(self.volname, clone)
        desired_pool = self.mount_a.getfattr(subvol_path, "ceph.dir.layout.pool")
        self.assertEqual(desired_pool, new_pool)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone_with_attrs(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        mode = "777"
        uid  = "1000"
        gid  = "1000"

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode", mode, "--uid", uid, "--gid", gid)

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=32)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # verify clone
        self._verify_clone(subvolume, clone)

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
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=32)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone1)

        # check clone status
        self._wait_for_clone_to_complete(clone1)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # verify clone
        self._verify_clone(subvolume, clone1)

        # now the clone is just like a normal subvolume -- snapshot the clone and fork
        # another clone. before that do some IO so it's can be differentiated.
        self._do_subvolume_io(clone1, create_dir="data", number_of_files=32)

        # snapshot clone -- use same snap name
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, clone1, snapshot)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, clone1, snapshot, clone2)

        # check clone status
        self._wait_for_clone_to_complete(clone2)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, clone1, snapshot)

        # verify clone
        self._verify_clone(clone1, clone2)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone1)
        self._fs_cmd("subvolume", "rm", self.volname, clone2)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone_under_group(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()
        group = self._generate_random_group_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

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

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # verify clone
        self._verify_clone(subvolume, clone, clone_group=group)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone, group)

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_under_group_snapshot_clone(self):
        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, group)

        # do some IO
        self._do_subvolume_io(subvolume, subvolume_group=group, number_of_files=32)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot, group)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone, '--group_name', group)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot, group)

        # verify clone
        self._verify_clone(subvolume, clone, source_group=group)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, group)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

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
        self._fs_cmd("subvolume", "create", self.volname, subvolume, s_group)

        # do some IO
        self._do_subvolume_io(subvolume, subvolume_group=s_group, number_of_files=32)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot, s_group)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone,
                     '--group_name', s_group, '--target_group_name', c_group)

        # check clone status
        self._wait_for_clone_to_complete(clone, clone_group=c_group)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot, s_group)

        # verify clone
        self._verify_clone(subvolume, clone, source_group=s_group, clone_group=c_group)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, s_group)
        self._fs_cmd("subvolume", "rm", self.volname, clone, c_group)

        # remove groups
        self._fs_cmd("subvolumegroup", "rm", self.volname, s_group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, c_group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone_with_upgrade(self):
        """
        yet another poor man's upgrade test -- rather than going through a full
        upgrade cycle, emulate old types subvolumes by going through the wormhole
        and verify clone operation.
        """
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        # emulate a old-fashioned subvolume
        createpath = os.path.join(".", "volumes", "_nogroup", subvolume)
        self.mount_a.run_shell(['mkdir', '-p', createpath])

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=64)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

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

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # verify clone
        self._verify_clone(subvolume, clone)

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
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=64)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

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

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # verify clone
        self._verify_clone(subvolume, clone)

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
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=64)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

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

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # verify clone
        self._verify_clone(subvolume, clone)

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
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=64)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

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

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # verify clone
        self._verify_clone(subvolume, clone)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

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

    def test_subvolume_snapshot_clone_on_existing_subvolumes(self):
        subvolume1, subvolume2 = self._generate_random_subvolume_name(2)
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        # create subvolumes
        self._fs_cmd("subvolume", "create", self.volname, subvolume1)
        self._fs_cmd("subvolume", "create", self.volname, subvolume2)

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

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume1, snapshot)

        # verify clone
        self._verify_clone(subvolume1, clone)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume1)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume2)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

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
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

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
        self._verify_clone(subvolume, clone1)

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

    def test_subvolume_snapshot_attr_clone(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # do some IO
        self._do_subvolume_io_mixed(subvolume)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # verify clone
        self._verify_clone(subvolume, clone)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone_cancel_in_progress(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=128)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

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
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

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
