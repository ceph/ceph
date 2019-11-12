import os
import json
import errno
import random
import logging
import collections

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)

class TestVolumes(CephFSTestCase):
    TEST_VOLUME_NAME = "fs_test_vol"
    TEST_SUBVOLUME_PREFIX="subvolume"
    TEST_GROUP_PREFIX="group"
    TEST_SNAPSHOT_PREFIX="snapshot"
    TEST_FILE_NAME_PREFIX="subvolume_file"

    # for filling subvolume with data
    CLIENTS_REQUIRED = 1

    # io defaults
    DEFAULT_FILE_SIZE = 1 # MB
    DEFAULT_NUMBER_OF_FILES = 1024

    def _fs_cmd(self, *args):
        return self.mgr_cluster.mon_manager.raw_cluster_cmd("fs", *args)

    def _generate_random_subvolume_name(self):
        return "{0}_{1}".format(TestVolumes.TEST_SUBVOLUME_PREFIX, random.randint(0, 10000))

    def _generate_random_group_name(self):
        return "{0}_{1}".format(TestVolumes.TEST_GROUP_PREFIX, random.randint(0, 100))

    def _generate_random_snapshot_name(self):
        return "{0}_{1}".format(TestVolumes.TEST_SNAPSHOT_PREFIX, random.randint(0, 100))

    def _enable_multi_fs(self):
        self._fs_cmd("flag", "set", "enable_multiple", "true", "--yes-i-really-mean-it")

    def _create_or_reuse_test_volume(self):
        result = json.loads(self._fs_cmd("volume", "ls"))
        if len(result) == 0:
            self.vol_created = True
            self.volname = TestVolumes.TEST_VOLUME_NAME
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

    def _delete_test_volume(self):
        self._fs_cmd("volume", "rm", self.volname, "--yes-i-really-mean-it")

    def _do_subvolume_io(self, subvolume, number_of_files=DEFAULT_NUMBER_OF_FILES,
                         file_size=DEFAULT_FILE_SIZE):
        # get subvolume path for IO
        subvolpath = self._fs_cmd("subvolume", "getpath", self.volname, subvolume)
        self.assertNotEqual(subvolpath, None)
        subvolpath = subvolpath[1:].rstrip() # remove "/" prefix and any trailing newline

        log.debug("filling subvolume {0} with {1} files each {2}MB size".format(subvolume, number_of_files, file_size))
        for i in range(number_of_files):
            filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, i)
            self.mount_a.write_n_mb(os.path.join(subvolpath, filename), file_size)

    def _wait_for_trash_empty(self, timeout=30):
        # XXX: construct the trash dir path (note that there is no mgr
        # [sub]volume interface for this).
        trashdir = os.path.join("./", "volumes", "_deleting")
        self.mount_a.wait_for_dir_empty(trashdir)

    def setUp(self):
        super(TestVolumes, self).setUp()
        self.volname = None
        self.vol_created = False
        self._enable_multi_fs()
        self._create_or_reuse_test_volume()

    def tearDown(self):
        if self.vol_created:
            self._delete_test_volume()
        super(TestVolumes, self).tearDown()

    def test_volume_rm(self):
        try:
            self._fs_cmd("volume", "rm", self.volname)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EPERM:
                raise RuntimeError("expected the 'fs volume rm' command to fail with EPERM, "
                                   "but it failed with {0}".format(ce.exitstatus))
            else:
                self._fs_cmd("volume", "rm", self.volname, "--yes-i-really-mean-it")

                #check if it's gone
                volumes = json.loads(self.mgr_cluster.mon_manager.raw_cluster_cmd('fs', 'volume', 'ls', '--format=json-pretty'))
                if (self.volname in [volume['name'] for volume in volumes]):
                    raise RuntimeError("Expected the 'fs volume rm' command to succeed. The volume {0} not removed.".format(self.volname))
        else:
            raise RuntimeError("expected the 'fs volume rm' command to fail.")

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
        nsize = osize/2
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
        nsize = usedsize/2
        try:
            self._fs_cmd("subvolume", "resize", self.volname, subvolname, str(nsize))
        except CommandFailedError as ce:
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
        nsize = usedsize/2
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
        file_size=file_size/2
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
            raise
        # clean up
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--force")

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
            raise

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

        # force remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--force")

    def test_nonexistent_subvolume_group_create(self):
        subvolume = self._generate_random_subvolume_name()
        group = "non_existent_group"

        # try, creating subvolume in a nonexistent group
        try:
            self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOENT:
                raise

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
        for i in range(3):
            svname = self._generate_random_subvolume_name()
            self._fs_cmd("subvolume", "create", self.volname, svname)
            subvolumes.append(svname)

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
        group1 = self._generate_random_group_name()
        group2 = self._generate_random_group_name()

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
            raise
        # clean up
        self._fs_cmd("subvolumegroup", "rm", self.volname, group, "--force")

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
            raise

    def test_subvolume_create_with_desired_data_pool_layout_in_group(self):
        subvol1 = self._generate_random_subvolume_name()
        subvol2 = self._generate_random_subvolume_name()
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
        group1 = self._generate_random_group_name()
        group2 = self._generate_random_group_name()
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

    def test_subvolume_create_with_desired_mode_in_group(self):
        subvol1 = self._generate_random_subvolume_name()
        subvol2 = self._generate_random_subvolume_name()
        subvol3 = self._generate_random_subvolume_name()
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

    def test_nonexistent_subvolme_group_rm(self):
        group = "non_existent_group"

        # try, remove subvolume group
        try:
            self._fs_cmd("subvolumegroup", "rm", self.volname, group)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOENT:
                raise

        # force remove subvolume
        self._fs_cmd("subvolumegroup", "rm", self.volname, group, "--force")

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
        for i in range(3):
            groupname = self._generate_random_group_name()
            self._fs_cmd("subvolumegroup", "create", self.volname, groupname)
            subvolumegroups.append(groupname)

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

        # force remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot, "--force")

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
        for i in range(3):
            sname = self._generate_random_snapshot_name()
            self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, sname)
            snapshots.append(sname)

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

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_group_snapshot_ls(self):
        # tests the 'fs subvolumegroup snapshot ls' command

        snapshots = []

        # create group
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolumegroup snapshots
        for i in range(3):
            sname = self._generate_random_snapshot_name()
            self._fs_cmd("subvolumegroup", "snapshot", "create", self.volname, group, sname)
            snapshots.append(sname)

        subvolgrpsnapshotls = json.loads(self._fs_cmd('subvolumegroup', 'snapshot', 'ls', self.volname, group))
        if len(subvolgrpsnapshotls) == 0:
            raise RuntimeError("Expected the 'fs subvolumegroup snapshot ls' command to list the created subvolume group snapshots")
        else:
            snapshotnames = [snapshot['name'] for snapshot in subvolgrpsnapshotls]
            if collections.Counter(snapshotnames) != collections.Counter(snapshots):
                raise RuntimeError("Error creating or listing subvolume group snapshots")

    def test_async_subvolume_rm(self):
        subvolume = self._generate_random_subvolume_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # fill subvolume w/ some data
        self._do_subvolume_io(subvolume)

        self.mount_a.umount_wait()

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        self.mount_a.mount()

        # verify trash dir is clean
        self._wait_for_trash_empty()
