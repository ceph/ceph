import json
import errno
import random
import logging

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)

class TestVolumes(CephFSTestCase):
    TEST_VOLUME_NAME = "fs_test_vol"
    TEST_SUBVOLUME_PREFIX="subvolume"
    TEST_GROUP_PREFIX="group"
    TEST_SNAPSHOT_PREFIX="snapshot"

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

    def _delete_test_volume(self):
        self._fs_cmd("volume", "rm", self.volname)

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

    def test_subvolume_create_idempotence(self):
        # create subvolume
        subvolume = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # try creating w/ same subvolume name -- should be idempotent
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

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

        # remove group
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

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

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

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)
