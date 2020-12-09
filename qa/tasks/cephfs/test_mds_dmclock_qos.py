from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError
from pathlib import Path
import logging
import time
import random

log = logging.getLogger(__name__)


class TestMDSDmclockQoS(CephFSTestCase):

    CLIENTS_REQUIRED = 2
    MDSS_REQUIRED = 1
    REQUIRE_FILESYSTEM = True

    TEST_SUBVOLUME_PREFIX = "subvolume_"
    TEST_SUBVOLUME_COUNT = 2
    TEST_DIR_COUNT = 20
    TEST_COUNT = 1000
    subvolumes = []

    def setUp(self):
        super(TestMDSDmclockQoS, self).setUp()

        self.fs.set_max_mds(len(self.fs.mds_ids))

        # create test subvolumes
        self.subvolumes = [self.TEST_SUBVOLUME_PREFIX + str(i)\
                for i in range(self.TEST_SUBVOLUME_COUNT)]

        # create subvolumes
        for subv_ in self.subvolumes:
            for retry in range(5):
                try:
                    time.sleep(1)
                    self._fs_cmd("subvolume", "create", self.fs.name, subv_)
                    break
                except CommandFailedError:
                    log.info('create subvolume command failed.')

        if self.mount_a.mounted:
            self.mount_a.umount_wait()
        if self.mount_b.mounted:
            self.mount_b.umount_wait()

        self.mount_a.mount(cephfs_mntpt=self.get_subvolume_path(self.subvolumes[0]))
        self.mount_b.mount(cephfs_mntpt=self.get_subvolume_path(self.subvolumes[1]))

        # chown root to user
        from os import getuid, getgid
        self.mount_a.run_shell(['sudo', 'chown', "{0}:{1}".format(getuid(), getgid()), self.mount_a.hostfs_mntpt])
        self.mount_b.run_shell(['sudo', 'chown', "{0}:{1}".format(getuid(), getgid()), self.mount_b.hostfs_mntpt])

    def tearDown(self):
        super(TestMDSDmclockQoS, self).tearDown()

        self.mount_a.umount_wait()
        self.mount_b.umount_wait()

        for subv_ in self.subvolumes:
            self._fs_cmd("subvolume", "rm", self.fs.name, subv_)

        self.fs.set_max_mds(1)

    def _fs_cmd(self, *args):
        return self.mgr_cluster.mon_manager.raw_cluster_cmd("-c", "ceph.conf", "-k", "keyring", "fs", *args)

    def mds_asok_all(self, commands):
        """
        admin socket command to all active mds
        """
        from copy import deepcopy

        result = []
        origin = deepcopy(commands)
        for id in self.fs.mds_ids:
            result.append(self.fs.mds_asok(commands, mds_id=id))
            commands = deepcopy(origin)
        return result

    def get_subvolume_path(self, subvolume_name):
        return self._fs_cmd("subvolume", "getpath", self.fs.name, subvolume_name).rstrip()

    def enable_qos(self, id=None):
        if id:
            self.fs.mds_asok(["config", "set", "mds_dmclock_enable", "true"], mds_id=id)
        else:
            self.mds_asok_all(["config", "set", "mds_dmclock_enable", "true"])

    def disable_qos(self, id=None):
        if id:
            self.fs.mds_asok(["config", "set", "mds_dmclock_enable", "false"], mds_id=id)
        else:
            self.mds_asok_all(["config", "set", "mds_dmclock_enable", "false"])

    def assertBetween(self, result, res, lim):
        delta = 0.5  # TODO: find appropriate value

        self.assertGreaterEqual(result, res * (1 - delta))
        self.assertLessEqual(result, lim * (1 + delta))

    def is_equal_dict(self, a, b, ignore_key=[]):
        ignore_key = set(ignore_key)
        for key in a:
            if key in ignore_key:
                continue
            if key not in b:
                return False
            if a[key] != b[key]:
                return False
        return True

    def get_subvolume_root(self, mount):
        if mount == self.mount_a:
            return Path(self.get_subvolume_path(self.subvolumes[0])).parent

        return Path(self.get_subvolume_path(self.subvolumes[1])).parent

    def remount_to_mntpt(self, mount, mntpt):
        mount.umount_wait()
        mount.mount(cephfs_mntpt=str(mntpt))

    def remount_subvolume_path_root(self, mount):
        mount.umount_wait()
        if mount == self.mount_a:
            mount.mount(cephfs_mntpt=self.get_subvolume_path(self.subvolumes[0]))
        else:
            mount.mount(cephfs_mntpt=self.get_subvolume_path(self.subvolumes[1]))

    def set_qos_xattr(self, mount, reservation, weight, limit):
        from os import getuid, getgid
        mount.run_shell(['sudo', 'chown', "{0}:{1}".format(getuid(), getgid()), mount.hostfs_mntpt])

        mount.setfattr(mount.hostfs_mntpt, "ceph.dmclock.mds_reservation", str(reservation))
        mount.setfattr(mount.hostfs_mntpt, "ceph.dmclock.mds_limit", str(limit))
        mount.setfattr(mount.hostfs_mntpt, "ceph.dmclock.mds_weight", str(weight))

    def verify_qos_info(self, mount, reservation, weight, limit):
        for id in self.fs.mds_ids:
            stat_qos = self.dump_qos(id)
            for info in stat_qos[1]:
                self.assertIn("volume_id", info)
                if info["volume_id"] == str(self.get_subvolume_root(mount)):
                    self.assertEqual(info["reservation"], reservation)
                    self.assertEqual(info["weight"], weight)
                    self.assertEqual(info["limit"], limit)


    def dump_qos(self, id=None, ignore=["session_cnt", "session_list", "inflight_requests"]):
        """
        Get dump qos from all active mds, and then compare the result for each voluem_id.
        if dump qos results for each volume_id are different, assert error raises.
        """
        if id:
            return self.fs.mds_asok(["dump", "qos"], mds_id=id)

        result ={}

        for id_ in self.fs.mds_ids:
            out = self.fs.mds_asok(["dump", "qos"], mds_id=id_)  # out == list of volume qos info

            # first element of dump qos is about QoS' status and default values
            # second one is about volume info.
            _, out = out[0], out[1]

            for volume_qos in out:
                self.assertIn("volume_id", volume_qos)

                if volume_qos["volume_id"] in result:  # all volume_id info from mdsss should be same.
                    self.assertTrue(self.is_equal_dict(volume_qos, result[volume_qos["volume_id"]], ignore_key=ignore))
                result[volume_qos["volume_id"]] = volume_qos

        return list(result.values())  # list of each volume_id info, [{"voluem_id": "/subvol", "reservation": 100,}, ...]

    def test_enable_qos(self):
        """
        Enable QoS for all active mdss.
        """
        self.enable_qos()

        stat_qos = self.dump_qos()
        log.info(stat_qos)

        self.assertTrue(self.fs.is_mds_qos())

    def test_disable_qos(self):
        """
        Disable QoS for all active mdss.
        """
        self.disable_qos()

        stat_qos = self.dump_qos()
        log.info(stat_qos)

        self.assertFalse(self.fs.is_mds_qos())

    def test_set_default_qos_value(self):
        """
        Enable QoS and set default qos value, and then check the result.
        """
        log.info(self.fs.is_mds_qos())
        self.enable_qos()

        reservation, weight, limit = 200, 200, 200

        self.mds_asok_all(["config", "set", "mds_dmclock_reservation", str(reservation)])
        self.mds_asok_all(["config", "set", "mds_dmclock_limit", str(limit)])
        self.mds_asok_all(["config", "set", "mds_dmclock_weight", str(weight)])

        for id in self.fs.mds_ids:
            stat_qos = self.dump_qos(id)
            log.info(stat_qos[0])

            self.assertIn("qos_enabled", stat_qos[0])
            if stat_qos[0]["qos_enabled"]:
                self.assertEqual(stat_qos[0]["default_reservation"], reservation)
                self.assertEqual(stat_qos[0]["default_limit"], limit)
                self.assertEqual(stat_qos[0]["default_weight"], weight)

    def test_update_qos_value_root_volume(self):
        """
        Negative Testcase
        Try to update qos 3 values to "/" using setfattr.
        """
        log.info(self.fs.is_mds_qos())
        self.enable_qos()

        reservation, weight, limit = 100, 100, 100

        self.mount_a.umount_wait()
        self.mount_a.mount(cephfs_mntpt="/")

        with self.assertRaises(CommandFailedError):
            self.mount_a.setfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_reservation", str(reservation))
        with self.assertRaises(CommandFailedError):
            self.mount_a.setfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_limit", str(limit))
        with self.assertRaises(CommandFailedError):
            self.mount_a.setfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_weight", str(weight))

        # check whether values are none
        self.assertIsNone(self.mount_a.getfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_reservation"))
        self.assertIsNone(self.mount_a.getfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_limit"))
        self.assertIsNone(self.mount_a.getfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_weight"))

    def test_update_qos_value_subvolume(self):
        """
        Update qos 3 values using setfattr.
        Check its result via getfattr and dump qos.
        """
        log.info(self.fs.is_mds_qos())
        self.enable_qos()

        self.remount_to_mntpt(self.mount_a, self.get_subvolume_root(self.mount_a))

        reservation, weight, limit = 100, 100, 100
        self.set_qos_xattr(self.mount_a, reservation, weight, limit)

        # check updated vxattr using getfattr
        self.assertEqual(
                int(float(self.mount_a.getfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_reservation"))),
                reservation)
        self.assertEqual(
                int(float(self.mount_a.getfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_weight"))),
                weight)
        self.assertEqual(
                int(float(self.mount_a.getfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_limit"))),
                limit)

        stat_qos = self.dump_qos()
        log.info(stat_qos)

        # check updated dmclock info using dump qos
        for info in stat_qos:
            self.assertIn("volume_id", info)
            if info["volume_id"] == str(self.get_subvolume_root(self.mount_a)):
                self.assertEqual(info["reservation"], reservation)
                self.assertEqual(info["limit"], limit)
                self.assertEqual(info["weight"], weight)

    def test_update_random_qos_value_subvolume(self):
        """
        Update qos 3 values using setfattr.
        Check its result via dump qos.
        """
        log.info(self.fs.is_mds_qos())
        self.enable_qos()

        self.remount_to_mntpt(self.mount_a, self.get_subvolume_root(self.mount_a))
        self.remount_to_mntpt(self.mount_b, self.get_subvolume_root(self.mount_b))

        for it in range(1, 20):
            reservation_a = random.randrange(1, 1000)
            weight_a = random.randrange(1, 1000)
            limit_a = random.randrange(1, 1000)
            self.set_qos_xattr(self.mount_a, reservation_a, weight_a, limit_a)

            reservation_b = random.randrange(1, 1000)
            weight_b = random.randrange(1, 1000)
            limit_b = random.randrange(1, 1000)
            self.set_qos_xattr(self.mount_b, reservation_b, weight_b, limit_b)

            time.sleep(1)

            self.verify_qos_info(self.mount_a, reservation_a, weight_a, limit_a)
            self.verify_qos_info(self.mount_b, reservation_b, weight_b, limit_b)

    def test_verify_qos_value_on_cache_miss(self):
        """
        Update qos 3 values using setfattr.
        Check its result via dump qos on cache misses.
        """
        log.info(self.fs.is_mds_qos())
        self.enable_qos()

        self.remount_to_mntpt(self.mount_a, self.get_subvolume_root(self.mount_a))

        reservation_a = random.randrange(1, 1000)
        weight_a = random.randrange(1, 1000)
        limit_a = random.randrange(1, 1000)
        self.set_qos_xattr(self.mount_a, reservation_a, weight_a, limit_a)

        time.sleep(1)

        self.verify_qos_info(self.mount_a, reservation_a, weight_a, limit_a)

        self.mount_a.umount_wait()
        self.mount_a.mount(cephfs_mntpt="/")

        log.info(self.mount_a.hostfs_mntpt)
        sub_ino = self.mount_a.path_to_ino("volumes/_nogroup/subvolume_0")
        log.info(f"/volumes/_nogroup/subvolume_0 inode {sub_ino}")

        nogroup_ino = self.mount_a.path_to_ino("volumes/_nogroup")
        log.info(f"/volumes/_nogroup inode {nogroup_ino}")

        self.mount_a.umount_wait()

        self.mds_asok_all(["config", "set", "mds_cache_memory_limit", str(1024)])

        results = [0]
        import threading
        thread = threading.Thread(target=workload_create_files, args=(0, self.mount_b, 1000, results))

        log.info("IO Testing...")

        thread.start()
        thread.join()

        self.mount_b.umount_wait()

        while (self.fs.mds_asok(['dump', 'inode', str(sub_ino)], mds_id='a') is not None or
               self.fs.mds_asok(['dump', 'inode', str(sub_ino)], mds_id='b') is not None or
               self.fs.mds_asok(['dump', 'inode', str(nogroup_ino)], mds_id='a') is not None or
               self.fs.mds_asok(['dump', 'inode', str(nogroup_ino)], mds_id='b') is not None):
            self.mds_asok_all(["flush", "journal"])
            self.mds_asok_all(["flush_path", "/volumes/_nogroup"])
            time.sleep(10)

        self.assertIsNone(self.fs.mds_asok(['dump', 'inode', str(sub_ino)], mds_id='a'))
        self.assertIsNone(self.fs.mds_asok(['dump', 'inode', str(sub_ino)], mds_id='b'))

        self.assertIsNone(self.fs.mds_asok(['dump', 'inode', str(nogroup_ino)], mds_id='a'))
        self.assertIsNone(self.fs.mds_asok(['dump', 'inode', str(nogroup_ino)], mds_id='b'))

        self.mount_a.mount(cephfs_mntpt=str(self.get_subvolume_root(self.mount_a)))

        self.verify_qos_info(self.mount_a, reservation_a, weight_a, limit_a)

        self.mount_a.umount_wait()

        self.fs.mds_restart()
        self.fs.wait_for_daemons()
        log.info(str(self.mds_cluster.status()))

        self.mount_a.mount(cephfs_mntpt=str(self.get_subvolume_root(self.mount_a)))
        self.verify_qos_info(self.mount_a, reservation_a, weight_a, limit_a)

    def test_update_qos_value_dir(self):
        """
        Update qos 3 values using setfattr.
        Check its result via getfattr and dump qos.
        """
        from os import mkdir, path

        log.info(self.fs.is_mds_qos())
        self.enable_qos()

        test_dir = path.join(self.mount_a.hostfs_mntpt, "test_dir")

        mkdir(test_dir)

        reservation, weight, limit = 100, 100, 100

        with self.assertRaises(CommandFailedError):
            self.mount_a.setfattr(test_dir, "ceph.dmclock.mds_reservation", str(reservation))
        with self.assertRaises(CommandFailedError):
            self.mount_a.setfattr(test_dir, "ceph.dmclock.mds_limit", str(limit))
        with self.assertRaises(CommandFailedError):
            self.mount_a.setfattr(test_dir, "ceph.dmclock.mds_weight", str(weight))

        # check updated vxattr using getfattr
        self.assertIsNone(self.mount_a.getfattr(test_dir, "ceph.dmclock.mds_reservation"))
        self.assertIsNone(self.mount_a.getfattr(test_dir, "ceph.dmclock.mds_limit"))
        self.assertIsNone(self.mount_a.getfattr(test_dir, "ceph.dmclock.mds_weight"))

    def test_mount_subdir_update_qos_value(self):
        """
        Test setfattr to subdir in subvolume.
        """
        import threading

        self.enable_qos()
        reservation, weight, limit = 50, 50, 50
        self.mds_asok_all(["config", "set", "mds_dmclock_reservation", str(reservation)])
        self.mds_asok_all(["config", "set", "mds_dmclock_limit", str(limit)])
        self.mds_asok_all(["config", "set", "mds_dmclock_weight", str(weight)])

        reservation, weight, limit = 100, 100, 100

        self.remount_to_mntpt(self.mount_a, self.get_subvolume_root(self.mount_a))
        self.set_qos_xattr(self.mount_a, reservation, weight, limit)

        # check updated vxattr using getfattr
        self.assertEqual(
                int(float(self.mount_a.getfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_reservation"))),
                reservation)
        self.assertEqual(
                int(float(self.mount_a.getfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_weight"))),
                weight)
        self.assertEqual(
                int(float(self.mount_a.getfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_limit"))),
                limit)

        self.remount_subvolume_path_root(self.mount_a)

        stat_qos = self.dump_qos()
        log.info(stat_qos)

        results = [0]

        thread = threading.Thread(target=workload_create_files, args=(0, self.mount_a, self.TEST_COUNT, results))

        log.info("IO Testing...")

        thread.start()
        thread.join()

        stat_qos = self.dump_qos()
        log.info(stat_qos)
        log.info(results)

    def test_remote_update_qos_value(self):
        """
        Update qos 3 values using setfattr.
        And check it from another mount path.
        """
        log.info(self.fs.is_mds_qos())
        self.enable_qos()

        self.remount_to_mntpt(self.mount_a, self.get_subvolume_root(self.mount_a))
        self.remount_to_mntpt(self.mount_b, self.get_subvolume_root(self.mount_a))

        reservation, weight, limit = 50, 50, 50

        self.set_qos_xattr(self.mount_a, reservation * 2, weight * 2, limit * 2)
        self.set_qos_xattr(self.mount_b, reservation * 2, weight * 2, limit * 2)
        self.set_qos_xattr(self.mount_a, reservation, weight, limit)

        self.assertEqual(
                int(float(self.mount_a.getfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_reservation"))),
                reservation)
        self.assertEqual(
                int(float(self.mount_a.getfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_weight"))),
                weight)
        self.assertEqual(
                int(float(self.mount_a.getfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_limit"))),
                limit)
        time.sleep(10)

        # check remote vxattr
        self.assertEqual(
                int(float(self.mount_b.getfattr(self.mount_b.hostfs_mntpt, "ceph.dmclock.mds_reservation"))),
                reservation)
        self.assertEqual(
                int(float(self.mount_b.getfattr(self.mount_b.hostfs_mntpt, "ceph.dmclock.mds_weight"))),
                weight)
        self.assertEqual(
                int(float(self.mount_b.getfattr(self.mount_b.hostfs_mntpt, "ceph.dmclock.mds_limit"))),
                limit)

    def test_pinning_each_rank_with_different_default_values(self):
        """
        Test a situation with different QoS default values in between MDS.
        """
        import threading

        if len(self.fs.mds_ids) < 2:
            self.skipTest("To test multi MDSS, The number of MDS should be bigger than 2")

        self.enable_qos()

        reservation, weight, limit = 25, 50, 50

        for rank in self.fs.get_ranks():
            if rank['name'] == 'a':
                self.mount_a.setfattr(self.mount_a.hostfs_mntpt, "ceph.dir.pin", str(rank['rank']))
            elif rank['name'] == 'b':
                self.mount_b.setfattr(self.mount_b.hostfs_mntpt, "ceph.dir.pin", str(rank['rank']))

        self.fs.mds_asok(["config", "set", "mds_dmclock_reservation", str(reservation)], mds_id='a')
        self.fs.mds_asok(["config", "set", "mds_dmclock_limit", str(limit)], mds_id='a')
        self.fs.mds_asok(["config", "set", "mds_dmclock_weight", str(weight)], mds_id='a')

        self.fs.mds_asok(["config", "set", "mds_dmclock_reservation", str(reservation * 2)], mds_id='b')
        self.fs.mds_asok(["config", "set", "mds_dmclock_limit", str(limit * 2)], mds_id='b')
        self.fs.mds_asok(["config", "set", "mds_dmclock_weight", str(weight * 2)], mds_id='b')

        threads = []
        results = [0, 0]

        threads.append(threading.Thread(target=workload_create_files, args=(0, self.mount_a, self.TEST_COUNT, results)))
        threads.append(threading.Thread(target=workload_create_files, args=(1, self.mount_b, self.TEST_COUNT, results)))

        log.info("IO Testing...")

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        log.info(results)

        #  self.assertLess(results[0], results[1])
        log.info("IO Test Result: should be {0} < {1}".format(results[0], results[1]))

    def test_mixed_mds(self):
        """
        Test a situation which some mdss are with enabled QoS, others are with disabled QoS.
        """
        import threading

        if len(self.fs.mds_ids) < 2:
            self.skipTest("To test multi MDSS, The number of MDS should be bigger than 2")

        self.disable_qos()

        for rank in self.fs.get_ranks():
            if rank['rank'] == 0:
                self.enable_qos(rank['name'])

        self.mount_a.setfattr(self.mount_a.hostfs_mntpt, "ceph.dir.pin", str(0))  # pinning to QoS enabled MDS
        self.mount_b.setfattr(self.mount_b.hostfs_mntpt, "ceph.dir.pin", str(1))  # pinning to QoS disabled MDS

        reservation, weight, limit = 25, 50, 50
        self.remount_to_mntpt(self.mount_a, self.get_subvolume_root(self.mount_a))
        self.set_qos_xattr(self.mount_a, reservation, weight, limit)
        self.remount_subvolume_path_root(self.mount_a)

        threads = []
        results = [0, 0]
        threads.append(threading.Thread(target=workload_create_files, args=(0, self.mount_a, self.TEST_COUNT, results)))
        threads.append(threading.Thread(target=workload_create_files, args=(1, self.mount_b, self.TEST_COUNT, results)))

        log.info("IO Testing...")

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        log.info(results)

        #  self.assertLess(results[0], results[1])
        log.info("IO Test Result: should be {0} < {1}".format(results[0], results[1]))

    def test_qos_throttling_50IOPS(self):
        import threading

        log.info(self.fs.is_mds_qos())
        self.enable_qos()
        log.info(self.fs.is_mds_qos())

        reservation, weight, limit = 25, 50, 50

        self.mount_a.setfattr(self.mount_a.hostfs_mntpt, "ceph.dir.pin", str(0))

        self.remount_to_mntpt(self.mount_a, self.get_subvolume_root(self.mount_a))
        self.set_qos_xattr(self.mount_a, reservation, weight, limit)
        self.remount_subvolume_path_root(self.mount_a)

        stat_qos = self.dump_qos()
        log.info(stat_qos)

        results = [0]

        thread = threading.Thread(target=workload_create_files, args=(0, self.mount_a, self.TEST_COUNT, results))

        log.info("IO Testing...")

        thread.start()
        thread.join()

        stat_qos = self.dump_qos()
        log.info(stat_qos)

        #  self.assertBetween(results[0], reservation, limit)
        log.info("IO Test Result: should be {1} <= {0} <= {2}".format(results[0], reservation, limit))

    def test_qos_throttling_100IOPS(self):
        import threading

        log.info(self.fs.is_mds_qos())
        self.enable_qos()
        log.info(self.fs.is_mds_qos())

        reservation, weight, limit = 50, 100, 100

        self.mount_a.setfattr(self.mount_a.hostfs_mntpt, "ceph.dir.pin", str(0))

        self.remount_to_mntpt(self.mount_a, self.get_subvolume_root(self.mount_a))
        self.set_qos_xattr(self.mount_a, reservation, weight, limit)
        self.remount_subvolume_path_root(self.mount_a)

        stat_qos = self.dump_qos()
        log.info(stat_qos)

        results = [0]

        thread = threading.Thread(target=workload_create_files, args=(0, self.mount_a, self.TEST_COUNT, results))

        log.info("IO Testing...")

        thread.start()
        thread.join()

        stat_qos = self.dump_qos()
        log.info(stat_qos)

        #  self.assertBetween(results[0], reservation, limit)
        log.info("IO Test Result: should be {1} <= {0} <= {2}".format(results[0], reservation, limit))

    def test_qos_ratio(self):
        """
        Test QoS throttling.
        For each subvolume, test QoS using thread.
        """
        import threading

        log.info(self.fs.is_mds_qos())
        self.enable_qos()

        reservation, weight, limit = 100, 100, 100
        self.remount_to_mntpt(self.mount_a, self.get_subvolume_root(self.mount_a))
        self.set_qos_xattr(self.mount_a, reservation, weight, limit)
        self.remount_subvolume_path_root(self.mount_a)

        self.remount_to_mntpt(self.mount_b, self.get_subvolume_root(self.mount_b))
        self.set_qos_xattr(self.mount_b, reservation*2, weight*2, limit*2)
        self.remount_subvolume_path_root(self.mount_b)

        stat_qos = self.dump_qos()
        log.info(stat_qos)

        log.info("[Thread 0]: reservation: {0}, weight: {1}, limit {2}".format(reservation, weight, limit))
        log.info("[Thread 1]: reservation: {0}, weight: {1}, limit {2}".format(reservation * 2, weight * 2, limit * 2))

        threads = []
        results = [0, 0]
        threads.append(threading.Thread(target=workload_create_files, args=(0, self.mount_a, self.TEST_COUNT, results)))
        threads.append(threading.Thread(target=workload_create_files, args=(1, self.mount_b, self.TEST_COUNT, results)))

        log.info("IO Testing...")

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        stat_qos = self.dump_qos()
        log.info(stat_qos)

        #  self.assertBetween(results[0] * 2, results[1], results[1])
        log.info("IO Test Result: should be {0} * 2 == {1}".format(results[0], results[1]))

def workload_create_files(tid, mount, test_cnt, results):
    from os import path, mkdir
    import time

    base = path.join(mount.hostfs_mntpt, "thread_{0}".format(tid))
    mkdir(base)

    start = time.time()

    for i in range(test_cnt):
        Path(path.join(base, "file_{0}".format(i))).touch()

    elapsed_time = time.time() - start

    results[tid] = test_cnt / elapsed_time

    log.info("[Thread {0}]: {1} IOs in {2}secs, OPs/sec: {3}".format(
                tid, test_cnt, elapsed_time, (test_cnt / elapsed_time)))

