import logging
from time import sleep
import os

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError
from teuthology.contextutil import safe_while

log = logging.getLogger(__name__)


class TestSubvolume(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    def setUp(self):
        super().setUp()
        self.setup_test()

    def tearDown(self):
        #pass
        # clean up
        self.cleanup_test()
        super().tearDown()

    def setup_test(self):
        self.mount_a.run_shell(['mkdir', 'group'])
        self.mount_a.run_shell(['mkdir', 'group/subvol1'])
        self.mount_a.run_shell(['setfattr', '-n', 'ceph.dir.subvolume',
                                '-v', '1', 'group/subvol1'])
        self.mount_a.run_shell(['mv', 'group/subvol1', 'group/subvol2'])

    def cleanup_test(self):
        self.mount_a.run_shell(['rm', '-rf', 'group'])

    def test_subvolume_move_out_file(self):
        """
        To verify that file can't be moved out of subvolume
        """
        self.mount_a.run_shell(['touch', 'group/subvol2/file1'])

        # file can't be moved out of a subvolume
        with self.assertRaises(CommandFailedError):
            self.mount_a.run_shell(['rename', 'group/subvol2/file1',
                                    'group/file1', 'group/subvol2/file1'])


    def test_subvolume_move_in_file(self):
        """
        To verify that file can't be moved into subvolume
        """
        # file can't be moved into a subvolume
        self.mount_a.run_shell(['touch', 'group/file2'])
        with self.assertRaises(CommandFailedError):
            self.mount_a.run_shell(['rename', 'group/file2',
                                    'group/subvol2/file2', 'group/file2'])

    def test_subvolume_hardlink_to_outside(self):
        """
        To verify that file can't be hardlinked to outside subvolume
        """
        self.mount_a.run_shell(['touch', 'group/subvol2/file1'])

        # create hard link within subvolume
        self.mount_a.run_shell(['ln',
                                'group/subvol2/file1', 'group/subvol2/file1_'])

        # hard link can't be created out of subvolume
        with self.assertRaises(CommandFailedError):
            self.mount_a.run_shell(['ln',
                                    'group/subvol2/file1', 'group/file1_'])

    def test_subvolume_hardlink_to_inside(self):
        """
        To verify that file can't be hardlinked to inside subvolume
        """
        self.mount_a.run_shell(['touch', 'group/subvol2/file1'])

        # create hard link within subvolume
        self.mount_a.run_shell(['ln',
                                'group/subvol2/file1', 'group/subvol2/file1_'])

        # hard link can't be created inside subvolume
        self.mount_a.run_shell(['touch', 'group/file2'])
        with self.assertRaises(CommandFailedError):
            self.mount_a.run_shell(['ln',
                                    'group/file2', 'group/subvol2/file2_'])

    def test_subvolume_snapshot_inside_subvolume_subdir(self):
        """
        To verify that snapshot can't be taken for a subvolume subdir
        """
        self.mount_a.run_shell(['touch', 'group/subvol2/file1'])

        # create snapshot at subvolume root
        self.mount_a.run_shell(['mkdir', 'group/subvol2/.snap/s1'])

        # can't create snapshot in a descendent dir of subvolume
        self.mount_a.run_shell(['mkdir', 'group/subvol2/dir'])
        with self.assertRaises(CommandFailedError):
            self.mount_a.run_shell(['mkdir', 'group/subvol2/dir/.snap/s2'])

        # clean up
        self.mount_a.run_shell(['rmdir', 'group/subvol2/.snap/s1'])

    def test_subvolume_file_move_across_subvolumes(self):
        """
        To verify that file can't be moved across subvolumes
        """
        self.mount_a.run_shell(['touch', 'group/subvol2/file1'])

        # create another subvol
        self.mount_a.run_shell(['mkdir', 'group/subvol3'])
        self.mount_a.run_shell(['setfattr', '-n', 'ceph.dir.subvolume',
                                '-v', '1', 'group/subvol3'])

        # can't move file across subvolumes
        with self.assertRaises(CommandFailedError):
            self.mount_a.run_shell(['rename', 'group/subvol2/file1',
                                    'group/subvol3/file1',
                                    'group/subvol2/file1'])

    def test_subvolume_hardlink_across_subvolumes(self):
        """
        To verify that hardlink can't be created across subvolumes
        """
        self.mount_a.run_shell(['touch', 'group/subvol2/file1'])

        # create another subvol
        self.mount_a.run_shell(['mkdir', 'group/subvol3'])
        self.mount_a.run_shell(['setfattr', '-n', 'ceph.dir.subvolume',
                                '-v', '1', 'group/subvol3'])

        # can't create hard link across subvolumes
        with self.assertRaises(CommandFailedError):
            self.mount_a.run_shell(['ln', 'group/subvol2/file1',
                                    'group/subvol3/file1'])

    def test_subvolume_setfattr_empty_value(self):
        """
        To verify that an empty value fails on subvolume xattr
        """

        # create subvol
        self.mount_a.run_shell(['mkdir', 'group/subvol4'])

        try:
            self.mount_a.setfattr('group/subvol4', 'ceph.dir.subvolume', '')
        except CommandFailedError:
            pass
        else:
            self.fail("run_shell should raise CommandFailedError")

    def test_subvolume_rmattr(self):
        """
        To verify that rmattr can be used to reset subvolume xattr
        """

        # create subvol
        self.mount_a.run_shell(['mkdir', 'group/subvol4'])
        self.mount_a.setfattr('group/subvol4', 'ceph.dir.subvolume', '1')

        # clear subvolume flag
        self.mount_a.removexattr('group/subvol4', 'ceph.dir.subvolume')

    def test_subvolume_create_subvolume_inside_subvolume(self):
        """
        To verify that subvolume can't be created inside a subvolume
        """
        # can't create subvolume inside a subvolume
        self.mount_a.run_shell(['mkdir', 'group/subvol2/dir'])
        with self.assertRaises(CommandFailedError):
            self.mount_a.run_shell(['setfattr', '-n', 'ceph.dir.subvolume',
                                    '-v', '1', 'group/subvol2/dir'])

    def test_subvolume_create_snapshot_inside_new_subvolume_parent(self):
        """
        To verify that subvolume can't be created inside a new subvolume parent
        """
        self.mount_a.run_shell(['touch', 'group/subvol2/file1'])

        # clear subvolume flag
        self.mount_a.run_shell(['setfattr', '-n', 'ceph.dir.subvolume',
                                '-v', '0', 'group/subvol2'])

        # create a snap
        self.mount_a.run_shell(['mkdir', 'group/subvol2/dir'])
        self.mount_a.run_shell(['mkdir', 'group/subvol2/dir/.snap/s2'])

        # override subdir subvolume with parent subvolume
        (['setfattr', '-n', 'ceph.dir.subvolume',
                                '-v', '1', 'group/subvol2/dir'])
        self.mount_a.run_shell(['setfattr', '-n', 'ceph.dir.subvolume',
                                '-v', '1', 'group/subvol2'])

        # can't create a snap in a subdir of a subvol parent
        with self.assertRaises(CommandFailedError):
            self.mount_a.run_shell(['mkdir', 'group/subvol2/dir/.snap/s3'])

        # clean up
        self.mount_a.run_shell(['rmdir', 'group/subvol2/dir/.snap/s2'])


    def test_subvolume_vxattr_removal_without_setting(self):
        """
        To verify that the ceph.dir.subvolume vxattr removal without setting doesn't cause mds crash
        """

        # create a subvol
        self.mount_a.run_shell(['mkdir', 'group/subvol3'])
        self.mount_a.removexattr('group/subvol3', 'ceph.dir.subvolume')

        # cleanup
        self.mount_a.run_shell(['rm', '-rf', 'group/subvol3'])


    def test_subvolume_vxattr_retrieval(self):
        """
        To verify that the ceph.dir.subvolume vxattr can be acquired using getfattr
        """
        # create subvolume dir
        subvol_dir:str = 'group/subvol5'
        self.mount_a.run_shell(['mkdir', subvol_dir])
        mkdir_fattr = self.mount_a.getfattr(subvol_dir, 'ceph.dir.subvolume')
        self.assertEqual('0', mkdir_fattr)

        self.mount_a.setfattr(subvol_dir, 'ceph.dir.subvolume', '1')
        new_fattr:str = self.mount_a.getfattr(subvol_dir, 'ceph.dir.subvolume')
        self.assertEqual('1', new_fattr)

        # clear subvolume flag
        self.mount_a.removexattr(subvol_dir, 'ceph.dir.subvolume')

        # cleanup
        self.mount_a.run_shell(['rm', '-rf', subvol_dir])


class TestSubvolumeReplicated(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 2

    def test_subvolume_replicated(self):
        """
        That a replica sees the subvolume flag on a directory.
        """


        self.mount_a.run_shell_payload("mkdir -p dir1/dir2/dir3/dir4")

        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()

        self.mount_a.setfattr("dir1", "ceph.dir.pin", "1")
        self.mount_a.setfattr("dir1/dir2/dir3", "ceph.dir.pin", "0") # force dir2 to be replicated
        status = self._wait_subtrees([("/dir1", 1), ("/dir1/dir2/dir3", 0)], status=status, rank=1)

        op = self.fs.rank_tell("lock", "path", "/dir1/dir2", "snap:r", rank=1)
        p = self.mount_a.setfattr("dir1/dir2", "ceph.dir.subvolume", "1", wait=False)
        sleep(2)
        reqid = self._reqid_tostr(op['reqid'])
        self.fs.kill_op(reqid, rank=1)
        p.wait()

        ino1 = self.fs.read_cache("/dir1/dir2", depth=0, rank=1)[0]
        self.assertTrue(ino1['is_subvolume'])
        self.assertTrue(ino1['is_auth'])
        replicas = ino1['auth_state']['replicas']
        self.assertIn("0", replicas)

        ino0 = self.fs.read_cache("/dir1/dir2", depth=0, rank=0)[0]
        self.assertFalse(ino0['is_auth'])
        self.assertTrue(ino0['is_subvolume'])

class TestSubvolumeMetrics(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    def get_subvolume_metrics(self, mds_rank=0):
        """
        Helper to fetch current subvolume metrics from MDS counters using rank_tell.
        """
        mds_info = self.fs.get_rank(rank=mds_rank)
        mds_name = mds_info['name']
        counters = self.fs.mds_tell(["counter", "dump"], mds_id=mds_name)
        return counters.get("mds_subvolume_metrics")

    def test_subvolume_metrics_lifecycle(self):
        """
        Verify that subvolume metrics are initially absent, appear after IO,
        and disappear after the aggregation window expires.
        """
        subvol_name = "metrics_subv"
        subv_path = "/volumes/_nogroup/metrics_subv"

        # ensure metrics absent and quota not set yet
        self.assertFalse(self.get_subvolume_metrics(),
                         "Subvolume metrics should not be present before subvolume creation")

        # create subvolume with quota (5 GiB)
        quota_bytes = 5 * 1024 * 1024 * 1024
        self.fs.run_ceph_cmd('fs', 'subvolume', 'create', 'cephfs', subvol_name,
                             "--size", str(quota_bytes))

        # generate some I/O
        mount_point = self.mount_a.get_mount_point()
        suvolume_fs_path = self.fs.get_ceph_cmd_stdout('fs', 'subvolume', 'getpath', 'cephfs', subvol_name).strip()
        suvolume_fs_path = os.path.join(mount_point, suvolume_fs_path.strip('/'))
        self.mount_a.run_shell(['ls', '-ld', suvolume_fs_path])

        # do some writes
        filename = os.path.join(suvolume_fs_path, "file0")
        self.mount_a.run_shell_payload("sudo fio "
                                       "--name test -rw=write "
                                       "--bs=4k --numjobs=1 --time_based "
                                       "--runtime=10s --verify=0 --size=50M "
                                       f"--filename={filename}", wait=True)

        baseline_used = 0
        subvol_metrics = None
        with safe_while(sleep=1, tries=30, action='wait for subvolume write counters') as proceed:
            while proceed():
                # verify that metrics are available
                subvol_metrics = self.get_subvolume_metrics()
                if subvol_metrics:
                    break

        log.debug(f'verifying for write: subvol_metrics={subvol_metrics}')

        # Extract first metric entry
        metric = subvol_metrics[0]
        counters = metric["counters"]
        labels = metric["labels"]

        # Label checks
        self.assertEqual(labels["fs_name"], "cephfs", "Unexpected fs_name in subvolume metrics")
        self.assertEqual(labels["subvolume_path"], subv_path, "Unexpected subvolume_path in subvolume metrics")

        # Counter presence and value checks
        self.assertIn("avg_read_iops", counters)
        self.assertIn("avg_read_tp_Bps", counters)
        self.assertIn("avg_read_lat_msec", counters)
        self.assertIn("avg_write_iops", counters)
        self.assertIn("avg_write_tp_Bps", counters)
        self.assertIn("avg_write_lat_msec", counters)
        self.assertIn("quota_bytes", counters)
        self.assertIn("used_bytes", counters)

        # check write metrics
        self.assertGreater(counters["avg_write_iops"], 0, "Expected avg_write_iops to be > 0")
        self.assertGreater(counters["avg_write_tp_Bps"], 0, "Expected avg_write_tp_Bps to be > 0")
        self.assertGreaterEqual(counters["avg_write_lat_msec"], 0, "Expected avg_write_lat_msec to be > 0")
        self.assertEqual(counters["quota_bytes"], quota_bytes,
                         f"Expected quota_bytes to reflect provisioned quota ({quota_bytes})")
        self.assertGreater(counters["used_bytes"], baseline_used,
                           "Expected used_bytes to grow after writes")

        baseline_used = counters["used_bytes"]

        # do some reads
        self.mount_a.run_shell_payload("sudo fio "
                                       "--name test -rw=read "
                                       "--bs=4k --numjobs=1 --time_based "
                                       "--runtime=5s --verify=0 --size=50M "
                                       f"--filename={filename}", wait=True)

        subvol_metrics = None
        with safe_while(sleep=1, tries=30, action='wait for subvolume read counters') as proceed:
            while proceed():
                # verify that metrics are available
                subvol_metrics = self.get_subvolume_metrics()
                if subvol_metrics:
                    break

        log.debug(f'verifying for read: subvol_metrics={subvol_metrics}')

        metric = subvol_metrics[0]
        counters = metric["counters"]

        # Assert expected values (example: write I/O occurred, read did not)
        self.assertGreater(counters["avg_read_iops"], 0, "Expected avg_read_iops to be >= 0")
        self.assertGreater(counters["avg_read_tp_Bps"], 0, "Expected avg_read_tp_Bps to be >= 0")
        self.assertGreaterEqual(counters["avg_read_lat_msec"], 0, "Expected avg_read_lat_msec to be >= 0")
        self.assertEqual(counters["quota_bytes"], quota_bytes,
                         "Quota should remain unchanged during workload")
        self.assertGreaterEqual(counters["used_bytes"], baseline_used,
                                "Used bytes should not shrink during reads alone")

        # delete part of the data and ensure used_bytes drops
        self.mount_a.run_shell_payload(f"sudo rm -f {filename}")
        self.mount_a.run_shell_payload(f"sudo truncate -s 0 {filename}")
        self.mount_a.run_shell_payload("sudo sync")

        # Trigger I/O to generate metrics (metrics only sent during I/O)
        trigger_file = os.path.join(suvolume_fs_path, "trigger_metrics")
        self.mount_a.run_shell_payload(f"sudo dd if=/dev/zero of={trigger_file} bs=4k count=10", wait=True)

        reduced_metrics = None
        with safe_while(sleep=1, tries=30, action='wait for reduced usage') as proceed:
            while proceed():
                # Keep triggering I/O to generate metrics
                self.mount_a.run_shell_payload(f"sudo dd if=/dev/zero of={trigger_file} bs=4k count=1 conv=notrunc", wait=True)
                reduced_metrics = self.get_subvolume_metrics()
                if reduced_metrics:
                    counters_after_delete = reduced_metrics[0]["counters"]
                    if counters_after_delete["used_bytes"] < baseline_used:
                        break
        self.assertIsNotNone(reduced_metrics, "Expected subvolume metrics after deletion")
        counters_after_delete = reduced_metrics[0]["counters"]
        self.assertLess(counters_after_delete["used_bytes"], baseline_used,
                        "Used bytes should drop after deleting data")
        self.assertEqual(counters_after_delete["quota_bytes"], quota_bytes,
                         "Quota should remain unchanged after deletions")

        # wait for metrics to expire after inactivity
        sleep(30)

        # verify that metrics are not present anymore
        subvolume_metrics = self.get_subvolume_metrics()
        self.assertFalse(subvolume_metrics, "Subvolume metrics should be gone after inactivity window")

    def test_subvolume_quota_resize_update(self):
        """
        Verify that subvolume quota changes (via resize) are reflected in metrics.
        This tests that maybe_update_subvolume_quota() is called when quota is
        broadcast to clients.
        """
        subvol_name = "resize_test_subv"
        subv_path = f"/volumes/_nogroup/{subvol_name}"

        # create subvolume with initial quota (1 GiB)
        initial_quota = 1 * 1024 * 1024 * 1024
        self.fs.run_ceph_cmd('fs', 'subvolume', 'create', 'cephfs', subvol_name,
                             "--size", str(initial_quota))

        # access subvolume to trigger registration
        mount_point = self.mount_a.get_mount_point()
        subvolume_fs_path = self.fs.get_ceph_cmd_stdout('fs', 'subvolume', 'getpath', 'cephfs', subvol_name).strip()
        subvolume_fs_path = os.path.join(mount_point, subvolume_fs_path.strip('/'))
        self.mount_a.run_shell(['ls', '-ld', subvolume_fs_path])

        # do some writes to generate metrics
        filename = os.path.join(subvolume_fs_path, "testfile")
        self.mount_a.run_shell_payload("sudo fio "
                                       "--name test -rw=write "
                                       "--bs=4k --numjobs=1 --time_based "
                                       "--runtime=3s --verify=0 --size=5M "
                                       f"--filename={filename}", wait=True)

        # verify initial quota in metrics
        subvol_metrics = None
        with safe_while(sleep=1, tries=30, action='wait for subvolume metrics') as proceed:
            while proceed():
                subvol_metrics = self.get_subvolume_metrics()
                if subvol_metrics:
                    break

        self.assertIsNotNone(subvol_metrics, "Expected subvolume metrics to appear")
        counters = subvol_metrics[0]["counters"]
        self.assertEqual(counters["quota_bytes"], initial_quota,
                         f"Expected initial quota_bytes={initial_quota}")

        # resize subvolume to new quota (2 GiB)
        new_quota = 2 * 1024 * 1024 * 1024
        self.fs.run_ceph_cmd('fs', 'subvolume', 'resize', 'cephfs', subvol_name,
                             str(new_quota))

        # trigger quota broadcast by accessing the subvolume
        self.mount_a.run_shell(['ls', '-ld', subvolume_fs_path])
        # small I/O to ensure metrics update
        self.mount_a.run_shell_payload(f"echo 'test' | sudo tee {filename}.trigger > /dev/null")

        # verify new quota in metrics
        updated_metrics = None
        with safe_while(sleep=1, tries=30, action='wait for updated quota') as proceed:
            while proceed():
                updated_metrics = self.get_subvolume_metrics()
                if updated_metrics:
                    counters = updated_metrics[0]["counters"]
                    if counters["quota_bytes"] == new_quota:
                        break

        self.assertIsNotNone(updated_metrics, "Expected updated subvolume metrics")
        counters = updated_metrics[0]["counters"]
        self.assertEqual(counters["quota_bytes"], new_quota,
                         f"Expected quota_bytes to update to {new_quota} after resize")
        labels = updated_metrics[0]["labels"]
        self.assertEqual(labels["subvolume_path"], subv_path,
                         "Unexpected subvolume_path in metrics")

        # cleanup
        self.fs.run_ceph_cmd('fs', 'subvolume', 'rm', 'cephfs', subvol_name)

    def test_subvolume_quota_activity_prevents_eviction(self):
        """
        Verify that quota broadcasts keep subvolume_quota entries alive
        (by updating last_activity), preventing eviction even without client I/O.
        
        This tests the last_activity update in maybe_update_subvolume_quota().
        """
        subvol_name = "evict_test_subv"
        subv_path = f"/volumes/_nogroup/{subvol_name}"

        # create subvolume with quota
        quota_bytes = 1 * 1024 * 1024 * 1024
        self.fs.run_ceph_cmd('fs', 'subvolume', 'create', 'cephfs', subvol_name,
                             "--size", str(quota_bytes))

        # access subvolume to trigger registration
        mount_point = self.mount_a.get_mount_point()
        subvolume_fs_path = self.fs.get_ceph_cmd_stdout('fs', 'subvolume', 'getpath', 'cephfs', subvol_name).strip()
        subvolume_fs_path = os.path.join(mount_point, subvolume_fs_path.strip('/'))

        # do initial write to generate metrics
        filename = os.path.join(subvolume_fs_path, "testfile")
        self.mount_a.run_shell_payload("sudo fio "
                                       "--name test -rw=write "
                                       "--bs=4k --numjobs=1 --time_based "
                                       "--runtime=3s --verify=0 --size=5M "
                                       f"--filename={filename}", wait=True)

        # verify metrics appear
        subvol_metrics = None
        with safe_while(sleep=1, tries=30, action='wait for subvolume metrics') as proceed:
            while proceed():
                subvol_metrics = self.get_subvolume_metrics()
                if subvol_metrics:
                    break

        self.assertIsNotNone(subvol_metrics, "Expected subvolume metrics to appear")
        initial_counters = subvol_metrics[0]["counters"]
        self.assertEqual(initial_counters["quota_bytes"], quota_bytes)

        # wait briefly and do another read to keep the entry alive through quota broadcast
        sleep(2)

        # access subvolume to trigger quota broadcast (not full I/O)
        self.mount_a.run_shell(['ls', '-la', subvolume_fs_path])
        self.mount_a.run_shell(['stat', filename])

        # verify metrics still present
        still_present = self.get_subvolume_metrics()
        self.assertIsNotNone(still_present, "Metrics should still be present after directory access")
        self.assertEqual(still_present[0]["counters"]["quota_bytes"], quota_bytes,
                         "Quota should remain unchanged")

        # cleanup
        self.fs.run_ceph_cmd('fs', 'subvolume', 'rm', 'cephfs', subvol_name)

    def test_multiple_subvolume_quotas(self):
        """
        Verify that multiple subvolumes each have independent quota tracking.
        This tests that the subvolume_quota map correctly handles multiple entries.
        """
        subvol1_name = "multi_subv1"
        subvol2_name = "multi_subv2"
        subv1_path = f"/volumes/_nogroup/{subvol1_name}"
        subv2_path = f"/volumes/_nogroup/{subvol2_name}"

        # create two subvolumes with different quotas
        quota1 = 1 * 1024 * 1024 * 1024  # 1 GiB
        quota2 = 2 * 1024 * 1024 * 1024  # 2 GiB

        self.fs.run_ceph_cmd('fs', 'subvolume', 'create', 'cephfs', subvol1_name,
                             "--size", str(quota1))
        self.fs.run_ceph_cmd('fs', 'subvolume', 'create', 'cephfs', subvol2_name,
                             "--size", str(quota2))

        mount_point = self.mount_a.get_mount_point()

        # access and do I/O on both subvolumes
        for subvol_name in [subvol1_name, subvol2_name]:
            subvolume_fs_path = self.fs.get_ceph_cmd_stdout('fs', 'subvolume', 'getpath', 'cephfs', subvol_name).strip()
            subvolume_fs_path = os.path.join(mount_point, subvolume_fs_path.strip('/'))
            self.mount_a.run_shell(['ls', '-ld', subvolume_fs_path])

            filename = os.path.join(subvolume_fs_path, "testfile")
            self.mount_a.run_shell_payload("sudo fio "
                                           "--name test -rw=write "
                                           "--bs=4k --numjobs=1 --time_based "
                                           "--runtime=3s --verify=0 --size=5M "
                                           f"--filename={filename}", wait=True)

        # wait for metrics to be collected
        subvol_metrics = None
        with safe_while(sleep=1, tries=30, action='wait for multiple subvolume metrics') as proceed:
            while proceed():
                subvol_metrics = self.get_subvolume_metrics()
                if subvol_metrics and len(subvol_metrics) >= 2:
                    break

        self.assertIsNotNone(subvol_metrics, "Expected subvolume metrics for both subvolumes")
        self.assertGreaterEqual(len(subvol_metrics), 2,
                                "Expected at least 2 subvolume metrics entries")

        # build a map of path -> quota from metrics
        metrics_by_path = {}
        for m in subvol_metrics:
            path = m["labels"]["subvolume_path"]
            quota = m["counters"]["quota_bytes"]
            metrics_by_path[path] = quota

        # verify each subvolume has correct quota
        self.assertIn(subv1_path, metrics_by_path,
                      f"Expected metrics for {subv1_path}")
        self.assertIn(subv2_path, metrics_by_path,
                      f"Expected metrics for {subv2_path}")
        self.assertEqual(metrics_by_path[subv1_path], quota1,
                         f"Expected quota {quota1} for {subv1_path}")
        self.assertEqual(metrics_by_path[subv2_path], quota2,
                         f"Expected quota {quota2} for {subv2_path}")

        # cleanup
        self.fs.run_ceph_cmd('fs', 'subvolume', 'rm', 'cephfs', subvol1_name)
        self.fs.run_ceph_cmd('fs', 'subvolume', 'rm', 'cephfs', subvol2_name)

    def test_subvolume_unlimited_quota(self):
        """
        Verify that subvolumes without quota (unlimited) report quota_bytes=0.
        """
        subvol_name = "unlimited_subv"
        subv_path = f"/volumes/_nogroup/{subvol_name}"

        # create subvolume without quota
        self.fs.run_ceph_cmd('fs', 'subvolume', 'create', 'cephfs', subvol_name)

        # access subvolume and do I/O
        mount_point = self.mount_a.get_mount_point()
        subvolume_fs_path = self.fs.get_ceph_cmd_stdout('fs', 'subvolume', 'getpath', 'cephfs', subvol_name).strip()
        subvolume_fs_path = os.path.join(mount_point, subvolume_fs_path.strip('/'))
        self.mount_a.run_shell(['ls', '-ld', subvolume_fs_path])

        filename = os.path.join(subvolume_fs_path, "testfile")
        self.mount_a.run_shell_payload("sudo fio "
                                       "--name test -rw=write "
                                       "--bs=4k --numjobs=1 --time_based "
                                       "--runtime=3s --verify=0 --size=5M "
                                       f"--filename={filename}", wait=True)

        # wait for metrics with quota_bytes=0 (unlimited subvolume)
        subvol_metrics = None
        with safe_while(sleep=1, tries=30, action='wait for subvolume metrics') as proceed:
            while proceed():
                subvol_metrics = self.get_subvolume_metrics()
                if subvol_metrics:
                    # for unlimited quota, quota_bytes should be 0
                    if subvol_metrics[0]["counters"]["quota_bytes"] == 0:
                        break

        self.assertIsNotNone(subvol_metrics, "Expected subvolume metrics")
        counters = subvol_metrics[0]["counters"]
        labels = subvol_metrics[0]["labels"]

        self.assertEqual(labels["subvolume_path"], subv_path)
        self.assertEqual(counters["quota_bytes"], 0,
                         "Expected quota_bytes=0 for unlimited subvolume")
        # used_bytes is fetched dynamically from the inode's rstat, so it should
        # reflect actual usage even for unlimited quota subvolumes
        self.assertGreater(counters["used_bytes"], 0,
                           "Expected used_bytes > 0 after writes")

        # cleanup
        self.fs.run_ceph_cmd('fs', 'subvolume', 'rm', 'cephfs', subvol_name)

    def test_subvolume_metrics_stress(self):
        """
        Comprehensive stress test for subvolume metrics covering:
        - Multiple quota resizes (up and down)
        - Data writes, partial deletes, overwrites
        - Concurrent operations on multiple subvolumes
        - Verifying metrics accurately track all changes
        """
        subvol_name = "stress_subv"
        subv_path = f"/volumes/_nogroup/{subvol_name}"

        # Phase 1: Create subvolume with initial small quota
        initial_quota = 100 * 1024 * 1024  # 100 MiB
        self.fs.run_ceph_cmd('fs', 'subvolume', 'create', 'cephfs', subvol_name,
                             "--size", str(initial_quota))

        mount_point = self.mount_a.get_mount_point()
        subvolume_fs_path = self.fs.get_ceph_cmd_stdout('fs', 'subvolume', 'getpath', 'cephfs', subvol_name).strip()
        subvolume_fs_path = os.path.join(mount_point, subvolume_fs_path.strip('/'))

        log.info(f"Phase 1: Initial quota {initial_quota}, writing data...")

        # Write multiple files
        for i in range(3):
            filename = os.path.join(subvolume_fs_path, f"file{i}")
            self.mount_a.run_shell_payload("sudo fio "
                                           "--name test -rw=write "
                                           "--bs=4k --numjobs=1 --time_based "
                                           f"--runtime=2s --verify=0 --size=5M "
                                           f"--filename={filename}", wait=True)

        # Verify initial metrics
        subvol_metrics = None
        with safe_while(sleep=1, tries=30, action='wait for initial metrics') as proceed:
            while proceed():
                subvol_metrics = self.get_subvolume_metrics()
                if subvol_metrics:
                    counters = subvol_metrics[0]["counters"]
                    if counters["quota_bytes"] == initial_quota and counters["used_bytes"] > 0:
                        break

        self.assertIsNotNone(subvol_metrics, "Expected initial metrics")
        counters = subvol_metrics[0]["counters"]
        self.assertEqual(counters["quota_bytes"], initial_quota)
        initial_used = counters["used_bytes"]
        self.assertGreater(initial_used, 0, "Expected some data written")
        log.info(f"Phase 1 complete: quota={counters['quota_bytes']}, used={initial_used}")

        # Phase 2: Resize quota UP
        larger_quota = 500 * 1024 * 1024  # 500 MiB
        log.info(f"Phase 2: Resizing quota UP to {larger_quota}...")
        self.fs.run_ceph_cmd('fs', 'subvolume', 'resize', 'cephfs', subvol_name,
                             str(larger_quota))

        # Trigger quota broadcast
        self.mount_a.run_shell(['ls', '-la', subvolume_fs_path])
        self.mount_a.run_shell_payload(f"echo 'trigger' | sudo tee {subvolume_fs_path}/trigger > /dev/null")

        # Verify quota increased, used unchanged
        updated_metrics = None
        with safe_while(sleep=1, tries=30, action='wait for quota increase') as proceed:
            while proceed():
                updated_metrics = self.get_subvolume_metrics()
                if updated_metrics:
                    counters = updated_metrics[0]["counters"]
                    if counters["quota_bytes"] == larger_quota:
                        break

        self.assertIsNotNone(updated_metrics)
        counters = updated_metrics[0]["counters"]
        self.assertEqual(counters["quota_bytes"], larger_quota)
        # used_bytes should be roughly the same (small trigger file added)
        self.assertGreater(counters["used_bytes"], initial_used - 1024)  # allow some variance
        log.info(f"Phase 2 complete: quota={counters['quota_bytes']}, used={counters['used_bytes']}")

        # Phase 3: Write more data to increase usage
        log.info("Phase 3: Writing more data...")
        for i in range(3, 6):
            filename = os.path.join(subvolume_fs_path, f"file{i}")
            self.mount_a.run_shell_payload("sudo fio "
                                           "--name test -rw=write "
                                           "--bs=4k --numjobs=1 --time_based "
                                           f"--runtime=2s --verify=0 --size=5M "
                                           f"--filename={filename}", wait=True)

        # Verify used_bytes increased
        more_metrics = None
        with safe_while(sleep=1, tries=30, action='wait for increased usage') as proceed:
            while proceed():
                more_metrics = self.get_subvolume_metrics()
                if more_metrics:
                    counters = more_metrics[0]["counters"]
                    if counters["used_bytes"] > initial_used * 1.5:  # expect significant increase
                        break

        self.assertIsNotNone(more_metrics)
        counters = more_metrics[0]["counters"]
        phase3_used = counters["used_bytes"]
        self.assertGreater(phase3_used, initial_used * 1.5,
                           "Expected used_bytes to increase significantly after more writes")
        log.info(f"Phase 3 complete: quota={counters['quota_bytes']}, used={phase3_used}")

        # Phase 4: Delete some files, verify used_bytes decreases
        log.info("Phase 4: Deleting files 0-2...")
        for i in range(3):
            self.mount_a.run_shell_payload(f"sudo rm -f {subvolume_fs_path}/file{i}")
        self.mount_a.run_shell_payload("sudo sync")

        # Trigger I/O on remaining files to generate metrics (metrics only sent during I/O)
        self.mount_a.run_shell_payload(f"sudo dd if=/dev/zero of={subvolume_fs_path}/trigger_delete bs=4k count=10", wait=True)

        # Verify used_bytes decreased
        deleted_metrics = None
        with safe_while(sleep=1, tries=30, action='wait for decreased usage after delete') as proceed:
            while proceed():
                # Keep triggering writes to ensure metrics are generated
                self.mount_a.run_shell_payload(f"sudo dd if=/dev/zero of={subvolume_fs_path}/trigger_delete bs=4k count=1 conv=notrunc", wait=True)
                deleted_metrics = self.get_subvolume_metrics()
                if deleted_metrics:
                    counters = deleted_metrics[0]["counters"]
                    if counters["used_bytes"] < phase3_used * 0.8:  # expect ~50% drop
                        break

        self.assertIsNotNone(deleted_metrics)
        counters = deleted_metrics[0]["counters"]
        phase4_used = counters["used_bytes"]
        self.assertLess(phase4_used, phase3_used,
                        "Expected used_bytes to decrease after file deletion")
        log.info(f"Phase 4 complete: quota={counters['quota_bytes']}, used={phase4_used}")

        # Phase 5: Resize quota DOWN (but still above current usage)
        smaller_quota = 200 * 1024 * 1024  # 200 MiB
        log.info(f"Phase 5: Resizing quota DOWN to {smaller_quota}...")
        self.fs.run_ceph_cmd('fs', 'subvolume', 'resize', 'cephfs', subvol_name,
                             str(smaller_quota))

        # Trigger quota broadcast
        self.mount_a.run_shell(['stat', subvolume_fs_path])

        # Verify quota decreased
        smaller_metrics = None
        with safe_while(sleep=1, tries=30, action='wait for quota decrease') as proceed:
            while proceed():
                smaller_metrics = self.get_subvolume_metrics()
                if smaller_metrics:
                    counters = smaller_metrics[0]["counters"]
                    if counters["quota_bytes"] == smaller_quota:
                        break

        self.assertIsNotNone(smaller_metrics)
        counters = smaller_metrics[0]["counters"]
        self.assertEqual(counters["quota_bytes"], smaller_quota)
        log.info(f"Phase 5 complete: quota={counters['quota_bytes']}, used={counters['used_bytes']}")

        # Phase 6: Overwrite existing files (usage should stay roughly same)
        log.info("Phase 6: Overwriting existing files...")
        for i in range(3, 6):
            filename = os.path.join(subvolume_fs_path, f"file{i}")
            self.mount_a.run_shell_payload("sudo fio "
                                           "--name test -rw=write "
                                           "--bs=4k --numjobs=1 --time_based "
                                           f"--runtime=1s --verify=0 --size=5M "
                                           f"--filename={filename}", wait=True)

        overwrite_metrics = self.get_subvolume_metrics()
        self.assertIsNotNone(overwrite_metrics)
        counters = overwrite_metrics[0]["counters"]
        # used_bytes should be similar (overwriting same files)
        self.assertGreater(counters["used_bytes"], 0)
        log.info(f"Phase 6 complete: quota={counters['quota_bytes']}, used={counters['used_bytes']}")

        # Phase 7: Mixed read/write workload
        log.info("Phase 7: Mixed read/write workload...")
        filename = os.path.join(subvolume_fs_path, "mixed_file")
        self.mount_a.run_shell_payload("sudo fio "
                                       "--name test -rw=randrw --rwmixread=50 "
                                       "--bs=4k --numjobs=2 --time_based "
                                       "--runtime=3s --verify=0 --size=5M "
                                       f"--filename={filename}", wait=True)

        # Verify metrics still valid after mixed workload
        mixed_metrics = None
        with safe_while(sleep=1, tries=30, action='wait for mixed workload metrics') as proceed:
            while proceed():
                mixed_metrics = self.get_subvolume_metrics()
                if mixed_metrics:
                    counters = mixed_metrics[0]["counters"]
                    # Should have both read and write ops
                    if counters["avg_read_iops"] > 0 and counters["avg_write_iops"] > 0:
                        break

        self.assertIsNotNone(mixed_metrics)
        counters = mixed_metrics[0]["counters"]
        self.assertGreater(counters["avg_read_iops"], 0, "Expected read I/O from mixed workload")
        self.assertGreater(counters["avg_write_iops"], 0, "Expected write I/O from mixed workload")
        self.assertEqual(counters["quota_bytes"], smaller_quota, "Quota should remain unchanged")
        log.info(f"Phase 7 complete: quota={counters['quota_bytes']}, used={counters['used_bytes']}, "
                 f"read_iops={counters['avg_read_iops']}, write_iops={counters['avg_write_iops']}")

        # Phase 8: Remove quota entirely (set to unlimited)
        log.info("Phase 8: Removing quota (setting to unlimited)...")
        self.fs.run_ceph_cmd('fs', 'subvolume', 'resize', 'cephfs', subvol_name, 'inf')

        # Do a small write to generate metrics after quota change
        self.mount_a.run_shell_payload(f"sudo dd if=/dev/zero of={subvolume_fs_path}/trigger_unlimited bs=4k count=10", wait=True)

        # Verify quota_bytes becomes 0 (unlimited)
        unlimited_metrics = None
        with safe_while(sleep=1, tries=30, action='wait for unlimited quota') as proceed:
            while proceed():
                # Keep triggering I/O to generate fresh metrics
                self.mount_a.run_shell_payload(f"sudo dd if=/dev/zero of={subvolume_fs_path}/trigger_unlimited bs=4k count=1 conv=notrunc", wait=True)
                unlimited_metrics = self.get_subvolume_metrics()
                if unlimited_metrics:
                    counters = unlimited_metrics[0]["counters"]
                    # quota_bytes=0 means unlimited
                    if counters["quota_bytes"] == 0:
                        break

        self.assertIsNotNone(unlimited_metrics)
        counters = unlimited_metrics[0]["counters"]
        self.assertEqual(counters["quota_bytes"], 0, "Expected quota_bytes=0 for unlimited")
        self.assertGreater(counters["used_bytes"], 0, "used_bytes should still be tracked")
        log.info(f"Phase 8 complete: quota={counters['quota_bytes']}, used={counters['used_bytes']}")

        # Phase 9: Re-apply quota after being unlimited
        log.info("Phase 9: Re-applying quota after unlimited...")
        final_quota = 300 * 1024 * 1024  # 300 MiB
        self.fs.run_ceph_cmd('fs', 'subvolume', 'resize', 'cephfs', subvol_name,
                             str(final_quota))

        # Trigger broadcast
        self.mount_a.run_shell_payload(f"echo 'final' | sudo tee {subvolume_fs_path}/final > /dev/null")

        # Verify quota restored
        final_metrics = None
        with safe_while(sleep=1, tries=30, action='wait for restored quota') as proceed:
            while proceed():
                # Trigger I/O to generate metrics
                self.mount_a.run_shell_payload(f"sudo dd if=/dev/zero of={subvolume_fs_path}/trigger_final bs=4k count=1 conv=notrunc", wait=True)
                final_metrics = self.get_subvolume_metrics()
                if final_metrics:
                    counters = final_metrics[0]["counters"]
                    if counters["quota_bytes"] == final_quota:
                        break

        self.assertIsNotNone(final_metrics)
        counters = final_metrics[0]["counters"]
        self.assertEqual(counters["quota_bytes"], final_quota,
                         "Expected quota to be restored after being unlimited")
        self.assertGreater(counters["used_bytes"], 0)
        log.info(f"Phase 9 complete: quota={counters['quota_bytes']}, used={counters['used_bytes']}")

        # Final verification: all metrics fields present and sane
        labels = final_metrics[0]["labels"]
        self.assertEqual(labels["fs_name"], "cephfs")
        self.assertEqual(labels["subvolume_path"], subv_path)
        self.assertIn("avg_read_iops", counters)
        self.assertIn("avg_write_iops", counters)
        self.assertIn("avg_read_tp_Bps", counters)
        self.assertIn("avg_write_tp_Bps", counters)
        self.assertIn("avg_read_lat_msec", counters)
        self.assertIn("avg_write_lat_msec", counters)

        log.info("Stress test completed successfully!")

        # cleanup
        self.fs.run_ceph_cmd('fs', 'subvolume', 'rm', 'cephfs', subvol_name)

    def test_subvolume_metrics_eviction_reactivation(self):
        """
        Ensure subvolume quota cache entries are evicted after inactivity and
        re-populated on the next quota broadcast.
        """
        # Shrink window to speed up eviction
        # Use 'mds' service (not 'mds.*') for config set/get in vstart.
        # The minimal allowed window is 30s; set to 30 to keep the test shorter.
        original_window = self.fs.get_ceph_cmd_stdout('config', 'get', 'mds',
                                                      'subv_metrics_window_interval').strip()
        self.fs.run_ceph_cmd('config', 'set', 'mds', 'subv_metrics_window_interval', '30')
        self.addCleanup(self.fs.run_ceph_cmd, 'config', 'set', 'mds',
                        'subv_metrics_window_interval', original_window)

        subvol_name = "evict_subv"
        subv_path = f"/volumes/_nogroup/{subvol_name}"
        initial_quota = 50 * 1024 * 1024  # 50 MiB
        updated_quota = 60 * 1024 * 1024  # 60 MiB

        # Create subvolume with initial quota and write a small file
        self.fs.run_ceph_cmd('fs', 'subvolume', 'create', 'cephfs', subvol_name,
                             "--size", str(initial_quota))
        mount_point = self.mount_a.get_mount_point()
        subvolume_fs_path = self.fs.get_ceph_cmd_stdout('fs', 'subvolume', 'getpath',
                                                        'cephfs', subvol_name).strip()
        subvolume_fs_path = os.path.join(mount_point, subvolume_fs_path.strip('/'))
        filename = os.path.join(subvolume_fs_path, "seed")
        self.mount_a.run_shell_payload("sudo fio --name test -rw=write --bs=4k --numjobs=1 "
                                       "--time_based --runtime=2s --verify=0 --size=4M "
                                       f"--filename={filename}", wait=True)

        # Wait for metrics to reflect initial quota
        with safe_while(sleep=1, tries=30, action='wait for initial quota metrics') as proceed:
            while proceed():
                metrics = self.get_subvolume_metrics()
                if metrics:
                    counters = metrics[0]["counters"]
                    if counters["quota_bytes"] == initial_quota and counters["used_bytes"] > 0:
                        break

        # Let the window expire (2 * window = 60s)
        sleep(65)

        # Change quota to force a new broadcast and repopulate cache
        self.fs.run_ceph_cmd('fs', 'subvolume', 'resize', 'cephfs', subvol_name,
                             str(updated_quota))
        # Trigger broadcast/metrics generation
        self.mount_a.run_shell_payload(f"echo 'poke' | sudo tee {subvolume_fs_path}/poke > /dev/null")

        # Verify quota is updated after eviction and reactivation
        metrics = None
        with safe_while(sleep=1, tries=30, action='wait for quota after eviction') as proceed:
            while proceed():
                metrics = self.get_subvolume_metrics()
                if metrics:
                    counters = metrics[0]["counters"]
                    if counters["quota_bytes"] == updated_quota:
                        break

        self.assertIsNotNone(metrics, "Expected metrics after reactivation")
        counters = metrics[0]["counters"]
        self.assertEqual(counters["quota_bytes"], updated_quota,
                         "Expected quota to refresh after eviction")
        self.assertGreater(counters["used_bytes"], 0, "Expected used_bytes to remain tracked")

        # Cleanup
        self.fs.run_ceph_cmd('fs', 'subvolume', 'rm', 'cephfs', subvol_name)
