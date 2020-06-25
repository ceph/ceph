import json
import time
import logging
from textwrap import dedent
import datetime
import gevent

from teuthology.orchestra.run import CommandFailedError, Raw
from tasks.cephfs.cephfs_test_case import CephFSTestCase, for_teuthology

log = logging.getLogger(__name__)


class TestStrays(CephFSTestCase):
    MDSS_REQUIRED = 2

    OPS_THROTTLE = 1
    FILES_THROTTLE = 2

    # Range of different file sizes used in throttle test's workload
    throttle_workload_size_range = 16

    @for_teuthology
    def test_ops_throttle(self):
        self._test_throttling(self.OPS_THROTTLE)

    @for_teuthology
    def test_files_throttle(self):
        self._test_throttling(self.FILES_THROTTLE)

    def test_dir_deletion(self):
        """
        That when deleting a bunch of dentries and the containing
        directory, everything gets purged.
        Catches cases where the client might e.g. fail to trim
        the unlinked dir from its cache.
        """
        file_count = 1000
        create_script = dedent("""
            import os

            mount_path = "{mount_path}"
            subdir = "delete_me"
            size = {size}
            file_count = {file_count}
            os.mkdir(os.path.join(mount_path, subdir))
            for i in range(0, file_count):
                filename = "{{0}}_{{1}}.bin".format(i, size)
                with open(os.path.join(mount_path, subdir, filename), 'w') as f:
                    f.write(size * 'x')
        """.format(
            mount_path=self.mount_a.mountpoint,
            size=1024,
            file_count=file_count
        ))

        self.mount_a.run_python(create_script)

        # That the dirfrag object is created
        self.fs.mds_asok(["flush", "journal"])
        dir_ino = self.mount_a.path_to_ino("delete_me")
        self.assertTrue(self.fs.dirfrag_exists(dir_ino, 0))

        # Remove everything
        self.mount_a.run_shell(["rm", "-rf", "delete_me"])
        self.fs.mds_asok(["flush", "journal"])

        # That all the removed files get created as strays
        strays = self.get_mdc_stat("strays_created")
        self.assertEqual(strays, file_count + 1)

        # That the strays all get enqueued for purge
        self.wait_until_equal(
            lambda: self.get_mdc_stat("strays_enqueued"),
            strays,
            timeout=600

        )

        # That all the purge operations execute
        self.wait_until_equal(
            lambda: self.get_stat("purge_queue", "pq_executed"),
            strays,
            timeout=600
        )

        # That finally, the directory metadata object is gone
        self.assertFalse(self.fs.dirfrag_exists(dir_ino, 0))

        # That finally, the data objects are all gone
        self.await_data_pool_empty()

    def _test_throttling(self, throttle_type):
        self.data_log = []
        try:
            return self._do_test_throttling(throttle_type)
        except:
            for l in self.data_log:
                log.info(",".join([l_.__str__() for l_ in l]))
            raise

    def _do_test_throttling(self, throttle_type):
        """
        That the mds_max_purge_ops setting is respected
        """

        def set_throttles(files, ops):
            """
            Helper for updating ops/files limits, and calculating effective
            ops_per_pg setting to give the same ops limit.
            """
            self.set_conf('mds', 'mds_max_purge_files', "%d" % files)
            self.set_conf('mds', 'mds_max_purge_ops', "%d" % ops)

            pgs = self.fs.mon_manager.get_pool_int_property(
                self.fs.get_data_pool_name(),
                "pg_num"
            )
            ops_per_pg = float(ops) / pgs
            self.set_conf('mds', 'mds_max_purge_ops_per_pg', "%s" % ops_per_pg)

        # Test conditions depend on what we're going to be exercising.
        # * Lift the threshold on whatever throttle we are *not* testing, so
        #   that the throttle of interest is the one that will be the bottleneck
        # * Create either many small files (test file count throttling) or fewer
        #   large files (test op throttling)
        if throttle_type == self.OPS_THROTTLE:
            set_throttles(files=100000000, ops=16)
            size_unit = 1024 * 1024  # big files, generate lots of ops
            file_multiplier = 100
        elif throttle_type == self.FILES_THROTTLE:
            # The default value of file limit is pretty permissive, so to avoid
            # the test running too fast, create lots of files and set the limit
            # pretty low.
            set_throttles(ops=100000000, files=6)
            size_unit = 1024  # small, numerous files
            file_multiplier = 200
        else:
            raise NotImplementedError(throttle_type)

        # Pick up config changes
        self.fs.mds_fail_restart()
        self.fs.wait_for_daemons()

        create_script = dedent("""
            import os

            mount_path = "{mount_path}"
            subdir = "delete_me"
            size_unit = {size_unit}
            file_multiplier = {file_multiplier}
            os.mkdir(os.path.join(mount_path, subdir))
            for i in range(0, file_multiplier):
                for size in range(0, {size_range}*size_unit, size_unit):
                    filename = "{{0}}_{{1}}.bin".format(i, size // size_unit)
                    with open(os.path.join(mount_path, subdir, filename), 'w') as f:
                        f.write(size * 'x')
        """.format(
            mount_path=self.mount_a.mountpoint,
            size_unit=size_unit,
            file_multiplier=file_multiplier,
            size_range=self.throttle_workload_size_range
        ))

        self.mount_a.run_python(create_script)

        # We will run the deletion in the background, to reduce the risk of it completing before
        # we have started monitoring the stray statistics.
        def background():
            self.mount_a.run_shell(["rm", "-rf", "delete_me"])
            self.fs.mds_asok(["flush", "journal"])

        background_thread = gevent.spawn(background)

        total_inodes = file_multiplier * self.throttle_workload_size_range + 1
        mds_max_purge_ops = int(self.fs.get_config("mds_max_purge_ops", 'mds'))
        mds_max_purge_files = int(self.fs.get_config("mds_max_purge_files", 'mds'))

        # During this phase we look for the concurrent ops to exceed half
        # the limit (a heuristic) and not exceed the limit (a correctness
        # condition).
        purge_timeout = 600
        elapsed = 0
        files_high_water = 0
        ops_high_water = 0

        while True:
            stats = self.fs.mds_asok(['perf', 'dump'])
            mdc_stats = stats['mds_cache']
            pq_stats = stats['purge_queue']
            if elapsed >= purge_timeout:
                raise RuntimeError("Timeout waiting for {0} inodes to purge, stats:{1}".format(total_inodes, mdc_stats))

            num_strays = mdc_stats['num_strays']
            num_strays_purging = pq_stats['pq_executing']
            num_purge_ops = pq_stats['pq_executing_ops']
            files_high_water = pq_stats['pq_executing_high_water']
            ops_high_water = pq_stats['pq_executing_ops_high_water']

            self.data_log.append([datetime.datetime.now(), num_strays, num_strays_purging, num_purge_ops, files_high_water, ops_high_water])

            total_strays_created = mdc_stats['strays_created']
            total_strays_purged = pq_stats['pq_executed']

            if total_strays_purged == total_inodes:
                log.info("Complete purge in {0} seconds".format(elapsed))
                break
            elif total_strays_purged > total_inodes:
                raise RuntimeError("Saw more strays than expected, mdc stats: {0}".format(mdc_stats))
            else:
                if throttle_type == self.OPS_THROTTLE:
                    # 11 is filer_max_purge_ops plus one for the backtrace:
                    # limit is allowed to be overshot by this much.
                    if num_purge_ops > mds_max_purge_ops + 11:
                        raise RuntimeError("num_purge_ops violates threshold {0}/{1}".format(
                            num_purge_ops, mds_max_purge_ops
                        ))
                elif throttle_type == self.FILES_THROTTLE:
                    if num_strays_purging > mds_max_purge_files:
                        raise RuntimeError("num_strays_purging violates threshold {0}/{1}".format(
                            num_strays_purging, mds_max_purge_files
                        ))
                else:
                    raise NotImplementedError(throttle_type)

                log.info("Waiting for purge to complete {0}/{1}, {2}/{3}".format(
                    num_strays_purging, num_strays,
                    total_strays_purged, total_strays_created
                ))
                time.sleep(1)
                elapsed += 1

        background_thread.join()

        # Check that we got up to a respectable rate during the purge.  This is totally
        # racy, but should be safeish unless the cluster is pathologically slow, or
        # insanely fast such that the deletions all pass before we have polled the
        # statistics.
        if throttle_type == self.OPS_THROTTLE:
            if ops_high_water < mds_max_purge_ops // 2:
                raise RuntimeError("Ops in flight high water is unexpectedly low ({0} / {1})".format(
                    ops_high_water, mds_max_purge_ops
                ))
            # The MDS may go over mds_max_purge_ops for some items, like a
            # heavily fragmented directory.  The throttle does not kick in
            # until *after* we reach or exceed the limit.  This is expected
            # because we don't want to starve the PQ or never purge a
            # particularly large file/directory.
            self.assertLessEqual(ops_high_water, mds_max_purge_ops+64)
        elif throttle_type == self.FILES_THROTTLE:
            if files_high_water < mds_max_purge_files // 2:
                raise RuntimeError("Files in flight high water is unexpectedly low ({0} / {1})".format(
                    files_high_water, mds_max_purge_files
                ))
            self.assertLessEqual(files_high_water, mds_max_purge_files)

        # Sanity check all MDC stray stats
        stats = self.fs.mds_asok(['perf', 'dump'])
        mdc_stats = stats['mds_cache']
        pq_stats = stats['purge_queue']
        self.assertEqual(mdc_stats['num_strays'], 0)
        self.assertEqual(mdc_stats['num_strays_delayed'], 0)
        self.assertEqual(pq_stats['pq_executing'], 0)
        self.assertEqual(pq_stats['pq_executing_ops'], 0)
        self.assertEqual(mdc_stats['strays_created'], total_inodes)
        self.assertEqual(mdc_stats['strays_enqueued'], total_inodes)
        self.assertEqual(pq_stats['pq_executed'], total_inodes)

    def get_mdc_stat(self, name, mds_id=None):
        return self.get_stat("mds_cache", name, mds_id)

    def get_stat(self, subsys, name, mds_id=None):
        return self.fs.mds_asok(['perf', 'dump', subsys, name],
                                mds_id=mds_id)[subsys][name]

    def _wait_for_counter(self, subsys, counter, expect_val, timeout=60,
                          mds_id=None):
        self.wait_until_equal(
            lambda: self.get_stat(subsys, counter, mds_id),
            expect_val=expect_val, timeout=timeout,
            reject_fn=lambda x: x > expect_val
        )

    def test_open_inode(self):
        """
        That the case of a dentry unlinked while a client holds an
        inode open is handled correctly.

        The inode should be moved into a stray dentry, while the original
        dentry and directory should be purged.

        The inode's data should be purged when the client eventually closes
        it.
        """
        mount_a_client_id = self.mount_a.get_global_id()

        # Write some bytes to a file
        size_mb = 8

        # Hold the file open
        p = self.mount_a.open_background("open_file")
        self.mount_a.write_n_mb("open_file", size_mb)
        open_file_ino = self.mount_a.path_to_ino("open_file")

        self.assertEqual(self.get_session(mount_a_client_id)['num_caps'], 2)

        # Unlink the dentry
        self.mount_a.run_shell(["rm", "-f", "open_file"])

        # Wait to see the stray count increment
        self.wait_until_equal(
            lambda: self.get_mdc_stat("num_strays"),
            expect_val=1, timeout=60, reject_fn=lambda x: x > 1)

        # See that while the stray count has incremented, none have passed
        # on to the purge queue
        self.assertEqual(self.get_mdc_stat("strays_created"), 1)
        self.assertEqual(self.get_mdc_stat("strays_enqueued"), 0)

        # See that the client still holds 2 caps
        self.assertEqual(self.get_session(mount_a_client_id)['num_caps'], 2)

        # See that the data objects remain in the data pool
        self.assertTrue(self.fs.data_objects_present(open_file_ino, size_mb * 1024 * 1024))

        # Now close the file
        self.mount_a.kill_background(p)

        # Wait to see the client cap count decrement
        self.wait_until_equal(
            lambda: self.get_session(mount_a_client_id)['num_caps'],
            expect_val=1, timeout=60, reject_fn=lambda x: x > 2 or x < 1
        )
        # Wait to see the purge counter increment, stray count go to zero
        self._wait_for_counter("mds_cache", "strays_enqueued", 1)
        self.wait_until_equal(
            lambda: self.get_mdc_stat("num_strays"),
            expect_val=0, timeout=6, reject_fn=lambda x: x > 1
        )
        self._wait_for_counter("purge_queue", "pq_executed", 1)

        # See that the data objects no longer exist
        self.assertTrue(self.fs.data_objects_absent(open_file_ino, size_mb * 1024 * 1024))

        self.await_data_pool_empty()

    def test_hardlink_reintegration(self):
        """
        That removal of primary dentry of hardlinked inode results
        in reintegration of inode into the previously-remote dentry,
        rather than lingering as a stray indefinitely.
        """
        # Write some bytes to file_a
        size_mb = 8
        self.mount_a.run_shell(["mkdir", "dir_1"])
        self.mount_a.write_n_mb("dir_1/file_a", size_mb)
        ino = self.mount_a.path_to_ino("dir_1/file_a")

        # Create a hardlink named file_b
        self.mount_a.run_shell(["mkdir", "dir_2"])
        self.mount_a.run_shell(["ln", "dir_1/file_a", "dir_2/file_b"])
        self.assertEqual(self.mount_a.path_to_ino("dir_2/file_b"), ino)

        # Flush journal
        self.fs.mds_asok(['flush', 'journal'])

        # See that backtrace for the file points to the file_a path
        pre_unlink_bt = self.fs.read_backtrace(ino)
        self.assertEqual(pre_unlink_bt['ancestors'][0]['dname'], "file_a")

        # empty mds cache. otherwise mds reintegrates stray when unlink finishes
        self.mount_a.umount_wait()
        self.fs.mds_asok(['flush', 'journal'])
        self.fs.mds_fail_restart()
        self.fs.wait_for_daemons()
        self.mount_a.mount_wait()

        # Unlink file_a
        self.mount_a.run_shell(["rm", "-f", "dir_1/file_a"])

        # See that a stray was created
        self.assertEqual(self.get_mdc_stat("num_strays"), 1)
        self.assertEqual(self.get_mdc_stat("strays_created"), 1)

        # Wait, see that data objects are still present (i.e. that the
        # stray did not advance to purging given time)
        time.sleep(30)
        self.assertTrue(self.fs.data_objects_present(ino, size_mb * 1024 * 1024))
        self.assertEqual(self.get_mdc_stat("strays_enqueued"), 0)

        # See that before reintegration, the inode's backtrace points to a stray dir
        self.fs.mds_asok(['flush', 'journal'])
        self.assertTrue(self.get_backtrace_path(ino).startswith("stray"))

        last_reintegrated = self.get_mdc_stat("strays_reintegrated")

        # Do a metadata operation on the remaining link (mv is heavy handed, but
        # others like touch may be satisfied from caps without poking MDS)
        self.mount_a.run_shell(["mv", "dir_2/file_b", "dir_2/file_c"])

        # Stray reintegration should happen as a result of the eval_remote call
        # on responding to a client request.
        self.wait_until_equal(
            lambda: self.get_mdc_stat("num_strays"),
            expect_val=0,
            timeout=60
        )

        # See the reintegration counter increment
        curr_reintegrated = self.get_mdc_stat("strays_reintegrated")
        self.assertGreater(curr_reintegrated, last_reintegrated)
        last_reintegrated = curr_reintegrated

        # Flush the journal
        self.fs.mds_asok(['flush', 'journal'])

        # See that the backtrace for the file points to the remaining link's path
        post_reint_bt = self.fs.read_backtrace(ino)
        self.assertEqual(post_reint_bt['ancestors'][0]['dname'], "file_c")

        # mds should reintegrates stray when unlink finishes
        self.mount_a.run_shell(["ln", "dir_2/file_c", "dir_2/file_d"])
        self.mount_a.run_shell(["rm", "-f", "dir_2/file_c"])

        # Stray reintegration should happen as a result of the notify_stray call
        # on completion of unlink
        self.wait_until_equal(
            lambda: self.get_mdc_stat("num_strays"),
            expect_val=0,
            timeout=60
        )

        # See the reintegration counter increment
        curr_reintegrated = self.get_mdc_stat("strays_reintegrated")
        self.assertGreater(curr_reintegrated, last_reintegrated)
        last_reintegrated = curr_reintegrated

        # Flush the journal
        self.fs.mds_asok(['flush', 'journal'])

        # See that the backtrace for the file points to the newest link's path
        post_reint_bt = self.fs.read_backtrace(ino)
        self.assertEqual(post_reint_bt['ancestors'][0]['dname'], "file_d")

        # Now really delete it
        self.mount_a.run_shell(["rm", "-f", "dir_2/file_d"])
        self._wait_for_counter("mds_cache", "strays_enqueued", 1)
        self._wait_for_counter("purge_queue", "pq_executed", 1)

        self.assert_purge_idle()
        self.assertTrue(self.fs.data_objects_absent(ino, size_mb * 1024 * 1024))

        # We caused the inode to go stray 3 times
        self.assertEqual(self.get_mdc_stat("strays_created"), 3)
        # We purged it at the last
        self.assertEqual(self.get_mdc_stat("strays_enqueued"), 1)

    def test_mv_hardlink_cleanup(self):
        """
        That when doing a rename from A to B, and B has hardlinks,
        then we make a stray for B which is then reintegrated
        into one of his hardlinks.
        """
        # Create file_a, file_b, and a hardlink to file_b
        size_mb = 8
        self.mount_a.write_n_mb("file_a", size_mb)
        file_a_ino = self.mount_a.path_to_ino("file_a")

        self.mount_a.write_n_mb("file_b", size_mb)
        file_b_ino = self.mount_a.path_to_ino("file_b")

        self.mount_a.run_shell(["ln", "file_b", "linkto_b"])
        self.assertEqual(self.mount_a.path_to_ino("linkto_b"), file_b_ino)

        # mv file_a file_b
        self.mount_a.run_shell(["mv", "file_a", "file_b"])

        # Stray reintegration should happen as a result of the notify_stray call on
        # completion of rename
        self.wait_until_equal(
            lambda: self.get_mdc_stat("num_strays"),
            expect_val=0,
            timeout=60
        )

        self.assertEqual(self.get_mdc_stat("strays_created"), 1)
        self.assertGreaterEqual(self.get_mdc_stat("strays_reintegrated"), 1)

        # No data objects should have been deleted, as both files still have linkage.
        self.assertTrue(self.fs.data_objects_present(file_a_ino, size_mb * 1024 * 1024))
        self.assertTrue(self.fs.data_objects_present(file_b_ino, size_mb * 1024 * 1024))

        self.fs.mds_asok(['flush', 'journal'])

        post_reint_bt = self.fs.read_backtrace(file_b_ino)
        self.assertEqual(post_reint_bt['ancestors'][0]['dname'], "linkto_b")

    def _setup_two_ranks(self):
        # Set up two MDSs
        self.fs.set_max_mds(2)

        # See that we have two active MDSs
        self.wait_until_equal(lambda: len(self.fs.get_active_names()), 2, 30,
                              reject_fn=lambda v: v > 2 or v < 1)

        active_mds_names = self.fs.get_active_names()
        rank_0_id = active_mds_names[0]
        rank_1_id = active_mds_names[1]
        log.info("Ranks 0 and 1 are {0} and {1}".format(
            rank_0_id, rank_1_id))

        # Get rid of other MDS daemons so that it's easier to know which
        # daemons to expect in which ranks after restarts
        for unneeded_mds in set(self.mds_cluster.mds_ids) - {rank_0_id, rank_1_id}:
            self.mds_cluster.mds_stop(unneeded_mds)
            self.mds_cluster.mds_fail(unneeded_mds)

        return rank_0_id, rank_1_id

    def _force_migrate(self, path, rank=1):
        """
        :param to_id: MDS id to move it to
        :param path: Filesystem path (string) to move
        :param watch_ino: Inode number to look for at destination to confirm move
        :return: None
        """
        self.mount_a.run_shell(["setfattr", "-n", "ceph.dir.pin", "-v", str(rank), path])
        rpath = "/"+path
        self._wait_subtrees([(rpath, rank)], rank=rank, path=rpath)

    def _is_stopped(self, rank):
        mds_map = self.fs.get_mds_map()
        return rank not in [i['rank'] for i in mds_map['info'].values()]

    def test_purge_on_shutdown(self):
        """
        That when an MDS rank is shut down, its purge queue is
        drained in the process.
        """
        rank_0_id, rank_1_id = self._setup_two_ranks()

        self.set_conf("mds.{0}".format(rank_1_id), 'mds_max_purge_files', "0")
        self.mds_cluster.mds_fail_restart(rank_1_id)
        self.fs.wait_for_daemons()

        file_count = 5

        self.mount_a.create_n_files("delete_me/file", file_count)

        self._force_migrate("delete_me")

        self.mount_a.run_shell(["rm", "-rf", Raw("delete_me/*")])
        self.mount_a.umount_wait()

        # See all the strays go into purge queue
        self._wait_for_counter("mds_cache", "strays_created", file_count, mds_id=rank_1_id)
        self._wait_for_counter("mds_cache", "strays_enqueued", file_count, mds_id=rank_1_id)
        self.assertEqual(self.get_stat("mds_cache", "num_strays", mds_id=rank_1_id), 0)

        # See nothing get purged from the purge queue (yet)
        time.sleep(10)
        self.assertEqual(self.get_stat("purge_queue", "pq_executed", mds_id=rank_1_id), 0)

        # Shut down rank 1
        self.fs.set_max_mds(1)

        # It shouldn't proceed past stopping because its still not allowed
        # to purge
        time.sleep(10)
        self.assertEqual(self.get_stat("purge_queue", "pq_executed", mds_id=rank_1_id), 0)
        self.assertFalse(self._is_stopped(1))

        # Permit the daemon to start purging again
        self.fs.mon_manager.raw_cluster_cmd('tell', 'mds.{0}'.format(rank_1_id),
                                            'injectargs',
                                            "--mds_max_purge_files 100")

        # It should now proceed through shutdown
        self.fs.wait_for_daemons(timeout=120)

        # ...and in the process purge all that data
        self.await_data_pool_empty()

    def test_migration_on_shutdown(self):
        """
        That when an MDS rank is shut down, any non-purgeable strays
        get migrated to another rank.
        """

        rank_0_id, rank_1_id = self._setup_two_ranks()

        # Create a non-purgeable stray in a ~mds1 stray directory
        # by doing a hard link and deleting the original file
        self.mount_a.run_shell_payload("""
mkdir dir_1 dir_2
touch dir_1/original
ln dir_1/original dir_2/linkto
""")

        self._force_migrate("dir_1")
        self._force_migrate("dir_2", rank=0)

        # empty mds cache. otherwise mds reintegrates stray when unlink finishes
        self.mount_a.umount_wait()
        self.fs.mds_asok(['flush', 'journal'], rank_1_id)
        self.fs.mds_asok(['cache', 'drop'], rank_1_id)

        self.mount_a.mount_wait()
        self.mount_a.run_shell(["rm", "-f", "dir_1/original"])
        self.mount_a.umount_wait()

        self._wait_for_counter("mds_cache", "strays_created", 1,
                               mds_id=rank_1_id)

        # Shut down rank 1
        self.fs.set_max_mds(1)
        self.fs.wait_for_daemons(timeout=120)

        # See that the stray counter on rank 0 has incremented
        self.assertEqual(self.get_mdc_stat("strays_created", rank_0_id), 1)

    def assert_backtrace(self, ino, expected_path):
        """
        Assert that the backtrace in the data pool for an inode matches
        an expected /foo/bar path.
        """
        expected_elements = expected_path.strip("/").split("/")
        bt = self.fs.read_backtrace(ino)
        actual_elements = list(reversed([dn['dname'] for dn in bt['ancestors']]))
        self.assertListEqual(expected_elements, actual_elements)

    def get_backtrace_path(self, ino):
        bt = self.fs.read_backtrace(ino)
        elements = reversed([dn['dname'] for dn in bt['ancestors']])
        return "/".join(elements)

    def assert_purge_idle(self):
        """
        Assert that the MDS perf counters indicate no strays exist and
        no ongoing purge activity.  Sanity check for when PurgeQueue should
        be idle.
        """
        mdc_stats = self.fs.mds_asok(['perf', 'dump', "mds_cache"])['mds_cache']
        pq_stats = self.fs.mds_asok(['perf', 'dump', "purge_queue"])['purge_queue']
        self.assertEqual(mdc_stats["num_strays"], 0)
        self.assertEqual(mdc_stats["num_strays_delayed"], 0)
        self.assertEqual(pq_stats["pq_executing"], 0)
        self.assertEqual(pq_stats["pq_executing_ops"], 0)

    def test_mv_cleanup(self):
        """
        That when doing a rename from A to B, and B has no hardlinks,
        then we make a stray for B and purge him.
        """
        # Create file_a and file_b, write some to both
        size_mb = 8
        self.mount_a.write_n_mb("file_a", size_mb)
        file_a_ino = self.mount_a.path_to_ino("file_a")
        self.mount_a.write_n_mb("file_b", size_mb)
        file_b_ino = self.mount_a.path_to_ino("file_b")

        self.fs.mds_asok(['flush', 'journal'])
        self.assert_backtrace(file_a_ino, "file_a")
        self.assert_backtrace(file_b_ino, "file_b")

        # mv file_a file_b
        self.mount_a.run_shell(['mv', 'file_a', 'file_b'])

        # See that stray counter increments
        self.assertEqual(self.get_mdc_stat("strays_created"), 1)
        # Wait for purge counter to increment
        self._wait_for_counter("mds_cache", "strays_enqueued", 1)
        self._wait_for_counter("purge_queue", "pq_executed", 1)

        self.assert_purge_idle()

        # file_b should have been purged
        self.assertTrue(self.fs.data_objects_absent(file_b_ino, size_mb * 1024 * 1024))

        # Backtrace should have updated from file_a to file_b
        self.fs.mds_asok(['flush', 'journal'])
        self.assert_backtrace(file_a_ino, "file_b")

        # file_a's data should still exist
        self.assertTrue(self.fs.data_objects_present(file_a_ino, size_mb * 1024 * 1024))

    def _pool_df(self, pool_name):
        """
        Return a dict like
            {
                "kb_used": 0,
                "bytes_used": 0,
                "max_avail": 19630292406,
                "objects": 0
            }

        :param pool_name: Which pool (must exist)
        """
        out = self.fs.mon_manager.raw_cluster_cmd("df", "--format=json-pretty")
        for p in json.loads(out)['pools']:
            if p['name'] == pool_name:
                return p['stats']

        raise RuntimeError("Pool '{0}' not found".format(pool_name))

    def await_data_pool_empty(self):
        self.wait_until_true(
            lambda: self._pool_df(
                self.fs.get_data_pool_name()
            )['objects'] == 0,
            timeout=60)

    def test_snapshot_remove(self):
        """
        That removal of a snapshot that references a now-unlinked file results
        in purging on the stray for the file.
        """
        # Enable snapshots
        self.fs.set_allow_new_snaps(True)

        # Create a dir with a file in it
        size_mb = 8
        self.mount_a.run_shell(["mkdir", "snapdir"])
        self.mount_a.run_shell(["mkdir", "snapdir/subdir"])
        self.mount_a.write_test_pattern("snapdir/subdir/file_a", size_mb * 1024 * 1024)
        file_a_ino = self.mount_a.path_to_ino("snapdir/subdir/file_a")

        # Snapshot the dir
        self.mount_a.run_shell(["mkdir", "snapdir/.snap/snap1"])

        # Cause the head revision to deviate from the snapshot
        self.mount_a.write_n_mb("snapdir/subdir/file_a", size_mb)

        # Flush the journal so that backtraces, dirfrag objects will actually be written
        self.fs.mds_asok(["flush", "journal"])

        # Unlink the file
        self.mount_a.run_shell(["rm", "-f", "snapdir/subdir/file_a"])
        self.mount_a.run_shell(["rmdir", "snapdir/subdir"])

        # Unmount the client because when I come back to check the data is still
        # in the file I don't want to just see what's in the page cache.
        self.mount_a.umount_wait()

        self.assertEqual(self.get_mdc_stat("strays_created"), 2)

        # FIXME: at this stage we see a purge and the stray count drops to
        # zero, but there's actually still a stray, so at the very
        # least the StrayManager stats code is slightly off

        self.mount_a.mount_wait()

        # See that the data from the snapshotted revision of the file is still present
        # and correct
        self.mount_a.validate_test_pattern("snapdir/.snap/snap1/subdir/file_a", size_mb * 1024 * 1024)

        # Remove the snapshot
        self.mount_a.run_shell(["rmdir", "snapdir/.snap/snap1"])

        # Purging file_a doesn't happen until after we've flushed the journal, because
        # it is referenced by the snapshotted subdir, and the snapshot isn't really
        # gone until the journal references to it are gone
        self.fs.mds_asok(["flush", "journal"])

        # Wait for purging to complete, which requires the OSDMap to propagate to the OSDs.
        # See also: http://tracker.ceph.com/issues/20072
        self.wait_until_true(
            lambda: self.fs.data_objects_absent(file_a_ino, size_mb * 1024 * 1024),
            timeout=60
        )

        # See that a purge happens now
        self._wait_for_counter("mds_cache", "strays_enqueued", 2)
        self._wait_for_counter("purge_queue", "pq_executed", 2)

        self.await_data_pool_empty()

    def test_fancy_layout(self):
        """
        purge stray file with fancy layout
        """

        file_name = "fancy_layout_file"
        self.mount_a.run_shell(["touch", file_name])

        file_layout = "stripe_unit=1048576 stripe_count=4 object_size=8388608"
        self.mount_a.setfattr(file_name, "ceph.file.layout", file_layout)

        # 35MB requires 7 objects
        size_mb = 35
        self.mount_a.write_n_mb(file_name, size_mb)

        self.mount_a.run_shell(["rm", "-f", file_name])
        self.fs.mds_asok(["flush", "journal"])

        # can't use self.fs.data_objects_absent here, it does not support fancy layout
        self.await_data_pool_empty()

    def test_dirfrag_limit(self):
        """
        That the directory fragment size cannot exceed mds_bal_fragment_size_max (using a limit of 50 in all configurations).

        That fragmentation (forced) will allow more entries to be created.

        That unlinking fails when the stray directory fragment becomes too large and that unlinking may continue once those strays are purged.
        """

        LOW_LIMIT = 50
        for mds in self.fs.get_daemon_names():
            self.fs.mds_asok(["config", "set", "mds_bal_fragment_size_max", str(LOW_LIMIT)], mds)

        try:
            self.mount_a.run_python(dedent("""
                import os
                path = os.path.join("{path}", "subdir")
                os.mkdir(path)
                for n in range(0, {file_count}):
                    with open(os.path.join(path, "%s" % n), 'w') as f:
                        f.write(str(n))
                """.format(
            path=self.mount_a.mountpoint,
            file_count=LOW_LIMIT+1
            )))
        except CommandFailedError:
            pass # ENOSPAC
        else:
            raise RuntimeError("fragment size exceeded")

        # Now test that we can go beyond the limit if we fragment the directory

        self.mount_a.run_python(dedent("""
            import os
            path = os.path.join("{path}", "subdir2")
            os.mkdir(path)
            for n in range(0, {file_count}):
                with open(os.path.join(path, "%s" % n), 'w') as f:
                    f.write(str(n))
            dfd = os.open(path, os.O_DIRECTORY)
            os.fsync(dfd)
            """.format(
        path=self.mount_a.mountpoint,
        file_count=LOW_LIMIT
        )))

        # Ensure that subdir2 is fragmented
        mds_id = self.fs.get_active_names()[0]
        self.fs.mds_asok(["dirfrag", "split", "/subdir2", "0/0", "1"], mds_id)

        # remount+flush (release client caps)
        self.mount_a.umount_wait()
        self.fs.mds_asok(["flush", "journal"], mds_id)
        self.mount_a.mount_wait()

        # Create 50% more files than the current fragment limit
        self.mount_a.run_python(dedent("""
            import os
            path = os.path.join("{path}", "subdir2")
            for n in range({file_count}, ({file_count}*3)//2):
                with open(os.path.join(path, "%s" % n), 'w') as f:
                    f.write(str(n))
            """.format(
        path=self.mount_a.mountpoint,
        file_count=LOW_LIMIT
        )))

        # Now test the stray directory size is limited and recovers
        strays_before = self.get_mdc_stat("strays_created")
        try:
            self.mount_a.run_python(dedent("""
                import os
                path = os.path.join("{path}", "subdir3")
                os.mkdir(path)
                for n in range({file_count}):
                    fpath = os.path.join(path, "%s" % n)
                    with open(fpath, 'w') as f:
                        f.write(str(n))
                    os.unlink(fpath)
                """.format(
            path=self.mount_a.mountpoint,
            file_count=LOW_LIMIT*10 # 10 stray directories, should collide before this count
            )))
        except CommandFailedError:
            pass # ENOSPAC
        else:
            raise RuntimeError("fragment size exceeded")

        strays_after = self.get_mdc_stat("strays_created")
        self.assertGreaterEqual(strays_after-strays_before, LOW_LIMIT)

        self._wait_for_counter("mds_cache", "strays_enqueued", strays_after)
        self._wait_for_counter("purge_queue", "pq_executed", strays_after)

        self.mount_a.run_python(dedent("""
            import os
            path = os.path.join("{path}", "subdir4")
            os.mkdir(path)
            for n in range({file_count}):
                fpath = os.path.join(path, "%s" % n)
                with open(fpath, 'w') as f:
                    f.write(str(n))
                os.unlink(fpath)
            """.format(
        path=self.mount_a.mountpoint,
        file_count=LOW_LIMIT
        )))

    def test_purge_queue_upgrade(self):
        """
        That when starting on a system with no purge queue in the metadata
        pool, we silently create one.
        :return:
        """

        self.mds_cluster.mds_stop()
        self.mds_cluster.mds_fail()
        self.fs.rados(["rm", "500.00000000"])
        self.mds_cluster.mds_restart()
        self.fs.wait_for_daemons()

    def test_replicated_delete_speed(self):
        """
        That deletions of replicated metadata are not pathologically slow
        """
        rank_0_id, rank_1_id = self._setup_two_ranks()

        self.set_conf("mds.{0}".format(rank_1_id), 'mds_max_purge_files', "0")
        self.mds_cluster.mds_fail_restart(rank_1_id)
        self.fs.wait_for_daemons()

        file_count = 10

        self.mount_a.create_n_files("delete_me/file", file_count)

        self._force_migrate("delete_me")

        begin = datetime.datetime.now()
        self.mount_a.run_shell(["rm", "-rf", Raw("delete_me/*")])
        end = datetime.datetime.now()

        # What we're really checking here is that we are completing client
        # operations immediately rather than delaying until the next tick.
        tick_period = float(self.fs.get_config("mds_tick_interval",
                                               service_type="mds"))

        duration = (end - begin).total_seconds()
        self.assertLess(duration, (file_count * tick_period) * 0.25)
