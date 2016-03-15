import json
import logging
from textwrap import dedent
import time
import gevent
from tasks.cephfs.cephfs_test_case import CephFSTestCase, long_running

log = logging.getLogger(__name__)


class TestStrays(CephFSTestCase):
    MDSS_REQUIRED = 2

    OPS_THROTTLE = 1
    FILES_THROTTLE = 2

    # Range of different file sizes used in throttle test's workload
    throttle_workload_size_range = 16

    @long_running
    def test_ops_throttle(self):
        self._test_throttling(self.OPS_THROTTLE)

    @long_running
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
            for i in xrange(0, file_count):
                filename = "{{0}}_{{1}}.bin".format(i, size)
                f = open(os.path.join(mount_path, subdir, filename), 'w')
                f.write(size * 'x')
                f.close()
        """.format(
            mount_path=self.mount_a.mountpoint,
            size=1024,
            file_count=file_count
        ))

        self.mount_a.run_python(create_script)
        self.mount_a.run_shell(["rm", "-rf", "delete_me"])
        self.fs.mds_asok(["flush", "journal"])
        strays = self.get_mdc_stat("strays_created")
        self.assertEqual(strays, file_count + 1)
        self.wait_until_equal(
            lambda: self.get_mdc_stat("strays_purged"),
            strays,
            timeout=600

        )

    def _test_throttling(self, throttle_type):
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

            pgs = self.fs.mon_manager.get_pool_property(
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
            raise NotImplemented(throttle_type)

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
            for i in xrange(0, file_multiplier):
                for size in xrange(0, {size_range}*size_unit, size_unit):
                    filename = "{{0}}_{{1}}.bin".format(i, size / size_unit)
                    f = open(os.path.join(mount_path, subdir, filename), 'w')
                    f.write(size * 'x')
                    f.close()
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
            mdc_stats = self.fs.mds_asok(['perf', 'dump', 'mds_cache'])['mds_cache']
            if elapsed >= purge_timeout:
                raise RuntimeError("Timeout waiting for {0} inodes to purge, stats:{1}".format(total_inodes, mdc_stats))

            num_strays = mdc_stats['num_strays']
            num_strays_purging = mdc_stats['num_strays_purging']
            num_purge_ops = mdc_stats['num_purge_ops']

            files_high_water = max(files_high_water, num_strays_purging)
            ops_high_water = max(ops_high_water, num_purge_ops)

            total_strays_created = mdc_stats['strays_created']
            total_strays_purged = mdc_stats['strays_purged']

            if total_strays_purged == total_inodes:
                log.info("Complete purge in {0} seconds".format(elapsed))
                break
            elif total_strays_purged > total_inodes:
                raise RuntimeError("Saw more strays than expected, mdc stats: {0}".format(mdc_stats))
            else:
                if throttle_type == self.OPS_THROTTLE:
                    if num_strays_purging > mds_max_purge_files:
                        raise RuntimeError("num_purge_ops violates threshold {0}/{1}".format(
                            num_purge_ops, mds_max_purge_ops
                        ))
                elif throttle_type == self.FILES_THROTTLE:
                    if num_strays_purging > mds_max_purge_files:
                        raise RuntimeError("num_strays_purging violates threshold {0}/{1}".format(
                            num_strays_purging, mds_max_purge_files
                        ))
                else:
                    raise NotImplemented(throttle_type)

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
            if ops_high_water < mds_max_purge_ops / 2:
                raise RuntimeError("Ops in flight high water is unexpectedly low ({0} / {1})".format(
                    ops_high_water, mds_max_purge_ops
                ))
        elif throttle_type == self.FILES_THROTTLE:
            if files_high_water < mds_max_purge_files / 2:
                raise RuntimeError("Files in flight high water is unexpectedly low ({0} / {1})".format(
                    ops_high_water, mds_max_purge_files
                ))

        # Sanity check all MDC stray stats
        mdc_stats = self.fs.mds_asok(['perf', 'dump', 'mds_cache'])['mds_cache']
        self.assertEqual(mdc_stats['num_strays'], 0)
        self.assertEqual(mdc_stats['num_strays_purging'], 0)
        self.assertEqual(mdc_stats['num_strays_delayed'], 0)
        self.assertEqual(mdc_stats['num_purge_ops'], 0)
        self.assertEqual(mdc_stats['strays_created'], total_inodes)
        self.assertEqual(mdc_stats['strays_purged'], total_inodes)

    def get_mdc_stat(self, name, mds_id=None):
        return self.fs.mds_asok(['perf', 'dump', "mds_cache", name],
                                mds_id=mds_id)['mds_cache'][name]

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
        self.mount_a.write_n_mb("open_file", size_mb)
        open_file_ino = self.mount_a.path_to_ino("open_file")

        # Hold the file open
        p = self.mount_a.open_background("open_file")

        self.assertEqual(self.get_session(mount_a_client_id)['num_caps'], 2)

        # Unlink the dentry
        self.mount_a.run_shell(["rm", "-f", "open_file"])

        # Wait to see the stray count increment
        self.wait_until_equal(
            lambda: self.get_mdc_stat("num_strays"),
            expect_val=1, timeout=60, reject_fn=lambda x: x > 1)

        # See that while the stray count has incremented, the purge count
        # has not
        self.assertEqual(self.get_mdc_stat("strays_created"), 1)
        self.assertEqual(self.get_mdc_stat("strays_purged"), 0)

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
        self.wait_until_equal(
            lambda: self.get_mdc_stat("strays_purged"),
            expect_val=1, timeout=60, reject_fn=lambda x: x > 1
        )
        self.wait_until_equal(
            lambda: self.get_mdc_stat("num_strays"),
            expect_val=0, timeout=6, reject_fn=lambda x: x > 1
        )

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
        self.mount_a.write_n_mb("file_a", size_mb)
        ino = self.mount_a.path_to_ino("file_a")

        # Create a hardlink named file_b
        self.mount_a.run_shell(["ln", "file_a", "file_b"])
        self.assertEqual(self.mount_a.path_to_ino("file_b"), ino)

        # Flush journal
        self.fs.mds_asok(['flush', 'journal'])

        # See that backtrace for the file points to the file_a path
        pre_unlink_bt = self.fs.read_backtrace(ino)
        self.assertEqual(pre_unlink_bt['ancestors'][0]['dname'], "file_a")

        # Unlink file_a
        self.mount_a.run_shell(["rm", "-f", "file_a"])

        # See that a stray was created
        self.assertEqual(self.get_mdc_stat("num_strays"), 1)
        self.assertEqual(self.get_mdc_stat("strays_created"), 1)

        # Wait, see that data objects are still present (i.e. that the
        # stray did not advance to purging given time)
        time.sleep(30)
        self.assertTrue(self.fs.data_objects_present(ino, size_mb * 1024 * 1024))
        self.assertEqual(self.get_mdc_stat("strays_purged"), 0)

        # See that before reintegration, the inode's backtrace points to a stray dir
        self.fs.mds_asok(['flush', 'journal'])
        self.assertTrue(self.get_backtrace_path(ino).startswith("stray"))

        # Do a metadata operation on the remaining link (mv is heavy handed, but
        # others like touch may be satisfied from caps without poking MDS)
        self.mount_a.run_shell(["mv", "file_b", "file_c"])

        # See the reintegration counter increment
        # This should happen as a result of the eval_remote call on
        # responding to a client request.
        self.wait_until_equal(
            lambda: self.get_mdc_stat("strays_reintegrated"),
            expect_val=1, timeout=60, reject_fn=lambda x: x > 1
        )

        # Flush the journal
        self.fs.mds_asok(['flush', 'journal'])

        # See that the backtrace for the file points to the remaining link's path
        post_reint_bt = self.fs.read_backtrace(ino)
        self.assertEqual(post_reint_bt['ancestors'][0]['dname'], "file_c")

        # See that the number of strays in existence is zero
        self.assertEqual(self.get_mdc_stat("num_strays"), 0)

        # Now really delete it
        self.mount_a.run_shell(["rm", "-f", "file_c"])
        self.wait_until_equal(
            lambda: self.get_mdc_stat("strays_purged"),
            expect_val=1, timeout=60, reject_fn=lambda x: x > 1
        )
        self.assert_purge_idle()
        self.assertTrue(self.fs.data_objects_absent(ino, size_mb * 1024 * 1024))

        # We caused the inode to go stray twice
        self.assertEqual(self.get_mdc_stat("strays_created"), 2)
        # One time we reintegrated it
        self.assertEqual(self.get_mdc_stat("strays_reintegrated"), 1)
        # Then the second time we purged it
        self.assertEqual(self.get_mdc_stat("strays_purged"), 1)

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

        self.fs.mds_asok(['flush', 'journal'])

        # Initially, linkto_b will still be a remote inode pointing to a newly created
        # stray from when file_b was unlinked due to the 'mv'.  No data objects should
        # have been deleted, as both files still have linkage.
        self.assertEqual(self.get_mdc_stat("num_strays"), 1)
        self.assertEqual(self.get_mdc_stat("strays_created"), 1)
        self.assertTrue(self.get_backtrace_path(file_b_ino).startswith("stray"))
        self.assertTrue(self.fs.data_objects_present(file_a_ino, size_mb * 1024 * 1024))
        self.assertTrue(self.fs.data_objects_present(file_b_ino, size_mb * 1024 * 1024))

        # Trigger reintegration and wait for it to happen
        self.assertEqual(self.get_mdc_stat("strays_reintegrated"), 0)
        self.mount_a.run_shell(["mv", "linkto_b", "file_c"])
        self.wait_until_equal(
            lambda: self.get_mdc_stat("strays_reintegrated"),
            expect_val=1, timeout=60, reject_fn=lambda x: x > 1
        )

        self.fs.mds_asok(['flush', 'journal'])

        post_reint_bt = self.fs.read_backtrace(file_b_ino)
        self.assertEqual(post_reint_bt['ancestors'][0]['dname'], "file_c")
        self.assertEqual(self.get_mdc_stat("num_strays"), 0)

    def test_migration_on_shutdown(self):
        """
        That when an MDS rank is shut down, any not-yet-purging strays
        are migrated to another MDS's stray dir.
        """

        # Set up two MDSs
        self.fs.mon_manager.raw_cluster_cmd_result('mds', 'set', "max_mds", "2")

        # See that we have two active MDSs
        self.wait_until_equal(lambda: len(self.fs.get_active_names()), 2, 30,
                              reject_fn=lambda v: v > 2 or v < 1)

        active_mds_names = self.fs.get_active_names()
        rank_0_id = active_mds_names[0]
        rank_1_id = active_mds_names[1]
        log.info("Ranks 0 and 1 are {0} and {1}".format(
            rank_0_id, rank_1_id))

        # Set the purge file throttle to 0 on MDS rank 1
        self.set_conf("mds.{0}".format(rank_1_id), 'mds_max_purge_files', "0")
        self.fs.mds_fail_restart(rank_1_id)
        self.wait_until_equal(lambda: len(self.fs.get_active_names()), 2, 30,
                              reject_fn=lambda v: v > 2 or v < 1)

        # Create a file
        # Export dir on an empty dir doesn't work, so we create the file before
        # calling export dir in order to kick a dirfrag into existence
        size_mb = 8
        self.mount_a.run_shell(["mkdir", "ALPHA"])
        self.mount_a.write_n_mb("ALPHA/alpha_file", size_mb)
        ino = self.mount_a.path_to_ino("ALPHA/alpha_file")

        result = self.fs.mds_asok(["export", "dir", "/ALPHA", "1"], rank_0_id)
        self.assertEqual(result["return_code"], 0)

        # Pool the MDS cache dump to watch for the export completing
        migrated = False
        migrate_timeout = 60
        migrate_elapsed = 0
        while not migrated:
            data = self.fs.mds_asok(["dump", "cache"], rank_1_id)
            for inode_data in data:
                if inode_data['ino'] == ino:
                    log.debug("Found ino in cache: {0}".format(json.dumps(inode_data, indent=2)))
                    if inode_data['is_auth'] is True:
                        migrated = True
                    break

            if not migrated:
                if migrate_elapsed > migrate_timeout:
                    raise RuntimeError("Migration hasn't happened after {0}s!".format(migrate_elapsed))
                else:
                    migrate_elapsed += 1
                    time.sleep(1)

        # Delete the file on rank 1
        self.mount_a.run_shell(["rm", "-f", "ALPHA/alpha_file"])

        # See the stray counter increment, but the purge counter doesn't
        # See that the file objects are still on disk
        self.wait_until_equal(
            lambda: self.get_mdc_stat("num_strays", rank_1_id),
            expect_val=1, timeout=60, reject_fn=lambda x: x > 1)
        self.assertEqual(self.get_mdc_stat("strays_created", rank_1_id), 1)
        time.sleep(60)  # period that we want to see if it gets purged
        self.assertEqual(self.get_mdc_stat("strays_purged", rank_1_id), 0)
        self.assertTrue(self.fs.data_objects_present(ino, size_mb * 1024 * 1024))

        # Shut down rank 1
        self.fs.mon_manager.raw_cluster_cmd_result('mds', 'set', "max_mds", "1")
        self.fs.mon_manager.raw_cluster_cmd_result('mds', 'deactivate', "1")

        # Wait til we get to a single active MDS mdsmap state
        def is_stopped():
            mds_map = self.fs.get_mds_map()
            return 1 not in [i['rank'] for i in mds_map['info'].values()]

        self.wait_until_true(is_stopped, timeout=120)

        # See that the stray counter on rank 0 has incremented
        self.assertEqual(self.get_mdc_stat("strays_created", rank_0_id), 1)

        # Wait til the purge counter on rank 0 increments
        self.wait_until_equal(
            lambda: self.get_mdc_stat("strays_purged", rank_0_id),
            1, timeout=60, reject_fn=lambda x: x > 1)

        # See that the file objects no longer exist
        self.assertTrue(self.fs.data_objects_absent(ino, size_mb * 1024 * 1024))

        self.await_data_pool_empty()

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
        stats = self.fs.mds_asok(['perf', 'dump', "mds_cache"])['mds_cache']
        self.assertEqual(stats["num_strays"], 0)
        self.assertEqual(stats["num_strays_purging"], 0)
        self.assertEqual(stats["num_strays_delayed"], 0)
        self.assertEqual(stats["num_purge_ops"], 0)

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
        self.wait_until_equal(
            lambda: self.get_mdc_stat("strays_purged"),
            expect_val=1, timeout=60, reject_fn=lambda x: x > 1
        )
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
        self.fs.mon_manager.raw_cluster_cmd("mds", "set", "allow_new_snaps", "true",
                                            "--yes-i-really-mean-it")

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

        self.mount_a.mount()

        # See that the data from the snapshotted revision of the file is still present
        # and correct
        self.mount_a.validate_test_pattern("snapdir/.snap/snap1/subdir/file_a", size_mb * 1024 * 1024)

        # Remove the snapshot
        self.mount_a.run_shell(["rmdir", "snapdir/.snap/snap1"])
        self.mount_a.umount_wait()

        # Purging file_a doesn't happen until after we've flushed the journal, because
        # it is referenced by the snapshotted subdir, and the snapshot isn't really
        # gone until the journal references to it are gone
        self.fs.mds_asok(["flush", "journal"])

        # See that a purge happens now
        self.wait_until_equal(
            lambda: self.get_mdc_stat("strays_purged"),
            expect_val=2, timeout=60, reject_fn=lambda x: x > 1
        )

        self.assertTrue(self.fs.data_objects_absent(file_a_ino, size_mb * 1024 * 1024))
        self.await_data_pool_empty()

    def test_fancy_layout(self):
        """
        purge stray file with fancy layout
        """

        file_name = "fancy_layout_file"
        self.mount_a.run_shell(["touch", file_name])

        file_layout = "stripe_unit=1048576 stripe_count=4 object_size=8388608"
        self.mount_a.run_shell(["setfattr", "-n", "ceph.file.layout", "-v", file_layout, file_name])

        # 35MB requires 7 objects
        size_mb = 35
        self.mount_a.write_n_mb(file_name, size_mb)

        self.mount_a.run_shell(["rm", "-f", file_name])
        self.fs.mds_asok(["flush", "journal"])

        # can't use self.fs.data_objects_absent here, it does not support fancy layout
        self.await_data_pool_empty()
