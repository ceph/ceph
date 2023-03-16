import json
import logging
import os
from textwrap import dedent
from typing import Optional
from teuthology.exceptions import CommandFailedError
from tasks.cephfs.fuse_mount import FuseMount
from tasks.cephfs.cephfs_test_case import CephFSTestCase


log = logging.getLogger(__name__)


class FullnessTestCase(CephFSTestCase):
    CLIENTS_REQUIRED = 2

    # Subclasses define whether they're filling whole cluster or just data pool
    data_only = False

    # Subclasses define how many bytes should be written to achieve fullness
    pool_capacity: Optional[int] = None
    fill_mb = None

    def is_full(self):
        return self.fs.is_full()

    def setUp(self):
        CephFSTestCase.setUp(self)

        mds_status = self.fs.rank_asok(["status"])

        # Capture the initial OSD map epoch for later use
        self.initial_osd_epoch = mds_status['osdmap_epoch_barrier']

    def test_barrier(self):
        """
        That when an OSD epoch barrier is set on an MDS, subsequently
        issued capabilities cause clients to update their OSD map to that
        epoch.
        """

        # script that sync up client with MDS OSD map barrier. The barrier should
        # be updated by cap flush ack message.
        pyscript = dedent("""
            import os
            fd = os.open("{path}", os.O_CREAT | os.O_RDWR, 0O600)
            os.fchmod(fd, 0O666)
            os.fsync(fd)
            os.close(fd)
            """)

        # Sync up client with initial MDS OSD map barrier.
        path = os.path.join(self.mount_a.mountpoint, "foo")
        self.mount_a.run_python(pyscript.format(path=path))

        # Grab mounts' initial OSD epochs: later we will check that
        # it hasn't advanced beyond this point.
        mount_a_initial_epoch, mount_a_initial_barrier = self.mount_a.get_osd_epoch()

        # Freshly mounted at start of test, should be up to date with OSD map
        self.assertGreaterEqual(mount_a_initial_epoch, self.initial_osd_epoch)

        # Set and unset a flag to cause OSD epoch to increment
        self.get_ceph_cmd_stdout("osd", "set", "pause")
        self.get_ceph_cmd_stdout("osd", "unset", "pause")

        out = self.get_ceph_cmd_stdout("osd", "dump", "--format=json").strip()
        new_epoch = json.loads(out)['epoch']
        self.assertNotEqual(self.initial_osd_epoch, new_epoch)

        # Do a metadata operation on clients, witness that they end up with
        # the old OSD map from startup time (nothing has prompted client
        # to update its map)
        path = os.path.join(self.mount_a.mountpoint, "foo")
        self.mount_a.run_python(pyscript.format(path=path))
        mount_a_epoch, mount_a_barrier = self.mount_a.get_osd_epoch()
        self.assertEqual(mount_a_epoch, mount_a_initial_epoch)
        self.assertEqual(mount_a_barrier, mount_a_initial_barrier)

        # Set a barrier on the MDS
        self.fs.rank_asok(["osdmap", "barrier", new_epoch.__str__()])

        # Sync up client with new MDS OSD map barrier
        path = os.path.join(self.mount_a.mountpoint, "baz")
        self.mount_a.run_python(pyscript.format(path=path))
        mount_a_epoch, mount_a_barrier = self.mount_a.get_osd_epoch()
        self.assertEqual(mount_a_barrier, new_epoch)

        # Some time passes here because the metadata part of the operation
        # completes immediately, while the resulting OSD map update happens
        # asynchronously (it's an Objecter::_maybe_request_map) as a result
        # of seeing the new epoch barrier.
        self.wait_until_true(
            lambda: self.mount_a.get_osd_epoch()[0] >= new_epoch,
            timeout=30)

    def _data_pool_name(self):
        data_pool_names = self.fs.get_data_pool_names()
        if len(data_pool_names) > 1:
            raise RuntimeError("This test can't handle multiple data pools")
        else:
            return data_pool_names[0]

    def _test_full(self, easy_case):
        """
        - That a client trying to write data to a file is prevented
        from doing so with an -EFULL result
        - That they are also prevented from creating new files by the MDS.
        - That they may delete another file to get the system healthy again

        :param easy_case: if true, delete a successfully written file to
                          free up space.  else, delete the file that experienced
                          the failed write.
        """

        osd_mon_report_interval = int(self.fs.get_config("osd_mon_report_interval", service_type='osd'))

        log.info("Writing {0}MB should fill this cluster".format(self.fill_mb))

        # Fill up the cluster.  This dd may or may not fail, as it depends on
        # how soon the cluster recognises its own fullness
        self.mount_a.write_n_mb("large_file_a", self.fill_mb // 2)
        try:
            self.mount_a.write_n_mb("large_file_b", (self.fill_mb * 1.1) // 2)
        except CommandFailedError:
            log.info("Writing file B failed (full status happened already)")
            assert self.is_full()
        else:
            log.info("Writing file B succeeded (full status will happen soon)")
            self.wait_until_true(lambda: self.is_full(),
                                 timeout=osd_mon_report_interval * 120)

        # Attempting to write more data should give me ENOSPC
        with self.assertRaises(CommandFailedError) as ar:
            self.mount_a.write_n_mb("large_file_b", 50, seek=self.fill_mb // 2)
        self.assertEqual(ar.exception.exitstatus, 1)  # dd returns 1 on "No space"

        # Wait for the MDS to see the latest OSD map so that it will reliably
        # be applying the policy of rejecting non-deletion metadata operations
        # while in the full state.
        osd_epoch = json.loads(self.get_ceph_cmd_stdout("osd", "dump", "--format=json-pretty"))['epoch']
        self.wait_until_true(
            lambda: self.fs.rank_asok(['status'])['osdmap_epoch'] >= osd_epoch,
            timeout=10)

        if not self.data_only:
            with self.assertRaises(CommandFailedError):
                self.mount_a.write_n_mb("small_file_1", 0)

        # Clear out some space
        if easy_case:
            self.mount_a.run_shell(['rm', '-f', 'large_file_a'])
            self.mount_a.run_shell(['rm', '-f', 'large_file_b'])
        else:
            # In the hard case it is the file that filled the system.
            # Before the new #7317 (ENOSPC, epoch barrier) changes, this
            # would fail because the last objects written would be
            # stuck in the client cache as objecter operations.
            self.mount_a.run_shell(['rm', '-f', 'large_file_b'])
            self.mount_a.run_shell(['rm', '-f', 'large_file_a'])

        # Here we are waiting for two things to happen:
        # * The MDS to purge the stray folder and execute object deletions
        #  * The OSDs to inform the mon that they are no longer full
        self.wait_until_true(lambda: not self.is_full(),
                             timeout=osd_mon_report_interval * 120)

        # Wait for the MDS to see the latest OSD map so that it will reliably
        # be applying the free space policy
        osd_epoch = json.loads(self.get_ceph_cmd_stdout("osd", "dump", "--format=json-pretty"))['epoch']
        self.wait_until_true(
            lambda: self.fs.rank_asok(['status'])['osdmap_epoch'] >= osd_epoch,
            timeout=10)

        # Now I should be able to write again
        self.mount_a.write_n_mb("large_file", 50, seek=0)

        # Ensure that the MDS keeps its OSD epoch barrier across a restart

    def test_full_different_file(self):
        self._test_full(True)

    def test_full_same_file(self):
        self._test_full(False)

    def _remote_write_test(self, template):
        """
        Run some remote python in a way that's useful for
        testing free space behaviour (see test_* methods using this)
        """
        file_path = os.path.join(self.mount_a.mountpoint, "full_test_file")

        # Enough to trip the full flag
        osd_mon_report_interval = int(self.fs.get_config("osd_mon_report_interval", service_type='osd'))
        mon_tick_interval = int(self.fs.get_config("mon_tick_interval", service_type="mon"))

        # Sufficient data to cause RADOS cluster to go 'full'
        log.info("pool capacity {0}, {1}MB should be enough to fill it".format(self.pool_capacity, self.fill_mb))

        # Long enough for RADOS cluster to notice it is full and set flag on mons
        # (report_interval for mon to learn PG stats, tick interval for it to update OSD map,
        #  factor of 1.5 for I/O + network latency in committing OSD map and distributing it
        #  to the OSDs)
        full_wait = (osd_mon_report_interval + mon_tick_interval) * 1.5

        # Configs for this test should bring this setting down in order to
        # run reasonably quickly
        if osd_mon_report_interval > 10:
            log.warning("This test may run rather slowly unless you decrease"
                     "osd_mon_report_interval (5 is a good setting)!")

        # set the object_size to 1MB to make the objects destributed more evenly
        # among the OSDs to fix Tracker#45434
        file_layout = "stripe_unit=1048576 stripe_count=1 object_size=1048576"
        self.mount_a.run_python(template.format(
            fill_mb=self.fill_mb,
            file_path=file_path,
            file_layout=file_layout,
            full_wait=full_wait,
            is_fuse=isinstance(self.mount_a, FuseMount)
        ))

    def test_full_fclose(self):
        # A remote script which opens a file handle, fills up the filesystem, and then
        # checks that ENOSPC errors on buffered writes appear correctly as errors in fsync
        remote_script = dedent("""
            import time
            import datetime
            import subprocess
            import os

            # Write some buffered data through before going full, all should be well
            print("writing some data through which we expect to succeed")
            bytes = 0
            f = os.open("{file_path}", os.O_WRONLY | os.O_CREAT)
            os.setxattr("{file_path}", 'ceph.file.layout', b'{file_layout}')
            bytes += os.write(f, b'a' * 512 * 1024)
            os.fsync(f)
            print("fsync'ed data successfully, will now attempt to fill fs")

            # Okay, now we're going to fill up the filesystem, and then keep
            # writing until we see an error from fsync.  As long as we're doing
            # buffered IO, the error should always only appear from fsync and not
            # from write
            full = False

            for n in range(0, int({fill_mb} * 0.9)):
                bytes += os.write(f, b'x' * 1024 * 1024)
                print("wrote {{0}} bytes via buffered write, may repeat".format(bytes))
            print("done writing {{0}} bytes".format(bytes))

            # OK, now we should sneak in under the full condition
            # due to the time it takes the OSDs to report to the
            # mons, and get a successful fsync on our full-making data
            os.fsync(f)
            print("successfully fsync'ed prior to getting full state reported")

            # buffered write, add more dirty data to the buffer
            print("starting buffered write")
            try:
                for n in range(0, int({fill_mb} * 0.2)):
                    bytes += os.write(f, b'x' * 1024 * 1024)
                    print("sleeping a bit as we've exceeded 90% of our expected full ratio")
                    time.sleep({full_wait})
            except OSError:
                pass;

            print("wrote, now waiting 30s and then doing a close we expect to fail")

            # Wait long enough for a background flush that should fail
            time.sleep(30)

            if {is_fuse}:
                # ...and check that the failed background flush is reflected in fclose
                try:
                    os.close(f)
                except OSError:
                    print("close() returned an error as expected")
                else:
                    raise RuntimeError("close() failed to raise error")
            else:
                # The kernel cephfs client does not raise errors on fclose
                os.close(f)

            os.unlink("{file_path}")
            """)
        self._remote_write_test(remote_script)

    def test_full_fsync(self):
        """
        That when the full flag is encountered during asynchronous
        flushes, such that an fwrite() succeeds but an fsync/fclose()
        should return the ENOSPC error.
        """

        # A remote script which opens a file handle, fills up the filesystem, and then
        # checks that ENOSPC errors on buffered writes appear correctly as errors in fsync
        remote_script = dedent("""
            import time
            import datetime
            import subprocess
            import os

            # Write some buffered data through before going full, all should be well
            print("writing some data through which we expect to succeed")
            bytes = 0
            f = os.open("{file_path}", os.O_WRONLY | os.O_CREAT)
            os.setxattr("{file_path}", 'ceph.file.layout', b'{file_layout}')
            bytes += os.write(f, b'a' * 4096)
            os.fsync(f)
            print("fsync'ed data successfully, will now attempt to fill fs")

            # Okay, now we're going to fill up the filesystem, and then keep
            # writing until we see an error from fsync.  As long as we're doing
            # buffered IO, the error should always only appear from fsync and not
            # from write
            full = False

            for n in range(0, int({fill_mb} * 1.1)):
                try:
                    bytes += os.write(f, b'x' * 1024 * 1024)
                    print("wrote bytes via buffered write, moving on to fsync")
                except OSError as e:
                    if {is_fuse}:
                        print("Unexpected error %s from write() instead of fsync()" % e)
                        raise
                    else:
                        print("Reached fullness after %.2f MB" % (bytes / (1024.0 * 1024.0)))
                        full = True
                        break

                try:
                    os.fsync(f)
                    print("fsync'ed successfully")
                except OSError as e:
                    print("Reached fullness after %.2f MB" % (bytes / (1024.0 * 1024.0)))
                    full = True
                    break
                else:
                    print("Not full yet after %.2f MB" % (bytes / (1024.0 * 1024.0)))

                if n > {fill_mb} * 0.9:
                    # Be cautious in the last region where we expect to hit
                    # the full condition, so that we don't overshoot too dramatically
                    print("sleeping a bit as we've exceeded 90% of our expected full ratio")
                    time.sleep({full_wait})

            if not full:
                raise RuntimeError("Failed to reach fullness after writing %d bytes" % bytes)

            # close() should not raise an error because we already caught it in
            # fsync.  There shouldn't have been any more writeback errors
            # since then because all IOs got cancelled on the full flag.
            print("calling close")
            os.close(f)
            print("close() did not raise error")

            os.unlink("{file_path}")
            """)

        self._remote_write_test(remote_script)


class TestQuotaFull(FullnessTestCase):
    """
    Test per-pool fullness, which indicates quota limits exceeded
    """
    pool_capacity = 1024 * 1024 * 32  # arbitrary low-ish limit
    fill_mb = pool_capacity // (1024 * 1024)  # type: ignore

    # We are only testing quota handling on the data pool, not the metadata
    # pool.
    data_only = True

    def setUp(self):
        super(TestQuotaFull, self).setUp()

        pool_name = self.fs.get_data_pool_name()
        self.get_ceph_cmd_stdout("osd", "pool", "set-quota", pool_name,
                                 "max_bytes", f"{self.pool_capacity}")


class TestClusterFull(FullnessTestCase):
    """
    Test data pool fullness, which indicates that an OSD has become too full
    """
    pool_capacity = None
    REQUIRE_MEMSTORE = True

    def setUp(self):
        super(TestClusterFull, self).setUp()

        if self.pool_capacity is None:
            TestClusterFull.pool_capacity = self.fs.get_pool_df(self._data_pool_name())['max_avail']
            TestClusterFull.fill_mb = (self.pool_capacity // (1024 * 1024))

# Hide the parent class so that unittest.loader doesn't try to run it.
del globals()['FullnessTestCase']
