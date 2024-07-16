"""
Test our tools for recovering metadata from the data pool into an alternate pool
"""

import logging
import traceback
from collections import namedtuple

from teuthology.exceptions import CommandFailedError
from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)


ValidationError = namedtuple("ValidationError", ["exception", "backtrace"])


class OverlayWorkload(object):
    def __init__(self):
        self._initial_state = None

        # Accumulate backtraces for every failed validation, and return them.  Backtraces
        # are rather verbose, but we only see them when something breaks, and they
        # let us see which check failed without having to decorate each check with
        # a string
        self._errors = []

    def assert_equal(self, a, b):
        try:
            if a != b:
                raise AssertionError("{0} != {1}".format(a, b))
        except AssertionError as e:
            self._errors.append(
                ValidationError(e, traceback.format_exc(3))
            )

    def write(self):
        """
        Write the workload files to the mount
        """
        raise NotImplementedError()

    def validate(self):
        """
        Read from the mount and validate that the workload files are present (i.e. have
        survived or been reconstructed from the test scenario)
        """
        raise NotImplementedError()

    def damage(self, fs):
        """
        Damage the filesystem pools in ways that will be interesting to recover from.  By
        default just wipe everything in the metadata pool
        """

        pool = fs.get_metadata_pool_name()
        fs.rados(["purge", pool, '--yes-i-really-really-mean-it'])

    def flush(self, fs):
        """
        Called after client unmount, after write: flush whatever you want
        """
        fs.rank_asok(["flush", "journal"])


class SimpleOverlayWorkload(OverlayWorkload):
    """
    Single file, single directory, check that it gets recovered and so does its size
    """
    def write(self, mount):
        mount.run_shell(["mkdir", "subdir"])
        mount.write_n_mb("subdir/sixmegs", 6)
        self._initial_state = mount.stat("subdir/sixmegs")

    def validate(self, recovery_mount):
        recovery_mount.run_shell(["ls", "subdir"])
        st = recovery_mount.stat("subdir/sixmegs")
        self.assert_equal(st['st_size'], self._initial_state['st_size'])
        return self._errors

class TestRecoveryPool(CephFSTestCase):
    MDSS_REQUIRED = 2
    CLIENTS_REQUIRED = 1
    REQUIRE_RECOVERY_FILESYSTEM = True

    def is_marked_damaged(self, rank):
        mds_map = self.fs.get_mds_map()
        return rank in mds_map['damaged']

    def _rebuild_metadata(self, workload, other_pool=None, workers=1):
        """
        That when all objects in metadata pool are removed, we can rebuild a metadata pool
        based on the contents of a data pool, and a client can see and read our files.
        """

        # First, inject some files

        workload.write(self.mount_a)

        # Unmount the client and flush the journal: the tool should also cope with
        # situations where there is dirty metadata, but we'll test that separately
        self.mount_a.umount_wait()
        workload.flush(self.fs)
        self.fs.fail()

        # After recovery, we need the MDS to not be strict about stats (in production these options
        # are off by default, but in QA we need to explicitly disable them)
        # Note: these have to be written to ceph.conf to override existing ceph.conf values.
        self.fs.set_ceph_conf('mds', 'mds verify scatter', False)
        self.fs.set_ceph_conf('mds', 'mds debug scatterstat', False)
        self.fs.mds_restart()

        # Apply any data damage the workload wants
        workload.damage(self.fs)

        # Create the alternate pool if requested
        recovery_fs = self.mds_cluster.newfs(name="recovery_fs", create=False)
        recovery_fs.set_data_pool_name(self.fs.get_data_pool_name())
        recovery_fs.create(recover=True, metadata_overlay=True)

        recovery_pool = recovery_fs.get_metadata_pool_name()
        self.run_ceph_cmd('-s')

        # Reset the MDS map in case multiple ranks were in play: recovery procedure
        # only understands how to rebuild metadata under rank 0
        #self.fs.reset()
        #self.fs.table_tool([self.fs.name + ":0", "reset", "session"])
        #self.fs.table_tool([self.fs.name + ":0", "reset", "snap"])
        #self.fs.table_tool([self.fs.name + ":0", "reset", "inode"])

        # Run the recovery procedure
        recovery_fs.data_scan(['init', '--force-init',
                               '--filesystem', recovery_fs.name,
                               '--alternate-pool', recovery_pool])
        recovery_fs.table_tool([recovery_fs.name + ":0", "reset", "session"])
        recovery_fs.table_tool([recovery_fs.name + ":0", "reset", "snap"])
        recovery_fs.table_tool([recovery_fs.name + ":0", "reset", "inode"])
        if False:
            with self.assertRaises(CommandFailedError):
                # Normal reset should fail when no objects are present, we'll use --force instead
                self.fs.journal_tool(["journal", "reset", "--yes-i-really-really-mean-it"], 0)

        recovery_fs.data_scan(['scan_extents', '--alternate-pool',
                           recovery_pool, '--filesystem', self.fs.name,
                           self.fs.get_data_pool_name()])
        recovery_fs.data_scan(['scan_inodes', '--alternate-pool',
                           recovery_pool, '--filesystem', self.fs.name,
                           '--force-corrupt', '--force-init',
                           self.fs.get_data_pool_name()])
        recovery_fs.data_scan(['scan_links', '--filesystem', recovery_fs.name])
        recovery_fs.journal_tool(['event', 'recover_dentries', 'list',
                              '--alternate-pool', recovery_pool], 0)
        recovery_fs.journal_tool(["journal", "reset", "--force", "--yes-i-really-really-mean-it"], 0)

        # Start the MDS
        recovery_fs.set_joinable()
        status = recovery_fs.wait_for_daemons()

        self.config_set('mds', 'debug_mds', '20')
        for rank in recovery_fs.get_ranks(status=status):
            recovery_fs.rank_tell(['scrub', 'start', '/', 'force,recursive,repair'], rank=rank['rank'], status=status)
        log.info(str(recovery_fs.status()))

        # Mount a client
        self.mount_a.mount_wait(cephfs_name=recovery_fs.name)

        # See that the files are present and correct
        errors = workload.validate(self.mount_a)
        if errors:
            log.error("Validation errors found: {0}".format(len(errors)))
            for e in errors:
                log.error(e.exception)
                log.error(e.backtrace)
            raise AssertionError("Validation failed, first error: {0}\n{1}".format(
                errors[0].exception, errors[0].backtrace
            ))

    def test_rebuild_simple(self):
        self._rebuild_metadata(SimpleOverlayWorkload())
