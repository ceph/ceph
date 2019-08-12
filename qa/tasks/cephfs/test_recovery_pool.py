
"""
Test our tools for recovering metadata from the data pool into an alternate pool
"""
import json

import logging
import os
from textwrap import dedent
import traceback
from collections import namedtuple, defaultdict

from teuthology.orchestra.run import CommandFailedError
from tasks.cephfs.cephfs_test_case import CephFSTestCase, for_teuthology

log = logging.getLogger(__name__)


ValidationError = namedtuple("ValidationError", ["exception", "backtrace"])


class OverlayWorkload(object):
    def __init__(self, orig_fs, recovery_fs, orig_mount, recovery_mount):
        self._orig_fs = orig_fs
        self._recovery_fs = recovery_fs
        self._orig_mount = orig_mount
        self._recovery_mount = recovery_mount
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

    def damage(self):
        """
        Damage the filesystem pools in ways that will be interesting to recover from.  By
        default just wipe everything in the metadata pool
        """
        # Delete every object in the metadata pool
        objects = self._orig_fs.rados(["ls"]).split("\n")
        for o in objects:
            self._orig_fs.rados(["rm", o])

    def flush(self):
        """
        Called after client unmount, after write: flush whatever you want
        """
        self._orig_fs.mds_asok(["flush", "journal"])
        self._recovery_fs.mds_asok(["flush", "journal"])


class SimpleOverlayWorkload(OverlayWorkload):
    """
    Single file, single directory, check that it gets recovered and so does its size
    """
    def write(self):
        self._orig_mount.run_shell(["mkdir", "subdir"])
        self._orig_mount.write_n_mb("subdir/sixmegs", 6)
        self._initial_state = self._orig_mount.stat("subdir/sixmegs")

    def validate(self):
        self._recovery_mount.run_shell(["ls", "subdir"])
        st = self._recovery_mount.stat("subdir/sixmegs")
        self.assert_equal(st['st_size'], self._initial_state['st_size'])
        return self._errors

class TestRecoveryPool(CephFSTestCase):
    MDSS_REQUIRED = 2
    CLIENTS_REQUIRED = 2
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

        workload.write()

        # Unmount the client and flush the journal: the tool should also cope with
        # situations where there is dirty metadata, but we'll test that separately
        self.mount_a.umount_wait()
        self.mount_b.umount_wait()
        workload.flush()

        # Create the alternate pool if requested
        recovery_fs = self.recovery_fs.name
        recovery_pool = self.recovery_fs.get_metadata_pool_name()
        self.recovery_fs.data_scan(['init', '--force-init',
                                    '--filesystem', recovery_fs,
                                    '--alternate-pool', recovery_pool])
        self.recovery_fs.mon_manager.raw_cluster_cmd('-s')
        self.recovery_fs.table_tool([recovery_fs + ":0", "reset", "session"])
        self.recovery_fs.table_tool([recovery_fs + ":0", "reset", "snap"])
        self.recovery_fs.table_tool([recovery_fs + ":0", "reset", "inode"])

        # Stop the MDS
        self.fs.mds_stop()
        self.fs.mds_fail()

        # After recovery, we need the MDS to not be strict about stats (in production these options
        # are off by default, but in QA we need to explicitly disable them)
        self.fs.set_ceph_conf('mds', 'mds verify scatter', False)
        self.fs.set_ceph_conf('mds', 'mds debug scatterstat', False)

        # Apply any data damage the workload wants
        workload.damage()

        # Reset the MDS map in case multiple ranks were in play: recovery procedure
        # only understands how to rebuild metadata under rank 0
        self.fs.mon_manager.raw_cluster_cmd('fs', 'reset', self.fs.name,
                '--yes-i-really-mean-it')

        self.fs.table_tool([self.fs.name + ":0", "reset", "session"])
        self.fs.table_tool([self.fs.name + ":0", "reset", "snap"])
        self.fs.table_tool([self.fs.name + ":0", "reset", "inode"])

        # Run the recovery procedure
        if False:
            with self.assertRaises(CommandFailedError):
                # Normal reset should fail when no objects are present, we'll use --force instead
                self.fs.journal_tool(["journal", "reset"], 0)

        self.fs.mds_stop()
        self.fs.data_scan(['scan_extents', '--alternate-pool',
                           recovery_pool, '--filesystem', self.fs.name,
                           self.fs.get_data_pool_name()])
        self.fs.data_scan(['scan_inodes', '--alternate-pool',
                           recovery_pool, '--filesystem', self.fs.name,
                           '--force-corrupt', '--force-init',
                           self.fs.get_data_pool_name()])
        self.fs.journal_tool(['event', 'recover_dentries', 'list',
                              '--alternate-pool', recovery_pool], 0)

        self.fs.data_scan(['init', '--force-init', '--filesystem',
                           self.fs.name])
        self.fs.data_scan(['scan_inodes', '--filesystem', self.fs.name,
                           '--force-corrupt', '--force-init',
                           self.fs.get_data_pool_name()])
        self.fs.journal_tool(['event', 'recover_dentries', 'list'], 0)

        self.recovery_fs.journal_tool(['journal', 'reset', '--force'], 0)
        self.fs.journal_tool(['journal', 'reset', '--force'], 0)
        self.fs.mon_manager.raw_cluster_cmd('mds', 'repaired',
                                            recovery_fs + ":0")

        # Mark the MDS repaired
        self.fs.mon_manager.raw_cluster_cmd('mds', 'repaired', '0')

        # Start the MDS
        self.fs.mds_restart()
        self.recovery_fs.mds_restart()
        self.fs.wait_for_daemons()
        self.recovery_fs.wait_for_daemons()
        status = self.recovery_fs.status()
        for rank in self.recovery_fs.get_ranks(status=status):
            self.fs.mon_manager.raw_cluster_cmd('tell', "mds." + rank['name'],
                                                'injectargs', '--debug-mds=20')
            self.fs.rank_tell(['scrub', 'start', '/', 'recursive', 'repair'], rank=rank['rank'], status=status)
        log.info(str(self.mds_cluster.status()))

        # Mount a client
        self.mount_a.mount()
        self.mount_b.mount(mount_fs_name=recovery_fs)
        self.mount_a.wait_until_mounted()
        self.mount_b.wait_until_mounted()

        # See that the files are present and correct
        errors = workload.validate()
        if errors:
            log.error("Validation errors found: {0}".format(len(errors)))
            for e in errors:
                log.error(e.exception)
                log.error(e.backtrace)
            raise AssertionError("Validation failed, first error: {0}\n{1}".format(
                errors[0].exception, errors[0].backtrace
            ))

    def test_rebuild_simple(self):
        self._rebuild_metadata(SimpleOverlayWorkload(self.fs, self.recovery_fs,
                                                     self.mount_a, self.mount_b))
