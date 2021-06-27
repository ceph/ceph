"""
Test CephFS scrub (distinct from OSD scrub) functionality
"""

from io import BytesIO
import logging
from collections import namedtuple

from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)

ValidationError = namedtuple("ValidationError", ["exception", "backtrace"])


class Workload(CephFSTestCase):
    def __init__(self, test, filesystem, mount):
        super().__init__()
        self._test =  test
        self._mount = mount
        self._filesystem = filesystem
        self._initial_state = None

        # Accumulate backtraces for every failed validation, and return them.  Backtraces
        # are rather verbose, but we only see them when something breaks, and they
        # let us see which check failed without having to decorate each check with
        # a string
        self._errors = []

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
        pool = self._filesystem.get_metadata_pool_name()
        self._filesystem.rados(["purge", pool, '--yes-i-really-really-mean-it'])

    def flush(self):
        """
        Called after client unmount, after write: flush whatever you want
        """
        self._filesystem.mds_asok(["flush", "journal"])


class BacktraceWorkload(Workload):
    """
    Single file, single directory, wipe the backtrace and check it.
    """
    def write(self):
        self._mount.run_shell(["mkdir", "subdir"])
        self._mount.write_n_mb("subdir/sixmegs", 6)

    def validate(self):
        st = self._mount.stat("subdir/sixmegs")
        self._filesystem.mds_asok(["flush", "journal"])
        bt = self._filesystem.read_backtrace(st['st_ino'])
        parent = bt['ancestors'][0]['dname']
        self.assertEqual(parent, 'sixmegs')
        return self._errors

    def damage(self):
        st = self._mount.stat("subdir/sixmegs")
        self._filesystem.mds_asok(["flush", "journal"])
        self._filesystem._write_data_xattr(st['st_ino'], "parent", "")

    def create_files(self, nfiles=1000):
        self._mount.create_n_files("scrub-new-files/file", nfiles)


class DupInodeWorkload(Workload):
    """
    Duplicate an inode and try scrubbing it twice."
    """

    def write(self):
        self._mount.run_shell(["mkdir", "parent"])
        self._mount.run_shell(["mkdir", "parent/child"])
        self._mount.write_n_mb("parent/parentfile", 6)
        self._mount.write_n_mb("parent/child/childfile", 6)

    def damage(self):
        self._mount.umount_wait()
        self._filesystem.mds_asok(["flush", "journal"])
        self._filesystem.fail()
        d = self._filesystem.radosmo(["getomapval", "10000000000.00000000", "parentfile_head", "-"])
        self._filesystem.radosm(["setomapval", "10000000000.00000000", "shadow_head"], stdin=BytesIO(d))
        self._test.config_set('mds', 'mds_hack_allow_loading_invalid_metadata', True)
        self._filesystem.set_joinable()
        self._filesystem.wait_for_daemons()

    def validate(self):
        out_json = self._filesystem.run_scrub(["start", "/", "recursive,repair"])
        self.assertNotEqual(out_json, None)
        self.assertEqual(out_json["return_code"], 0)
        self.assertEqual(self._filesystem.wait_until_scrub_complete(tag=out_json["scrub_tag"]), True)
        self.assertTrue(self._filesystem.are_daemons_healthy())
        return self._errors


class TestScrub(CephFSTestCase):
    MDSS_REQUIRED = 1

    def setUp(self):
        super().setUp()

    def _scrub(self, workload, workers=1):
        """
        That when all objects in metadata pool are removed, we can rebuild a metadata pool
        based on the contents of a data pool, and a client can see and read our files.
        """

        # First, inject some files

        workload.write()

        # are off by default, but in QA we need to explicitly disable them)
        self.fs.set_ceph_conf('mds', 'mds verify scatter', False)
        self.fs.set_ceph_conf('mds', 'mds debug scatterstat', False)

        # Apply any data damage the workload wants
        workload.damage()

        out_json = self.fs.run_scrub(["start", "/", "recursive,repair"])
        self.assertNotEqual(out_json, None)
        self.assertEqual(out_json["return_code"], 0)
        self.assertEqual(self.fs.wait_until_scrub_complete(tag=out_json["scrub_tag"]), True)

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

    def _get_damage_count(self, damage_type='backtrace'):
        out_json = self.fs.rank_tell(["damage", "ls"])
        self.assertNotEqual(out_json, None)

        damage_count = 0
        for it in out_json:
            if it['damage_type'] == damage_type:
                damage_count += 1
        return damage_count

    def _scrub_new_files(self, workload):
        """
        That scrubbing new files does not lead to errors
        """
        workload.create_files(1000)
        self.fs.wait_until_scrub_complete()
        self.assertEqual(self._get_damage_count(), 0)

    def test_scrub_backtrace_for_new_files(self):
        self._scrub_new_files(BacktraceWorkload(self, self.fs, self.mount_a))

    def test_scrub_backtrace(self):
        self._scrub(BacktraceWorkload(self, self.fs, self.mount_a))

    def test_scrub_dup_inode(self):
        self._scrub(DupInodeWorkload(self, self.fs, self.mount_a))
