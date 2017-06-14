
"""
Test our tools for recovering metadata from the data pool
"""

import logging
import os
from textwrap import dedent
import traceback
from collections import namedtuple

from teuthology.orchestra.run import CommandFailedError
from tasks.cephfs.cephfs_test_case import CephFSTestCase, long_running

log = logging.getLogger(__name__)


ValidationError = namedtuple("ValidationError", ["exception", "backtrace"])


class Workload(object):
    def __init__(self, filesystem, mount):
        self._mount = mount
        self._filesystem = filesystem
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
        objects = self._filesystem.rados(["ls"]).split("\n")
        for o in objects:
            self._filesystem.rados(["rm", o])

    def flush(self):
        """
        Called after client unmount, after write: flush whatever you want
        """
        self._filesystem.mds_asok(["flush", "journal"])


class SimpleWorkload(Workload):
    """
    Single file, single directory, check that it gets recovered and so does its size
    """
    def write(self):
        self._mount.run_shell(["mkdir", "subdir"])
        self._mount.write_n_mb("subdir/sixmegs", 6)
        self._initial_state = self._mount.stat("subdir/sixmegs")

    def validate(self):
        self._mount.run_shell(["ls", "subdir"])
        st = self._mount.stat("subdir/sixmegs")
        self.assert_equal(st['st_size'], self._initial_state['st_size'])
        return self._errors


class MovedFile(Workload):
    def write(self):
        # Create a file whose backtrace disagrees with his eventual position
        # in the metadata.  We will see that he gets reconstructed in his
        # original position according to his backtrace.
        self._mount.run_shell(["mkdir", "subdir_alpha"])
        self._mount.run_shell(["mkdir", "subdir_bravo"])
        self._mount.write_n_mb("subdir_alpha/sixmegs", 6)
        self._filesystem.mds_asok(["flush", "journal"])
        self._mount.run_shell(["mv", "subdir_alpha/sixmegs", "subdir_bravo/sixmegs"])
        self._initial_state = self._mount.stat("subdir_bravo/sixmegs")

    def flush(self):
        pass

    def validate(self):
        self.assert_equal(self._mount.ls(), ["subdir_alpha"])
        st = self._mount.stat("subdir_alpha/sixmegs")
        self.assert_equal(st['st_size'], self._initial_state['st_size'])
        return self._errors


class BacktracelessFile(Workload):
    def write(self):
        self._mount.run_shell(["mkdir", "subdir"])
        self._mount.write_n_mb("subdir/sixmegs", 6)
        self._initial_state = self._mount.stat("subdir/sixmegs")

    def flush(self):
        # Never flush metadata, so backtrace won't be written
        pass

    def validate(self):
        ino_name = "%x" % self._initial_state["st_ino"]

        # The inode should be linked into lost+found because we had no path for it
        self.assert_equal(self._mount.ls(), ["lost+found"])
        self.assert_equal(self._mount.ls("lost+found"), [ino_name])
        st = self._mount.stat("lost+found/{ino_name}".format(ino_name=ino_name))

        # We might not have got the name or path, but we should still get the size
        self.assert_equal(st['st_size'], self._initial_state['st_size'])

        return self._errors


class StripedStashedLayout(Workload):
    def __init__(self, fs, m):
        super(StripedStashedLayout, self).__init__(fs, m)

        # Nice small stripes so we can quickly do our writes+validates
        self.sc = 4
        self.ss = 65536
        self.os = 262144

        self.interesting_sizes = [
            # Exactly stripe_count objects will exist
            self.os * self.sc,
            # Fewer than stripe_count objects will exist
            self.os * self.sc / 2,
            self.os * (self.sc - 1) + self.os / 2,
            self.os * (self.sc - 1) + self.os / 2 - 1,
            self.os * (self.sc + 1) + self.os / 2,
            self.os * (self.sc + 1) + self.os / 2 + 1,
            # More than stripe_count objects will exist
            self.os * self.sc + self.os * self.sc / 2
        ]

    def write(self):
        # Create a dir with a striped layout set on it
        self._mount.run_shell(["mkdir", "stripey"])

        self._mount.setfattr("./stripey", "ceph.dir.layout",
             "stripe_unit={ss} stripe_count={sc} object_size={os} pool={pool}".format(
                 ss=self.ss, os=self.os, sc=self.sc,
                 pool=self._filesystem.get_data_pool_name()
             ))

        # Write files, then flush metadata so that its layout gets written into an xattr
        for i, n_bytes in enumerate(self.interesting_sizes):
            self._mount.write_test_pattern("stripey/flushed_file_{0}".format(i), n_bytes)
            # This is really just validating the validator
            self._mount.validate_test_pattern("stripey/flushed_file_{0}".format(i), n_bytes)
        self._filesystem.mds_asok(["flush", "journal"])

        # Write another file in the same way, but this time don't flush the metadata,
        # so that it won't have the layout xattr
        self._mount.write_test_pattern("stripey/unflushed_file", 1024 * 512)
        self._mount.validate_test_pattern("stripey/unflushed_file", 1024 * 512)

        self._initial_state = {
            "unflushed_ino": self._mount.path_to_ino("stripey/unflushed_file")
        }

    def flush(self):
        # Pass because we already selectively flushed during write
        pass

    def validate(self):
        # The first files should have been recovered into its original location
        # with the correct layout: read back correct data
        for i, n_bytes in enumerate(self.interesting_sizes):
            try:
                self._mount.validate_test_pattern("stripey/flushed_file_{0}".format(i), n_bytes)
            except CommandFailedError as e:
                self._errors.append(
                    ValidationError("File {0} (size {1}): {2}".format(i, n_bytes, e), traceback.format_exc(3))
                )

        # The unflushed file should have been recovered into lost+found without
        # the correct layout: read back junk
        ino_name = "%x" % self._initial_state["unflushed_ino"]
        self.assert_equal(self._mount.ls("lost+found"), [ino_name])
        try:
            self._mount.validate_test_pattern(os.path.join("lost+found", ino_name), 1024 * 512)
        except CommandFailedError:
            pass
        else:
            self._errors.append(
                ValidationError("Unexpectedly valid data in unflushed striped file", "")
            )

        return self._errors


class ManyFilesWorkload(Workload):
    def __init__(self, filesystem, mount, file_count):
        super(ManyFilesWorkload, self).__init__(filesystem, mount)
        self.file_count = file_count

    def write(self):
        self._mount.run_shell(["mkdir", "subdir"])
        for n in range(0, self.file_count):
            self._mount.write_test_pattern("subdir/{0}".format(n), 6 * 1024 * 1024)

    def validate(self):
        for n in range(0, self.file_count):
            try:
                self._mount.validate_test_pattern("subdir/{0}".format(n), 6 * 1024 * 1024)
            except CommandFailedError as e:
                self._errors.append(
                    ValidationError("File {0}: {1}".format(n, e), traceback.format_exc(3))
                )

        return self._errors


class MovedDir(Workload):
    def write(self):
        # Create a nested dir that we will then move.  Two files with two different
        # backtraces referring to the moved dir, claiming two different locations for
        # it.  We will see that only one backtrace wins and the dir ends up with
        # single linkage.
        self._mount.run_shell(["mkdir", "-p", "grandmother/parent"])
        self._mount.write_n_mb("grandmother/parent/orig_pos_file", 1)
        self._filesystem.mds_asok(["flush", "journal"])
        self._mount.run_shell(["mkdir", "grandfather"])
        self._mount.run_shell(["mv", "grandmother/parent", "grandfather"])
        self._mount.write_n_mb("grandfather/parent/new_pos_file", 2)
        self._filesystem.mds_asok(["flush", "journal"])

        self._initial_state = (
            self._mount.stat("grandfather/parent/orig_pos_file"),
            self._mount.stat("grandfather/parent/new_pos_file")
        )

    def validate(self):
        root_files = self._mount.ls()
        self.assert_equal(len(root_files), 1)
        self.assert_equal(root_files[0] in ["grandfather", "grandmother"], True)
        winner = root_files[0]
        st_opf = self._mount.stat("{0}/parent/orig_pos_file".format(winner))
        st_npf = self._mount.stat("{0}/parent/new_pos_file".format(winner))

        self.assert_equal(st_opf['st_size'], self._initial_state[0]['st_size'])
        self.assert_equal(st_npf['st_size'], self._initial_state[1]['st_size'])


class MissingZerothObject(Workload):
    def write(self):
        self._mount.run_shell(["mkdir", "subdir"])
        self._mount.write_n_mb("subdir/sixmegs", 6)
        self._initial_state = self._mount.stat("subdir/sixmegs")

    def damage(self):
        super(MissingZerothObject, self).damage()
        zeroth_id = "{0:x}.00000000".format(self._initial_state['st_ino'])
        self._filesystem.rados(["rm", zeroth_id], pool=self._filesystem.get_data_pool_name())

    def validate(self):
        st = self._mount.stat("lost+found/{0:x}".format(self._initial_state['st_ino']))
        self.assert_equal(st['st_size'], self._initial_state['st_size'])


class NonDefaultLayout(Workload):
    """
    Check that the reconstruction copes with files that have a different
    object size in their layout
    """
    def write(self):
        self._mount.run_shell(["touch", "datafile"])
        self._mount.setfattr("./datafile", "ceph.file.layout.object_size", "8388608")
        self._mount.run_shell(["dd", "if=/dev/urandom", "of=./datafile", "bs=1M", "count=32"])
        self._initial_state = self._mount.stat("datafile")

    def validate(self):
        # Check we got the layout reconstructed properly
        object_size = int(self._mount.getfattr(
            "./datafile", "ceph.file.layout.object_size"))
        self.assert_equal(object_size, 8388608)

        # Check we got the file size reconstructed properly
        st = self._mount.stat("datafile")
        self.assert_equal(st['st_size'], self._initial_state['st_size'])


class TestDataScan(CephFSTestCase):
    MDSS_REQUIRED = 2

    def is_marked_damaged(self, rank):
        mds_map = self.fs.get_mds_map()
        return rank in mds_map['damaged']

    def _rebuild_metadata(self, workload, workers=1):
        """
        That when all objects in metadata pool are removed, we can rebuild a metadata pool
        based on the contents of a data pool, and a client can see and read our files.
        """

        # First, inject some files
        workload.write()

        # Unmount the client and flush the journal: the tool should also cope with
        # situations where there is dirty metadata, but we'll test that separately
        self.mount_a.umount_wait()
        workload.flush()

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

        # Attempt to start an MDS, see that it goes into damaged state
        self.fs.mds_restart()

        def get_state(mds_id):
            info = self.mds_cluster.get_mds_info(mds_id)
            return info['state'] if info is not None else None

        self.wait_until_true(lambda: self.is_marked_damaged(0), 60)
        for mds_id in self.fs.mds_ids:
            self.wait_until_equal(
                    lambda: get_state(mds_id),
                    "up:standby",
                    timeout=60)

        # Run the recovery procedure
        self.fs.table_tool(["0", "reset", "session"])
        self.fs.table_tool(["0", "reset", "snap"])
        self.fs.table_tool(["0", "reset", "inode"])
        if False:
            with self.assertRaises(CommandFailedError):
                # Normal reset should fail when no objects are present, we'll use --force instead
                self.fs.journal_tool(["journal", "reset"])
        self.fs.journal_tool(["journal", "reset", "--force"])
        self.fs.data_scan(["init"])
        self.fs.data_scan(["scan_extents", self.fs.get_data_pool_name()], worker_count=workers)
        self.fs.data_scan(["scan_inodes", self.fs.get_data_pool_name()], worker_count=workers)

        # Mark the MDS repaired
        self.fs.mon_manager.raw_cluster_cmd('mds', 'repaired', '0')

        # Start the MDS
        self.fs.mds_restart()
        self.fs.wait_for_daemons()
        import json
        log.info(json.dumps(self.mds_cluster.get_fs_map(), indent=2))

        # Mount a client
        self.mount_a.mount()
        self.mount_a.wait_until_mounted()

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
        self._rebuild_metadata(SimpleWorkload(self.fs, self.mount_a))

    def test_rebuild_moved_file(self):
        self._rebuild_metadata(MovedFile(self.fs, self.mount_a))

    def test_rebuild_backtraceless(self):
        self._rebuild_metadata(BacktracelessFile(self.fs, self.mount_a))

    def test_rebuild_moved_dir(self):
        self._rebuild_metadata(MovedDir(self.fs, self.mount_a))

    def test_rebuild_missing_zeroth(self):
        self._rebuild_metadata(MissingZerothObject(self.fs, self.mount_a))

    def test_rebuild_nondefault_layout(self):
        self._rebuild_metadata(NonDefaultLayout(self.fs, self.mount_a))

    def test_stashed_layout(self):
        self._rebuild_metadata(StripedStashedLayout(self.fs, self.mount_a))

    def _dirfrag_keys(self, object_id):
        keys_str = self.fs.rados(["listomapkeys", object_id])
        if keys_str:
            return keys_str.split("\n")
        else:
            return []

    def test_fragmented_injection(self):
        """
        That when injecting a dentry into a fragmented directory, we put it in the right fragment.
        """

        self.fs.mon_manager.raw_cluster_cmd("mds", "set", "allow_dirfrags", "true",
                                            "--yes-i-really-mean-it")

        file_count = 100
        file_names = ["%s" % n for n in range(0, file_count)]

        # Create a directory of `file_count` files, each named after its
        # decimal number and containing the string of its decimal number
        self.mount_a.run_python(dedent("""
        import os
        path = os.path.join("{path}", "subdir")
        os.mkdir(path)
        for n in range(0, {file_count}):
            open(os.path.join(path, "%s" % n), 'w').write("%s" % n)
        """.format(
            path=self.mount_a.mountpoint,
            file_count=file_count
        )))

        dir_ino = self.mount_a.path_to_ino("subdir")

        # Only one MDS should be active!
        self.assertEqual(len(self.fs.get_active_names()), 1)

        # Ensure that one directory is fragmented
        mds_id = self.fs.get_active_names()[0]
        self.fs.mds_asok(["dirfrag", "split", "/subdir", "0/0", "1"], mds_id)

        # Flush journal and stop MDS
        self.mount_a.umount_wait()
        self.fs.mds_asok(["flush", "journal"], mds_id)
        self.fs.mds_stop()
        self.fs.mds_fail()

        # Pick a dentry and wipe out its key
        # Because I did a 1 bit split, I know one frag will be named <inode>.01000000
        frag_obj_id = "{0:x}.01000000".format(dir_ino)
        keys = self._dirfrag_keys(frag_obj_id)
        victim_key = keys[7]  # arbitrary choice
        log.info("victim_key={0}".format(victim_key))
        victim_dentry = victim_key.split("_head")[0]
        self.fs.rados(["rmomapkey", frag_obj_id, victim_key])

        # Start filesystem back up, observe that the file appears to be gone in an `ls`
        self.fs.mds_restart()
        self.fs.wait_for_daemons()
        self.mount_a.mount()
        self.mount_a.wait_until_mounted()
        files = self.mount_a.run_shell(["ls", "subdir/"]).stdout.getvalue().strip().split("\n")
        self.assertListEqual(sorted(files), sorted(list(set(file_names) - set([victim_dentry]))))

        # Stop the filesystem
        self.mount_a.umount_wait()
        self.fs.mds_stop()
        self.fs.mds_fail()

        # Run data-scan, observe that it inserts our dentry back into the correct fragment
        # by checking the omap now has the dentry's key again
        self.fs.data_scan(["scan_extents", self.fs.get_data_pool_name()])
        self.fs.data_scan(["scan_inodes", self.fs.get_data_pool_name()])
        self.assertIn(victim_key, self._dirfrag_keys(frag_obj_id))

        # Start the filesystem and check that the dentry we deleted is now once again visible
        # and points to the correct file data.
        self.fs.mds_restart()
        self.fs.wait_for_daemons()
        self.mount_a.mount()
        self.mount_a.wait_until_mounted()
        out = self.mount_a.run_shell(["cat", "subdir/{0}".format(victim_dentry)]).stdout.getvalue().strip()
        self.assertEqual(out, victim_dentry)

        # Finally, close the loop by checking our injected dentry survives a merge
        mds_id = self.fs.get_active_names()[0]
        self.mount_a.ls("subdir")  # Do an ls to ensure both frags are in cache so the merge will work
        self.fs.mds_asok(["dirfrag", "merge", "/subdir", "0/0"], mds_id)
        self.fs.mds_asok(["flush", "journal"], mds_id)
        frag_obj_id = "{0:x}.00000000".format(dir_ino)
        keys = self._dirfrag_keys(frag_obj_id)
        self.assertListEqual(sorted(keys), sorted(["%s_head" % f for f in file_names]))

    @long_running
    def test_parallel_execution(self):
        self._rebuild_metadata(ManyFilesWorkload(self.fs, self.mount_a, 25), workers=7)
