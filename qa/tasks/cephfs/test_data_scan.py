
"""
Test our tools for recovering metadata from the data pool
"""
import json

import logging
import os
import time
from textwrap import dedent
import traceback
from collections import namedtuple, defaultdict

from teuthology.orchestra.run import CommandFailedError
from tasks.cephfs.cephfs_test_case import CephFSTestCase, for_teuthology

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

        self.fs.table_tool([self.fs.name + ":0", "reset", "session"])
        self.fs.table_tool([self.fs.name + ":0", "reset", "snap"])
        self.fs.table_tool([self.fs.name + ":0", "reset", "inode"])

        # Run the recovery procedure
        if False:
            with self.assertRaises(CommandFailedError):
                # Normal reset should fail when no objects are present, we'll use --force instead
                self.fs.journal_tool(["journal", "reset"], 0)

        self.fs.journal_tool(["journal", "reset", "--force"], 0)
        self.fs.data_scan(["init"])
        self.fs.data_scan(["scan_extents", self.fs.get_data_pool_name()], worker_count=workers)
        self.fs.data_scan(["scan_inodes", self.fs.get_data_pool_name()], worker_count=workers)

        # Mark the MDS repaired
        self.fs.mon_manager.raw_cluster_cmd('mds', 'repaired', '0')

        # Start the MDS
        self.fs.mds_restart()
        self.fs.wait_for_daemons()
        log.info(str(self.mds_cluster.status()))

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

    @for_teuthology
    def test_parallel_execution(self):
        self._rebuild_metadata(ManyFilesWorkload(self.fs, self.mount_a, 25), workers=7)

    def test_pg_files(self):
        """
        That the pg files command tells us which files are associated with
        a particular PG
        """
        file_count = 20
        self.mount_a.run_shell(["mkdir", "mydir"])
        self.mount_a.create_n_files("mydir/myfile", file_count)

        # Some files elsewhere in the system that we will ignore
        # to check that the tool is filtering properly
        self.mount_a.run_shell(["mkdir", "otherdir"])
        self.mount_a.create_n_files("otherdir/otherfile", file_count)

        pgs_to_files = defaultdict(list)
        # Rough (slow) reimplementation of the logic
        for i in range(0, file_count):
            file_path = "mydir/myfile_{0}".format(i)
            ino = self.mount_a.path_to_ino(file_path)
            obj = "{0:x}.{1:08x}".format(ino, 0)
            pgid = json.loads(self.fs.mon_manager.raw_cluster_cmd(
                "osd", "map", self.fs.get_data_pool_name(), obj,
                "--format=json-pretty"
            ))['pgid']
            pgs_to_files[pgid].append(file_path)
            log.info("{0}: {1}".format(file_path, pgid))

        pg_count = self.fs.get_pgs_per_fs_pool()
        for pg_n in range(0, pg_count):
            pg_str = "{0}.{1}".format(self.fs.get_data_pool_id(), pg_n)
            out = self.fs.data_scan(["pg_files", "mydir", pg_str])
            lines = [l for l in out.split("\n") if l]
            log.info("{0}: {1}".format(pg_str, lines))
            self.assertSetEqual(set(lines), set(pgs_to_files[pg_str]))

    def test_rebuild_linkage(self):
        """
        The scan_links command fixes linkage errors
        """
        self.mount_a.run_shell(["mkdir", "testdir1"])
        self.mount_a.run_shell(["mkdir", "testdir2"])
        dir1_ino = self.mount_a.path_to_ino("testdir1")
        dir2_ino = self.mount_a.path_to_ino("testdir2")
        dirfrag1_oid = "{0:x}.00000000".format(dir1_ino)
        dirfrag2_oid = "{0:x}.00000000".format(dir2_ino)

        self.mount_a.run_shell(["touch", "testdir1/file1"])
        self.mount_a.run_shell(["ln", "testdir1/file1", "testdir1/link1"])
        self.mount_a.run_shell(["ln", "testdir1/file1", "testdir2/link2"])

        mds_id = self.fs.get_active_names()[0]
        self.fs.mds_asok(["flush", "journal"], mds_id)

        dirfrag1_keys = self._dirfrag_keys(dirfrag1_oid)

        # introduce duplicated primary link
        file1_key = "file1_head"
        self.assertIn(file1_key, dirfrag1_keys)
        file1_omap_data = self.fs.rados(["getomapval", dirfrag1_oid, file1_key, '-'])
        self.fs.rados(["setomapval", dirfrag2_oid, file1_key], stdin_data=file1_omap_data)
        self.assertIn(file1_key, self._dirfrag_keys(dirfrag2_oid))

        # remove a remote link, make inode link count incorrect
        link1_key = 'link1_head'
        self.assertIn(link1_key, dirfrag1_keys)
        self.fs.rados(["rmomapkey", dirfrag1_oid, link1_key])

        # increase good primary link's version
        self.mount_a.run_shell(["touch", "testdir1/file1"])
        self.mount_a.umount_wait()

        self.fs.mds_asok(["flush", "journal"], mds_id)
        self.fs.mds_stop()
        self.fs.mds_fail()

        # repair linkage errors
        self.fs.data_scan(["scan_links"])

        # primary link in testdir2 was deleted?
        self.assertNotIn(file1_key, self._dirfrag_keys(dirfrag2_oid))

        self.fs.mds_restart()
        self.fs.wait_for_daemons()

        self.mount_a.mount()
        self.mount_a.wait_until_mounted()

        # link count was adjusted?
        file1_nlink = self.mount_a.path_to_nlink("testdir1/file1")
        self.assertEqual(file1_nlink, 2)

    def test_rebuild_inotable(self):
        """
        The scan_links command repair inotables
        """
        self.fs.set_max_mds(2)
        self.fs.wait_for_daemons()

        active_mds_names = self.fs.get_active_names()
        mds0_id = active_mds_names[0]
        mds1_id = active_mds_names[1]

        self.mount_a.run_shell(["mkdir", "dir1"])
        dir_ino = self.mount_a.path_to_ino("dir1")
        self.mount_a.setfattr("dir1", "ceph.dir.pin", "1")
        # wait for subtree migration

        file_ino = 0;
        while True:
            time.sleep(1)
            # allocate an inode from mds.1
            self.mount_a.run_shell(["touch", "dir1/file1"])
            file_ino = self.mount_a.path_to_ino("dir1/file1")
            if file_ino >= (2 << 40):
                break
            self.mount_a.run_shell(["rm", "-f", "dir1/file1"])

        self.mount_a.umount_wait()

        self.fs.mds_asok(["flush", "journal"], mds0_id)
        self.fs.mds_asok(["flush", "journal"], mds1_id)
        self.mds_cluster.mds_stop()

        self.fs.rados(["rm", "mds0_inotable"])
        self.fs.rados(["rm", "mds1_inotable"])

        self.fs.data_scan(["scan_links", "--filesystem", self.fs.name])

        mds0_inotable = json.loads(self.fs.table_tool([self.fs.name + ":0", "show", "inode"]))
        self.assertGreaterEqual(
            mds0_inotable['0']['data']['inotable']['free'][0]['start'], dir_ino)

        mds1_inotable = json.loads(self.fs.table_tool([self.fs.name + ":1", "show", "inode"]))
        self.assertGreaterEqual(
            mds1_inotable['1']['data']['inotable']['free'][0]['start'], file_ino)

    def test_rebuild_snaptable(self):
        """
        The scan_links command repair snaptable
        """
        self.fs.set_allow_new_snaps(True)

        self.mount_a.run_shell(["mkdir", "dir1"])
        self.mount_a.run_shell(["mkdir", "dir1/.snap/s1"])
        self.mount_a.run_shell(["mkdir", "dir1/.snap/s2"])
        self.mount_a.run_shell(["rmdir", "dir1/.snap/s2"])

        self.mount_a.umount_wait()

        mds0_id = self.fs.get_active_names()[0]
        self.fs.mds_asok(["flush", "journal"], mds0_id)

        # wait for mds to update removed snaps
        time.sleep(10)

        old_snaptable = json.loads(self.fs.table_tool([self.fs.name + ":0", "show", "snap"]))
        # stamps may have minor difference
        for item in old_snaptable['snapserver']['snaps']:
            del item['stamp']

        self.fs.rados(["rm", "mds_snaptable"])
        self.fs.data_scan(["scan_links", "--filesystem", self.fs.name])

        new_snaptable = json.loads(self.fs.table_tool([self.fs.name + ":0", "show", "snap"]))
        for item in new_snaptable['snapserver']['snaps']:
            del item['stamp']
        self.assertGreaterEqual(
            new_snaptable['snapserver']['last_snap'], old_snaptable['snapserver']['last_snap'])
        self.assertEqual(
            new_snaptable['snapserver']['snaps'], old_snaptable['snapserver']['snaps'])
