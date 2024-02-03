
"""
Test that the forward scrub functionality can traverse metadata and apply
requested tags, on well formed metadata.

This is *not* the real testing for forward scrub, which will need to test
how the functionality responds to damaged metadata.

"""
import logging
import json
import errno

from collections import namedtuple
from io import BytesIO
from textwrap import dedent

from teuthology.exceptions import CommandFailedError
from tasks.cephfs.cephfs_test_case import CephFSTestCase

import struct

log = logging.getLogger(__name__)


ValidationError = namedtuple("ValidationError", ["exception", "backtrace"])


class TestForwardScrub(CephFSTestCase):
    MDSS_REQUIRED = 1

    def _read_str_xattr(self, pool, obj, attr):
        """
        Read a ceph-encoded string from a rados xattr
        """
        output = self.fs.mon_manager.do_rados(["getxattr", obj, attr], pool=pool,
                               stdout=BytesIO()).stdout.getvalue()
        strlen = struct.unpack('i', output[0:4])[0]
        return output[4:(4 + strlen)].decode(encoding='ascii')

    def _get_paths_to_ino(self):
        inos = {}
        p = self.mount_a.run_shell(["find", "./"])
        paths = p.stdout.getvalue().strip().split()
        for path in paths:
            inos[path] = self.mount_a.path_to_ino(path)

        return inos

    def _is_MDS_damage(self):
        return "MDS_DAMAGE" in self.mds_cluster.mon_manager.get_mon_health()['checks']

    def test_apply_tag(self):
        self.mount_a.run_shell(["mkdir", "parentdir"])
        self.mount_a.run_shell(["mkdir", "parentdir/childdir"])
        self.mount_a.run_shell(["touch", "rfile"])
        self.mount_a.run_shell(["touch", "parentdir/pfile"])
        self.mount_a.run_shell(["touch", "parentdir/childdir/cfile"])

        # Build a structure mapping path to inode, as we will later want
        # to check object by object and objects are named after ino number
        inos = self._get_paths_to_ino()

        # Flush metadata: this is a friendly test of forward scrub so we're skipping
        # the part where it's meant to cope with dirty metadata
        self.mount_a.umount_wait()
        self.fs.mds_asok(["flush", "journal"])

        tag = "mytag"

        # Execute tagging forward scrub
        self.fs.mds_asok(["tag", "path", "/parentdir", tag])
        # Wait for completion
        import time
        time.sleep(10)
        # FIXME watching clog isn't a nice mechanism for this, once we have a ScrubMap we'll
        # watch that instead

        # Check that dirs were tagged
        for dirpath in ["./parentdir", "./parentdir/childdir"]:
            self.assertTagged(inos[dirpath], tag, self.fs.get_metadata_pool_name())

        # Check that files were tagged
        for filepath in ["./parentdir/pfile", "./parentdir/childdir/cfile"]:
            self.assertTagged(inos[filepath], tag, self.fs.get_data_pool_name())

        # This guy wasn't in the tag path, shouldn't have been tagged
        self.assertUntagged(inos["./rfile"])

    def assertUntagged(self, ino):
        file_obj_name = "{0:x}.00000000".format(ino)
        with self.assertRaises(CommandFailedError):
            self._read_str_xattr(
                self.fs.get_data_pool_name(),
                file_obj_name,
                "scrub_tag"
            )

    def assertTagged(self, ino, tag, pool):
        file_obj_name = "{0:x}.00000000".format(ino)
        wrote = self._read_str_xattr(
            pool,
            file_obj_name,
            "scrub_tag"
        )
        self.assertEqual(wrote, tag)

    def _validate_linkage(self, expected):
        inos = self._get_paths_to_ino()
        try:
            self.assertDictEqual(inos, expected)
        except AssertionError:
            log.error("Expected: {0}".format(json.dumps(expected, indent=2)))
            log.error("Actual: {0}".format(json.dumps(inos, indent=2)))
            raise

    def test_orphan_scan(self):
        # Create some files whose metadata we will flush
        self.mount_a.run_python(dedent("""
            import os
            mount_point = "{mount_point}"
            parent = os.path.join(mount_point, "parent")
            os.mkdir(parent)
            flushed = os.path.join(parent, "flushed")
            os.mkdir(flushed)
            for f in ["alpha", "bravo", "charlie"]:
                open(os.path.join(flushed, f), 'w').write(f)
        """.format(mount_point=self.mount_a.mountpoint)))

        inos = self._get_paths_to_ino()

        # Flush journal
        # Umount before flush to avoid cap releases putting
        # things we don't want in the journal later.
        self.mount_a.umount_wait()
        self.fs.flush()

        # Create a new inode that's just in the log, i.e. would
        # look orphaned to backward scan if backward scan wisnae
        # respectin' tha scrub_tag xattr.
        self.mount_a.mount_wait()
        self.mount_a.run_shell(["mkdir", "parent/unflushed"])
        self.mount_a.run_shell(["dd", "if=/dev/urandom",
                                "of=./parent/unflushed/jfile",
                                "bs=1M", "count=8"])
        inos["./parent/unflushed"] = self.mount_a.path_to_ino("./parent/unflushed")
        inos["./parent/unflushed/jfile"] = self.mount_a.path_to_ino("./parent/unflushed/jfile")
        self.mount_a.umount_wait()

        # Orphan an inode by deleting its dentry
        # Our victim will be.... bravo.
        self.mount_a.umount_wait()
        self.fs.fail()
        self.fs.set_ceph_conf('mds', 'mds verify scatter', False)
        self.fs.set_ceph_conf('mds', 'mds debug scatterstat', False)
        frag_obj_id = "{0:x}.00000000".format(inos["./parent/flushed"])
        self.fs.radosm(["rmomapkey", frag_obj_id, "bravo_head"])

        self.fs.set_joinable()
        self.fs.wait_for_daemons()

        # See that the orphaned file is indeed missing from a client's POV
        self.mount_a.mount_wait()
        damaged_state = self._get_paths_to_ino()
        self.assertNotIn("./parent/flushed/bravo", damaged_state)
        self.mount_a.umount_wait()

        # Run a tagging forward scrub
        tag = "mytag123"
        self.fs.rank_asok(["tag", "path", "/parent", tag])

        # See that the orphan wisnae tagged
        self.assertUntagged(inos['./parent/flushed/bravo'])

        # See that the flushed-metadata-and-still-present files are tagged
        self.assertTagged(inos['./parent/flushed/alpha'], tag, self.fs.get_data_pool_name())
        self.assertTagged(inos['./parent/flushed/charlie'], tag, self.fs.get_data_pool_name())

        # See that journalled-but-not-flushed file *was* tagged
        self.assertTagged(inos['./parent/unflushed/jfile'], tag, self.fs.get_data_pool_name())

        # okay, now we are going to run cephfs-data-scan. It's necessary to
        # have a clean journal otherwise replay will blowup on mismatched
        # inotable versions (due to scan_links)
        self.fs.flush()
        self.fs.fail()
        self.fs.journal_tool(["journal", "reset", "--force"], 0)

        # Run cephfs-data-scan targeting only orphans
        self.fs.data_scan(["scan_extents", self.fs.get_data_pool_name()])
        self.fs.data_scan([
            "scan_inodes",
            "--filter-tag", tag,
            self.fs.get_data_pool_name()
        ])
        self.fs.data_scan(["scan_links"])

        # After in-place injection stats should be kosher again
        self.fs.set_ceph_conf('mds', 'mds verify scatter', True)
        self.fs.set_ceph_conf('mds', 'mds debug scatterstat', True)

        # And we should have all the same linkage we started with,
        # and no lost+found, and no extra inodes!
        self.fs.set_joinable()
        self.fs.wait_for_daemons()
        self.mount_a.mount_wait()
        self._validate_linkage(inos)

    def _stash_inotable(self):
        # Get all active ranks
        ranks = self.fs.get_all_mds_rank()

        inotable_dict = {}
        for rank in ranks:
            inotable_oid = "mds{rank:d}_".format(rank=rank) + "inotable"
            print("Trying to fetch inotable object: " + inotable_oid)

            #self.fs.get_metadata_object("InoTable", "mds0_inotable")
            inotable_raw = self.fs.radosmo(['get', inotable_oid, '-'])
            inotable_dict[inotable_oid] = inotable_raw
        return inotable_dict

    def test_inotable_sync(self):
        self.mount_a.write_n_mb("file1_sixmegs", 6)

        # Flush journal
        self.mount_a.umount_wait()
        self.fs.mds_asok(["flush", "journal"])

        inotable_copy = self._stash_inotable()

        self.mount_a.mount_wait()

        self.mount_a.write_n_mb("file2_sixmegs", 6)
        self.mount_a.write_n_mb("file3_sixmegs", 6)

        inos = self._get_paths_to_ino()

        # Flush journal
        self.mount_a.umount_wait()
        self.fs.mds_asok(["flush", "journal"])

        self.mount_a.umount_wait()

        with self.assert_cluster_log("inode table repaired", invert_match=True):
            out_json = self.fs.run_scrub(["start", "/", "repair,recursive"])
            self.assertNotEqual(out_json, None)
            self.assertEqual(out_json["return_code"], 0)
            self.assertEqual(self.fs.wait_until_scrub_complete(tag=out_json["scrub_tag"]), True)

        self.fs.fail()

        # Truncate the journal (to ensure the inotable on disk
        # is all that will be in the InoTable in memory)

        self.fs.journal_tool(["event", "splice",
                              "--inode={0}".format(inos["./file2_sixmegs"]), "summary"], 0)

        self.fs.journal_tool(["event", "splice",
                              "--inode={0}".format(inos["./file3_sixmegs"]), "summary"], 0)

        # Revert to old inotable.
        for key, value in inotable_copy.items():
            self.fs.radosm(["put", key, "-"], stdin=BytesIO(value))

        self.fs.set_joinable()
        self.fs.wait_for_daemons()

        with self.assert_cluster_log("inode table repaired"):
            out_json = self.fs.run_scrub(["start", "/", "repair,recursive"])
            self.assertNotEqual(out_json, None)
            self.assertEqual(out_json["return_code"], 0)
            self.assertEqual(self.fs.wait_until_scrub_complete(tag=out_json["scrub_tag"]), True)

        self.fs.fail()
        table_text = self.fs.table_tool(["0", "show", "inode"])
        table = json.loads(table_text)
        self.assertGreater(
                table['0']['data']['inotable']['free'][0]['start'],
                inos['./file3_sixmegs'])

    def test_backtrace_repair(self):
        """
        That the MDS can repair an inodes backtrace in the data pool
        if it is found to be damaged.
        """
        # Create a file for subsequent checks
        self.mount_a.run_shell(["mkdir", "parent_a"])
        self.mount_a.run_shell(["touch", "parent_a/alpha"])
        file_ino = self.mount_a.path_to_ino("parent_a/alpha")

        # That backtrace and layout are written after initial flush
        self.fs.mds_asok(["flush", "journal"])
        backtrace = self.fs.read_backtrace(file_ino)
        self.assertEqual(['alpha', 'parent_a'],
                         [a['dname'] for a in backtrace['ancestors']])

        # Go corrupt the backtrace
        self.fs._write_data_xattr(file_ino, "parent",
                                  "oh i'm sorry did i overwrite your xattr?")

        with self.assert_cluster_log("bad backtrace on inode"):
            out_json = self.fs.run_scrub(["start", "/", "repair,recursive"])
            self.assertNotEqual(out_json, None)
            self.assertEqual(out_json["return_code"], 0)
            self.assertEqual(self.fs.wait_until_scrub_complete(tag=out_json["scrub_tag"]), True)

        self.fs.mds_asok(["flush", "journal"])
        backtrace = self.fs.read_backtrace(file_ino)
        self.assertEqual(['alpha', 'parent_a'],
                         [a['dname'] for a in backtrace['ancestors']])

    def test_health_status_after_dentry_repair(self):
        """
        Test that the damage health status is cleared
        after the damaged dentry is repaired
        """
        # Create a file for checks
        self.mount_a.run_shell(["mkdir", "subdir/"])

        self.mount_a.run_shell(["touch", "subdir/file_undamaged"])
        self.mount_a.run_shell(["touch", "subdir/file_to_be_damaged"])

        subdir_ino = self.mount_a.path_to_ino("subdir")

        self.mount_a.umount_wait()
        for mds_name in self.fs.get_active_names():
            self.fs.mds_asok(["flush", "journal"], mds_name)

        self.fs.fail()

        # Corrupt a dentry
        junk = "deadbeef" * 10
        dirfrag_obj = "{0:x}.00000000".format(subdir_ino)
        self.fs.radosm(["setomapval", dirfrag_obj, "file_to_be_damaged_head", junk])

        # Start up and try to list it
        self.fs.set_joinable()
        self.fs.wait_for_daemons()

        self.mount_a.mount_wait()
        dentries = self.mount_a.ls("subdir/")

        # The damaged guy should have disappeared
        self.assertEqual(dentries, ["file_undamaged"])

        # I should get ENOENT if I try and read it normally, because
        # the dir is considered complete
        try:
            self.mount_a.stat("subdir/file_to_be_damaged", wait=True)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            raise AssertionError("Expected ENOENT")

        nfiles = self.mount_a.getfattr("./subdir", "ceph.dir.files")
        self.assertEqual(nfiles, "2")

        self.mount_a.umount_wait()

        out_json = self.fs.run_scrub(["start", "/subdir", "recursive"])
        self.assertEqual(self.fs.wait_until_scrub_complete(tag=out_json["scrub_tag"]), True)

        # Check that an entry for dentry damage is created in the damage table
        damage = json.loads(
            self.fs.mon_manager.raw_cluster_cmd(
                'tell', 'mds.{0}'.format(self.fs.get_active_names()[0]),
                "damage", "ls", '--format=json-pretty'))
        self.assertEqual(len(damage), 1)
        self.assertEqual(damage[0]['damage_type'], "dentry")
        self.wait_until_true(lambda: self._is_MDS_damage(), timeout=100)

        out_json = self.fs.run_scrub(["start", "/subdir", "repair,recursive"])
        self.assertEqual(self.fs.wait_until_scrub_complete(tag=out_json["scrub_tag"]), True)

        # Check that the entry is cleared from the damage table
        damage = json.loads(
            self.fs.mon_manager.raw_cluster_cmd(
                'tell', 'mds.{0}'.format(self.fs.get_active_names()[0]),
                "damage", "ls", '--format=json-pretty'))
        self.assertEqual(len(damage), 0)
        self.wait_until_true(lambda: not self._is_MDS_damage(), timeout=100)

        self.mount_a.mount_wait()

        # Check that the file count is now correct
        nfiles = self.mount_a.getfattr("./subdir", "ceph.dir.files")
        self.assertEqual(nfiles, "1")

        # Clean up the omap object
        self.fs.radosm(["setomapval", dirfrag_obj, "file_to_be_damaged_head", junk])

    def test_health_status_after_dirfrag_repair(self):
        """
        Test that the damage health status is cleared
        after the damaged dirfrag is repaired
        """
        self.mount_a.run_shell(["mkdir", "dir"])
        self.mount_a.run_shell(["touch", "dir/file"])
        self.mount_a.run_shell(["mkdir", "testdir"])
        self.mount_a.run_shell(["ln", "dir/file", "testdir/hardlink"])

        dir_ino = self.mount_a.path_to_ino("dir")

        # Ensure everything is written to backing store
        self.mount_a.umount_wait()
        self.fs.mds_asok(["flush", "journal"])

        # Drop everything from the MDS cache
        self.fs.fail()

        self.fs.radosm(["rm", "{0:x}.00000000".format(dir_ino)])

        self.fs.journal_tool(['journal', 'reset'], 0)
        self.fs.set_joinable()
        self.fs.wait_for_daemons()
        self.mount_a.mount_wait()

        # Check that touching the hardlink gives EIO
        ran = self.mount_a.run_shell(["stat", "testdir/hardlink"], wait=False)
        try:
            ran.wait()
        except CommandFailedError:
            self.assertTrue("Input/output error" in ran.stderr.getvalue())

        out_json = self.fs.run_scrub(["start", "/dir", "recursive"])
        self.assertEqual(self.fs.wait_until_scrub_complete(tag=out_json["scrub_tag"]), True)

        # Check that an entry is created in the damage table
        damage = json.loads(
            self.fs.mon_manager.raw_cluster_cmd(
                'tell', 'mds.{0}'.format(self.fs.get_active_names()[0]),
                "damage", "ls", '--format=json-pretty'))
        self.assertEqual(len(damage), 3)
        damage_types = set()
        for i in range(0, 3):
            damage_types.add(damage[i]['damage_type'])
        self.assertIn("dir_frag", damage_types)
        self.wait_until_true(lambda: self._is_MDS_damage(), timeout=100)

        out_json = self.fs.run_scrub(["start", "/dir", "recursive,repair"])
        self.assertEqual(self.fs.wait_until_scrub_complete(tag=out_json["scrub_tag"]), True)

        # Check that the entry is cleared from the damage table
        damage = json.loads(
            self.fs.mon_manager.raw_cluster_cmd(
                'tell', 'mds.{0}'.format(self.fs.get_active_names()[0]),
                "damage", "ls", '--format=json-pretty'))
        self.assertEqual(len(damage), 1)
        self.assertNotEqual(damage[0]['damage_type'], "dir_frag")

        self.mount_a.umount_wait()
        self.fs.mds_asok(["flush", "journal"])
        self.fs.fail()

        # Run cephfs-data-scan
        self.fs.data_scan(["scan_extents", self.fs.get_data_pool_name()])
        self.fs.data_scan(["scan_inodes", self.fs.get_data_pool_name()])
        self.fs.data_scan(["scan_links"])

        self.fs.set_joinable()
        self.fs.wait_for_daemons()
        self.mount_a.mount_wait()

        out_json = self.fs.run_scrub(["start", "/dir", "recursive,repair"])
        self.assertEqual(self.fs.wait_until_scrub_complete(tag=out_json["scrub_tag"]), True)
        damage = json.loads(
            self.fs.mon_manager.raw_cluster_cmd(
                'tell', 'mds.{0}'.format(self.fs.get_active_names()[0]),
                "damage", "ls", '--format=json-pretty'))
        self.assertEqual(len(damage), 0)
        self.wait_until_true(lambda: not self._is_MDS_damage(), timeout=100)

    def test_health_status_after_backtrace_repair(self):
        """
        Test that the damage health status is cleared
        after the damaged backtrace is repaired
        """
        # Create a file for checks
        self.mount_a.run_shell(["mkdir", "dir_test"])
        self.mount_a.run_shell(["touch", "dir_test/file"])
        file_ino = self.mount_a.path_to_ino("dir_test/file")

        # That backtrace and layout are written after initial flush
        self.fs.mds_asok(["flush", "journal"])
        backtrace = self.fs.read_backtrace(file_ino)
        self.assertEqual(['file', 'dir_test'],
                         [a['dname'] for a in backtrace['ancestors']])

        # Corrupt the backtrace
        self.fs._write_data_xattr(file_ino, "parent",
                                  "The backtrace is corrupted")

        out_json = self.fs.run_scrub(["start", "/", "recursive"])
        self.assertEqual(self.fs.wait_until_scrub_complete(tag=out_json["scrub_tag"]), True)
        
        # Check that an entry for backtrace damage is created in the damage table
        damage = json.loads(
            self.fs.mon_manager.raw_cluster_cmd(
                'tell', 'mds.{0}'.format(self.fs.get_active_names()[0]),
                "damage", "ls", '--format=json-pretty'))
        self.assertEqual(len(damage), 1)
        self.assertEqual(damage[0]['damage_type'], "backtrace")
        self.wait_until_true(lambda: self._is_MDS_damage(), timeout=100)

        out_json = self.fs.run_scrub(["start", "/", "repair,recursive,force"])
        self.assertEqual(self.fs.wait_until_scrub_complete(tag=out_json["scrub_tag"]), True)

        # Check that the entry is cleared from the damage table
        damage = json.loads(
            self.fs.mon_manager.raw_cluster_cmd(
                'tell', 'mds.{0}'.format(self.fs.get_active_names()[0]),
                "damage", "ls", '--format=json-pretty'))
        self.assertEqual(len(damage), 0)
        self.wait_until_true(lambda: not self._is_MDS_damage(), timeout=100)
