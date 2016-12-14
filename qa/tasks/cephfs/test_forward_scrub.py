
"""
Test that the forward scrub functionality can traverse metadata and apply
requested tags, on well formed metadata.

This is *not* the real testing for forward scrub, which will need to test
how the functionality responds to damaged metadata.

"""
import json

import logging
from collections import namedtuple
from textwrap import dedent

from teuthology.orchestra.run import CommandFailedError
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
        output = self.fs.rados(["getxattr", obj, attr], pool=pool)
        strlen = struct.unpack('i', output[0:4])[0]
        return output[4:(4 + strlen)]

    def _get_paths_to_ino(self):
        inos = {}
        p = self.mount_a.run_shell(["find", "./"])
        paths = p.stdout.getvalue().strip().split()
        for path in paths:
            inos[path] = self.mount_a.path_to_ino(path)

        return inos

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
        self.fs.mds_asok(["flush", "journal"])

        # Create a new inode that's just in the log, i.e. would
        # look orphaned to backward scan if backward scan wisnae
        # respectin' tha scrub_tag xattr.
        self.mount_a.mount()
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
        self.fs.mds_stop()
        self.fs.mds_fail()
        self.fs.set_ceph_conf('mds', 'mds verify scatter', False)
        self.fs.set_ceph_conf('mds', 'mds debug scatterstat', False)
        frag_obj_id = "{0:x}.00000000".format(inos["./parent/flushed"])
        self.fs.rados(["rmomapkey", frag_obj_id, "bravo_head"])

        self.fs.mds_restart()
        self.fs.wait_for_daemons()

        # See that the orphaned file is indeed missing from a client's POV
        self.mount_a.mount()
        damaged_state = self._get_paths_to_ino()
        self.assertNotIn("./parent/flushed/bravo", damaged_state)
        self.mount_a.umount_wait()

        # Run a tagging forward scrub
        tag = "mytag123"
        self.fs.mds_asok(["tag", "path", "/parent", tag])

        # See that the orphan wisnae tagged
        self.assertUntagged(inos['./parent/flushed/bravo'])

        # See that the flushed-metadata-and-still-present files are tagged
        self.assertTagged(inos['./parent/flushed/alpha'], tag, self.fs.get_data_pool_name())
        self.assertTagged(inos['./parent/flushed/charlie'], tag, self.fs.get_data_pool_name())

        # See that journalled-but-not-flushed file *was* tagged
        self.assertTagged(inos['./parent/unflushed/jfile'], tag, self.fs.get_data_pool_name())

        # Run cephfs-data-scan targeting only orphans
        self.fs.mds_stop()
        self.fs.mds_fail()
        self.fs.data_scan(["scan_extents", self.fs.get_data_pool_name()])
        self.fs.data_scan([
            "scan_inodes",
            "--filter-tag", tag,
            self.fs.get_data_pool_name()
        ])

        # After in-place injection stats should be kosher again
        self.fs.set_ceph_conf('mds', 'mds verify scatter', True)
        self.fs.set_ceph_conf('mds', 'mds debug scatterstat', True)

        # And we should have all the same linkage we started with,
        # and no lost+found, and no extra inodes!
        self.fs.mds_restart()
        self.fs.wait_for_daemons()
        self.mount_a.mount()
        self._validate_linkage(inos)
