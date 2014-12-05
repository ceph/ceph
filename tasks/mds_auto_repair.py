
"""
Exercise the MDS's auto repair functions
"""

import contextlib
import logging
import time

from teuthology.orchestra.run import CommandFailedError

from tasks.cephfs.filesystem import Filesystem
from tasks.cephfs.fuse_mount import FuseMount
from tasks.cephfs.cephfs_test_case import CephFSTestCase, run_tests


log = logging.getLogger(__name__)


# Arbitrary timeouts for operations involving restarting
# an MDS or waiting for it to come up
MDS_RESTART_GRACE = 60

class TestMDSAutoRepair(CephFSTestCase):
    # Environment references
    mount_a = None

    def __init__(self, *args, **kwargs):
        super(TestMDSAutoRepair, self).__init__(*args, **kwargs)

        self.configs_set = set()

    def set_conf(self, subsys, key, value):
        self.configs_set.add((subsys, key))
        self.fs.set_ceph_conf(subsys, key, value)

    def setUp(self):
        self.fs.mds_restart()
        self.fs.wait_for_daemons()
        self.mount_a.mount()
        self.mount_a.wait_until_mounted()

    def tearDown(self):
        self.fs.clear_firewall()
        self.mount_a.teardown()

        for subsys, key in self.configs_set:
            self.fs.clear_ceph_conf(subsys, key)

    def test_backtrace_repair(self):
        """
        MDS should verify/fix backtrace on fetch dirfrag
        """

        # trim log segment as fast as possible
        self.fs.set_ceph_conf('mds', 'mds cache size', 100)
        self.fs.set_ceph_conf('mds', 'mds log max segments', 2)
        self.fs.set_ceph_conf('mds', 'mds log events per segment', 1)
        self.fs.set_ceph_conf('mds', 'mds verify backtrace', 1)
        self.fs.mds_restart()
        self.fs.wait_for_daemons()

        create_script = "mkdir {0}; for i in `seq 0 500`; do touch {0}/file$i; done"
        # create main test directory
        self.mount_a.run_shell(["sudo", "bash", "-c", create_script.format("testdir1")])

        # create more files in another directory. make sure MDS trim dentries in testdir1
        self.mount_a.run_shell(["sudo", "bash", "-c", create_script.format("testdir2")])

        # flush journal entries to dirfrag objects
        self.fs.mds_asok(['flush', 'journal'])

        # drop inodes caps
        self.mount_a.umount_wait()
        self.mount_a.mount()
        self.mount_a.wait_until_mounted()

        # wait MDS to trim dentries in testdir1. 60 seconds should be long enough.
        time.sleep(60)

        # remove testdir1's backtrace
        proc = self.mount_a.run_shell(["sudo", "ls", "-id", "testdir1"])
        self.assertEqual(proc.exitstatus, 0)
        objname = "{:x}.00000000".format(long(proc.stdout.getvalue().split()[0]))
        proc = self.mount_a.run_shell(["sudo", "rados", "-p", "metadata", "rmxattr", objname, "parent"])
        self.assertEqual(proc.exitstatus, 0);

        # readdir (fetch dirfrag) should fix testdir1's backtrace
        self.mount_a.run_shell(["sudo", "ls", "testdir1"])

        # add more entries to journal
        self.mount_a.run_shell(["sudo", "rm", "-rf", " testdir2"])

        # flush journal entries to dirfrag objects
        self.fs.mds_asok(['flush', 'journal'])

        # check if backtrace exists
        proc = self.mount_a.run_shell(["sudo", "rados", "-p", "metadata", "getxattr", objname, "parent"])
        self.assertEqual(proc.exitstatus, 0)

    def test_mds_readonly(self):
        """
        test if MDS behave correct when it's readonly
        """
        # operation should successd when MDS is not readonly
        self.mount_a.run_shell(["sudo", "touch", "test_file1"])
        writer = self.mount_a.write_background(loop=True)

        time.sleep(10)
        self.assertFalse(writer.finished)

        # force MDS to read-only mode
        self.fs.mds_asok(['force_readonly'])
        time.sleep(10)

        # touching test file should fail
        try:
            self.mount_a.run_shell(["sudo", "touch", "test_file1"])
        except CommandFailedError, e:
            pass
        else:
            self.assertTrue(False)

        # background writer also should fail
        self.assertTrue(writer.finished)

        # restart mds to make it writable
        self.fs.mds_restart()
        self.fs.wait_for_daemons()

@contextlib.contextmanager
def task(ctx, config):
    fs = Filesystem(ctx, config)
    mount_a = ctx.mounts.values()[0]

    # Stash references on ctx so that we can easily debug in interactive mode
    # =======================================================================
    ctx.filesystem = fs
    ctx.mount_a = mount_a

    run_tests(ctx, config, TestMDSAutoRepair, {
        'fs': fs,
        'mount_a': mount_a,
    })

    # Continue to any downstream tasks
    # ================================
    yield
