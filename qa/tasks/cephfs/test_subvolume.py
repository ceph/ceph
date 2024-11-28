import logging
from time import sleep

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)


class TestSubvolume(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    def setUp(self):
        super().setUp()
        self.setup_test()

    def tearDown(self):
        # clean up
        self.cleanup_test()
        super().tearDown()

    def setup_test(self):
        self.mount_a.run_shell(['mkdir', 'group'])
        self.mount_a.run_shell(['mkdir', 'group/subvol1'])
        self.mount_a.run_shell(['setfattr', '-n', 'ceph.dir.subvolume',
                                '-v', '1', 'group/subvol1'])
        self.mount_a.run_shell(['mv', 'group/subvol1', 'group/subvol2'])

    def cleanup_test(self):
        self.mount_a.run_shell(['rm', '-rf', 'group'])

    def test_subvolume_move_out_file(self):
        """
        To verify that file can't be moved out of subvolume
        """
        self.mount_a.run_shell(['touch', 'group/subvol2/file1'])

        # file can't be moved out of a subvolume
        with self.assertRaises(CommandFailedError):
            self.mount_a.run_shell(['rename', 'group/subvol2/file1',
                                    'group/file1', 'group/subvol2/file1'])


    def test_subvolume_move_in_file(self):
        """
        To verify that file can't be moved into subvolume
        """
        # file can't be moved into a subvolume
        self.mount_a.run_shell(['touch', 'group/file2'])
        with self.assertRaises(CommandFailedError):
            self.mount_a.run_shell(['rename', 'group/file2',
                                    'group/subvol2/file2', 'group/file2'])

    def test_subvolume_hardlink_to_outside(self):
        """
        To verify that file can't be hardlinked to outside subvolume
        """
        self.mount_a.run_shell(['touch', 'group/subvol2/file1'])

        # create hard link within subvolume
        self.mount_a.run_shell(['ln',
                                'group/subvol2/file1', 'group/subvol2/file1_'])

        # hard link can't be created out of subvolume
        with self.assertRaises(CommandFailedError):
            self.mount_a.run_shell(['ln',
                                    'group/subvol2/file1', 'group/file1_'])

    def test_subvolume_hardlink_to_inside(self):
        """
        To verify that file can't be hardlinked to inside subvolume
        """
        self.mount_a.run_shell(['touch', 'group/subvol2/file1'])

        # create hard link within subvolume
        self.mount_a.run_shell(['ln',
                                'group/subvol2/file1', 'group/subvol2/file1_'])

        # hard link can't be created inside subvolume
        self.mount_a.run_shell(['touch', 'group/file2'])
        with self.assertRaises(CommandFailedError):
            self.mount_a.run_shell(['ln',
                                    'group/file2', 'group/subvol2/file2_'])

    def test_subvolume_snapshot_inside_subvolume_subdir(self):
        """
        To verify that snapshot can't be taken for a subvolume subdir
        """
        self.mount_a.run_shell(['touch', 'group/subvol2/file1'])

        # create snapshot at subvolume root
        self.mount_a.run_shell(['mkdir', 'group/subvol2/.snap/s1'])

        # can't create snapshot in a descendent dir of subvolume
        self.mount_a.run_shell(['mkdir', 'group/subvol2/dir'])
        with self.assertRaises(CommandFailedError):
            self.mount_a.run_shell(['mkdir', 'group/subvol2/dir/.snap/s2'])

        # clean up
        self.mount_a.run_shell(['rmdir', 'group/subvol2/.snap/s1'])

    def test_subvolume_file_move_across_subvolumes(self):
        """
        To verify that file can't be moved across subvolumes
        """
        self.mount_a.run_shell(['touch', 'group/subvol2/file1'])

        # create another subvol
        self.mount_a.run_shell(['mkdir', 'group/subvol3'])
        self.mount_a.run_shell(['setfattr', '-n', 'ceph.dir.subvolume',
                                '-v', '1', 'group/subvol3'])

        # can't move file across subvolumes
        with self.assertRaises(CommandFailedError):
            self.mount_a.run_shell(['rename', 'group/subvol2/file1',
                                    'group/subvol3/file1',
                                    'group/subvol2/file1'])

    def test_subvolume_hardlink_across_subvolumes(self):
        """
        To verify that hardlink can't be created across subvolumes
        """
        self.mount_a.run_shell(['touch', 'group/subvol2/file1'])

        # create another subvol
        self.mount_a.run_shell(['mkdir', 'group/subvol3'])
        self.mount_a.run_shell(['setfattr', '-n', 'ceph.dir.subvolume',
                                '-v', '1', 'group/subvol3'])

        # can't create hard link across subvolumes
        with self.assertRaises(CommandFailedError):
            self.mount_a.run_shell(['ln', 'group/subvol2/file1',
                                    'group/subvol3/file1'])

    def test_subvolume_create_subvolume_inside_subvolume(self):
        """
        To verify that subvolume can't be created inside a subvolume
        """
        # can't create subvolume inside a subvolume
        self.mount_a.run_shell(['mkdir', 'group/subvol2/dir'])
        with self.assertRaises(CommandFailedError):
            self.mount_a.run_shell(['setfattr', '-n', 'ceph.dir.subvolume',
                                    '-v', '1', 'group/subvol2/dir'])

    def test_subvolume_create_snapshot_inside_new_subvolume_parent(self):
        """
        To verify that subvolume can't be created inside a new subvolume parent
        """
        self.mount_a.run_shell(['touch', 'group/subvol2/file1'])

        # clear subvolume flag
        self.mount_a.run_shell(['setfattr', '-n', 'ceph.dir.subvolume',
                                '-v', '0', 'group/subvol2'])

        # create a snap
        self.mount_a.run_shell(['mkdir', 'group/subvol2/dir'])
        self.mount_a.run_shell(['mkdir', 'group/subvol2/dir/.snap/s2'])

        # override subdir subvolume with parent subvolume
        self.mount_a.run_shell(['setfattr', '-n', 'ceph.dir.subvolume',
                                '-v', '1', 'group/subvol2/dir'])
        self.mount_a.run_shell(['setfattr', '-n', 'ceph.dir.subvolume',
                                '-v', '1', 'group/subvol2'])

        # can't create a snap in a subdir of a subvol parent
        with self.assertRaises(CommandFailedError):
            self.mount_a.run_shell(['mkdir', 'group/subvol2/dir/.snap/s3'])

        # clean up
        self.mount_a.run_shell(['rmdir', 'group/subvol2/dir/.snap/s2'])


class TestSubvolumeReplicated(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 2

    def test_subvolume_replicated(self):
        """
        That a replica sees the subvolume flag on a directory.
        """


        self.mount_a.run_shell_payload("mkdir -p dir1/dir2/dir3/dir4")

        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()

        self.mount_a.setfattr("dir1", "ceph.dir.pin", "1")
        self.mount_a.setfattr("dir1/dir2/dir3", "ceph.dir.pin", "0") # force dir2 to be replicated
        status = self._wait_subtrees([("/dir1", 1), ("/dir1/dir2/dir3", 0)], status=status, rank=1)

        op = self.fs.rank_tell("lock", "path", "/dir1/dir2", "snap:r", rank=1)
        p = self.mount_a.setfattr("dir1/dir2", "ceph.dir.subvolume", "1", wait=False)
        sleep(2)
        reqid = self._reqid_tostr(op['reqid'])
        self.fs.kill_op(reqid, rank=1)
        p.wait()

        ino1 = self.fs.read_cache("/dir1/dir2", depth=0, rank=1)[0]
        self.assertTrue(ino1['is_subvolume'])
        self.assertTrue(ino1['is_auth'])
        replicas = ino1['auth_state']['replicas']
        self.assertIn("0", replicas)

        ino0 = self.fs.read_cache("/dir1/dir2", depth=0, rank=0)[0]
        self.assertFalse(ino0['is_auth'])
        self.assertTrue(ino0['is_subvolume'])
