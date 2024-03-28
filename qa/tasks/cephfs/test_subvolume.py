import logging

from io import StringIO
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

    def test_subvolume_vxattr(self):
        """
        To test presence and absence of ceph.dir.subvolume xattr on a subvolume
        """
        # create a subvolume
        self.get_ceph_cmd_stdout('fs', 'subvolumegroup', 'create', self.fs.name, 'grp1')
        self.get_ceph_cmd_stdout('fs', 'subvolume', 'create', self.fs.name, 'sv1', 'grp1')

        # verify that ceph.dir.subvolume has value "1"
        stdo = StringIO()
        self.mount_a.run_shell(['getfattr', '-n', 'ceph.dir.subvolume',
                                '--only-values', 'volumes/grp1/sv1'],
                               stdout=stdo)
        o = stdo.getvalue().strip()
        self.assertTrue(o == "1")

        # clear the ceph.dir.subvolume vxattr value
        self.mount_a.run_shell(['sudo', 'setfattr', '-n', 'ceph.dir.subvolume',
                                '-v', '0', 'volumes/grp1/sv1'])
        stdo = StringIO()
        # verify that ceph.dir.subvolume has value "0"
        self.mount_a.run_shell(['getfattr', '-n', 'ceph.dir.subvolume',
                                '--only-values', 'volumes/grp1/sv1'],
                               stdout=stdo)
        o = stdo.getvalue().strip()
        self.assertTrue(o == "0")

        self.get_ceph_cmd_stdout('fs', 'subvolume', 'rm', self.fs.name, 'sv1', 'grp1')
        self.get_ceph_cmd_stdout('fs', 'subvolumegroup', 'rm', self.fs.name, 'grp1')
