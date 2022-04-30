"""
Helper methods to test that MON and MDS caps are enforced properly.
"""
from os.path import join as os_path_join

from tasks.cephfs.cephfs_test_case import CephFSTestCase

from teuthology.orchestra.run import Raw


class CapTester(CephFSTestCase):
    """
    Test that MON and MDS caps are enforced.

    MDS caps are tested by exercising read-write permissions and MON caps are
    tested using output of command "ceph fs ls". Besides, it provides
    write_test_files() which creates test files at the given path on CephFS
    mounts passed to it.

    USAGE: Call write_test_files() method at the beginning of the test and
    once the caps that needs to be tested are assigned to the client and
    CephFS be remount for caps to effective, call run_cap_tests(),
    run_mon_cap_tests() or run_mds_cap_tests() as per the need.
    """

    def write_test_files(self, mounts, testpath=''):
        """
        Exercising 'r' and 'w' access levels on a file on CephFS mount is
        pretty routine across all tests for caps. Adding to method to write
        that file will reduce clutter in these tests.

        This methods writes a fixed data in a file with a fixed name located
        at the path passed in testpath for the given list of mounts. If
        testpath is empty, the file is created at the root of the CephFS.
        """
        dirname, filename = 'testdir', 'testfile'
        self.test_set = []
        # XXX: The reason behind testpath[1:] below is that the testpath is
        # supposed to contain a path inside CephFS (which might be passed as
        # an absolute path). os.path.join() deletes all previous path
        # components when it encounters a path component starting with '/'.
        # Deleting the first '/' from the string in testpath ensures that
        # previous path components are not deleted by os.path.join().
        if testpath:
            testpath = testpath[1:] if testpath[0] == '/' else testpath
        # XXX: passing just '/' screw up os.path.join() ahead.
        if testpath == '/':
            testpath = ''

        for mount_x in mounts:
            dirpath = os_path_join(mount_x.hostfs_mntpt, testpath, dirname)
            mount_x.run_shell(f'mkdir {dirpath}')
            filepath = os_path_join(dirpath, filename)
            # XXX: the reason behind adding filepathm, cephfs_name and both
            # mntpts is to avoid a test bug where we mount cephfs1 but what
            # ends up being mounted cephfs2. since filepath and filedata are
            # identical, how would tests figure otherwise that they are
            # accessing the right filename but on wrong CephFS.
            filedata = (f'filepath = {filepath}\n'
                        f'cephfs_name = {mount_x.cephfs_name}\n'
                        f'cephfs_mntpt = {mount_x.cephfs_mntpt}\n'
                        f'hostfs_mntpt = {mount_x.hostfs_mntpt}')
            mount_x.write_file(filepath, filedata)
            self.test_set.append((mount_x, filepath, filedata))

    def run_cap_tests(self, perm, mntpt=None):
        # TODO
        #self.run_mon_cap_tests()
        self.run_mds_cap_tests(perm, mntpt=mntpt)

    def _get_fsnames_from_moncap(self, moncap):
        fsnames = []
        while moncap.find('fsname=') != -1:
            fsname_first_char = moncap.index('fsname=') + len('fsname=')

            if ',' in moncap:
                last = moncap.index(',')
                fsname = moncap[fsname_first_char : last]
                moncap = moncap.replace(moncap[0 : last+1], '')
            else:
                fsname = moncap[fsname_first_char : ]
                moncap = moncap.replace(moncap[0 : ], '')

            fsnames.append(fsname)

        return fsnames

    def run_mon_cap_tests(self, def_fs, client_id):
        """
        Check that MON cap is enforced for a client by searching for a Ceph
        FS name in output of cmd "fs ls" executed with that client's caps.

        def_fs stands for default FS on Ceph cluster.
        """
        get_cluster_cmd_op = def_fs.mon_manager.raw_cluster_cmd

        keyring = get_cluster_cmd_op(args=f'auth get client.{client_id}')

        moncap = None
        for line in keyring.split('\n'):
            if 'caps mon' in line:
                moncap = line[line.find(' = "') + 4 : -1]
                break
        else:
            raise RuntimeError('run_mon_cap_tests(): mon cap not found in '
                               'keyring. keyring -\n' + keyring)

        keyring_path = def_fs.admin_remote.mktemp(data=keyring)

        fsls = get_cluster_cmd_op(
            args=f'fs ls --id {client_id} -k {keyring_path}')

        if 'fsname=' not in moncap:
            fsls_admin = get_cluster_cmd_op(args='fs ls')
            self.assertEqual(fsls, fsls_admin)
            return

        for fsname in self._get_fsnames_from_moncap(moncap):
            self.assertIn('name: ' + fsname, fsls)

    def run_mds_cap_tests(self, perm, mntpt=None):
        """
        Run test for read perm and, for write perm, run positive test if it
        is present and run negative test if not.
        """
        # XXX: mntpt is path inside cephfs that serves as root for current
        # mount. Therefore, this path must me deleted from self.filepaths.
        # Example -
        #   orignal path: /mnt/cephfs_x/dir1/dir2/testdir
        #   cephfs dir serving as root for current mnt: /dir1/dir2
        #   therefore, final path: /mnt/cephfs_x//testdir
        if mntpt:
            self.test_set = [(x, y.replace(mntpt, ''), z) for x, y, z in \
                             self.test_set]

        self.conduct_pos_test_for_read_caps()

        if perm == 'rw':
            self.conduct_pos_test_for_write_caps()
        elif perm == 'r':
            self.conduct_neg_test_for_write_caps()
        else:
            raise RuntimeError(f'perm = {perm}\nIt should be "r" or "rw".')

    def conduct_pos_test_for_read_caps(self):
        for mount, path, data in self.test_set:
            contents = mount.read_file(path)
            self.assertEqual(data, contents)

    def conduct_pos_test_for_write_caps(self):
        for mount, path, data in self.test_set:
            mount.write_file(path=path, data=data)
            contents = mount.read_file(path=path)
            self.assertEqual(data, contents)

    def conduct_neg_test_for_write_caps(self):
        possible_errmsgs = ('permission denied', 'operation not permitted')
        cmdargs = ['echo', 'some random data', Raw('|'), 'tee']

        # don't use data, cmd args to write are set already above.
        for mount, path, data in self.test_set:
            cmdargs.append(path)
            mount.negtestcmd(args=cmdargs, retval=1, errmsgs=possible_errmsgs)
            cmdargs.pop(-1)
            log.info('absence of write perm was tested successfully: '
                     f'failed to be write data to file {path}.')
