"""
Helper methods to test that MON and MDS caps are enforced properly.
"""
from os.path import join as os_path_join
from logging import getLogger
from textwrap import dedent

from teuthology.orchestra.run import Raw


log = getLogger(__name__)


def gen_mon_cap_str(caps):
    """
    Accepts a tuple of tuples, where the inner tuple contains read/write
    permissions and CephFS name.

    caps = ((perm1, fsname1), (perm2, fsname2))
    """
    def _unpack_tuple(c):
        if len(c) == 1:
            perm, fsname = c[0], None
        elif len(c) == 2:
            perm, fsname = c
        else:
            raise RuntimeError('caps tuple received isn\'t right')
        return perm, fsname

    def _gen_mon_cap_str(c):
        perm, fsname = _unpack_tuple(c)
        mon_cap = f'allow {perm}'
        if fsname:
            mon_cap += f' fsname={fsname}'
        return mon_cap

    if len(caps) == 1:
        return _gen_mon_cap_str(caps[0])

    mon_cap = ''
    for i, c in enumerate(caps):
        mon_cap += _gen_mon_cap_str(c)
        if i != len(caps) - 1:
            mon_cap += ', '

    return mon_cap

def gen_osd_cap_str(caps):
    """
    Accepts a tuple of tuples, where the inner tuple contains read/write
    permissions and CephFS name.

    caps = ((perm1, fsname1), (perm2, fsname2))
    """
    def _gen_osd_cap_str(c):
        perm, fsname = c
        osd_cap = f'allow {perm} tag cephfs'
        if fsname:
            osd_cap += f' data={fsname}'
        return osd_cap

    if len(caps) == 1:
        return _gen_osd_cap_str(caps[0])

    osd_cap = ''
    for i, c in enumerate(caps):
        osd_cap += _gen_osd_cap_str(c)
        if i != len(caps) - 1:
            osd_cap += ', '

    return osd_cap

def gen_mds_cap_str(caps):
    """
    Accepts a tuple of tuples where inner the tuple containts read/write
    permissions, Ceph FS name and CephFS mountpoint.

    caps = ((perm1, fsname1, cephfs_mntpt1), (perm2, fsname2, cephfs_mntpt2))
    """
    def _unpack_tuple(c):
        if len(c) == 2:
            perm, fsname, cephfs_mntpt = c[0], c[1], '/'
        elif len(c) == 3:
            perm, fsname, cephfs_mntpt = c
        elif len(c) < 2:
            raise RuntimeError('received items are too less in caps')
        else: # len(c) > 3
            raise RuntimeError('received items are too many in caps')

        return perm, fsname, cephfs_mntpt

    def _gen_mds_cap_str(c):
        perm, fsname, cephfs_mntpt = _unpack_tuple(c)
        mds_cap = f'allow {perm}'
        if fsname:
            mds_cap += f' fsname={fsname}'
        if cephfs_mntpt != '/':
            if cephfs_mntpt[0] == '/':
                cephfs_mntpt = cephfs_mntpt[1:]
            mds_cap += f' path={cephfs_mntpt}'
        return mds_cap

    if len(caps) == 1:
        return _gen_mds_cap_str(caps[0])

    mds_cap = ''
    for i, c in enumerate(caps):
        mds_cap +=  _gen_mds_cap_str(c)
        if i != len(caps) - 1:
            mds_cap += ', '

    return mds_cap


def assert_equal(first, second):
    msg = f'Variables are not equal.\nfirst = {first}\nsecond = {second}'
    assert first == second, msg


def assert_in(member, container):
    msg = dedent(f'''
        First value is absent in second value.
        First value -
        {member}
        Second value -
        {container}
        ''')
    assert member in container, msg


class CapTester:
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
            log.info(f'creating test file on FS {mount_x.cephfs_name} '
                     f'mounted at {mount_x.mountpoint}...')
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
            self.test_set.append([mount_x, filepath, filedata])
            log.info(f'Test file created at "{filepath}" with the following '
                     f'data -\n"{filedata}"')

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
        log.info(f'output of fs ls cmd run by client.{client_id} -\n{fsls}')

        if 'fsname=' not in moncap:
            log.info('no FS name is mentioned in moncap, client has '
                     'permission to list all files. moncap -\n{moncap}')
            log.info('testing for presence of all FS names in output of '
                     '"fs ls" command run by client.')

            fsls_admin = get_cluster_cmd_op(args='fs ls')
            log.info('output of fs ls cmd run by admin -\n{fsls_admin}')

            assert_equal(fsls, fsls_admin)
            return

        log.info('FS names are mentioned in moncap. moncap -\n{moncap}')
        log.info('testing for presence of these FS names in output of '
                 '"fs ls" command run by client.')
        for fsname in self._get_fsnames_from_moncap(moncap):
            fsname_cap_str = f'name: {fsname}'
            assert_in(fsname_cap_str, fsls)

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
        #   therefore, final path: /mnt/cephfs_x/testdir
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
            log.info(f'test read perm: read file {path} and expect data '
                     f'"{data}"')
            contents = mount.read_file(path)
            assert_equal(data, contents)
            log.info(f'read perm was tested successfully: "{data}" was '
                     f'successfully read from path {path}')

    def conduct_pos_test_for_write_caps(self):
        for mount, path, data in self.test_set:
            log.info(f'test write perm: try writing data "{data}" to '
                     f'file {path}.')
            mount.write_file(path=path, data=data)
            contents = mount.read_file(path=path)
            assert_equal(data, contents)
            log.info(f'write perm was tested was successfully: data '
                     f'"{data}" was successfully written to file "{path}".')

    def conduct_neg_test_for_write_caps(self, sudo_write=False):
        possible_errmsgs = ('permission denied', 'operation not permitted')
        cmdargs = ['echo', 'some random data', Raw('|')]
        cmdargs += ['sudo', 'tee'] if sudo_write else ['tee']

        # don't use data, cmd args to write are set already above.
        for mount, path, data in self.test_set:
            log.info('test absence of write perm: expect failure '
                     f'writing data to file {path}.')
            cmdargs.append(path)
            mount.negtestcmd(args=cmdargs, retval=1, errmsgs=possible_errmsgs)
            cmdargs.pop(-1)
            log.info('absence of write perm was tested successfully: '
                     f'failed to be write data to file {path}.')
