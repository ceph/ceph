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


def get_mon_cap_from_keyring(keyring):
    '''
    Extract MON cap for keyring and return it.
    '''
    moncap = None
    for line in keyring.split('\n'):
        if 'caps mon' in line:
            moncap = line[line.find(' = "') + 4 : -1]
            break
    else:
        raise RuntimeError('mon cap not found in keyring below -\n{keyring}')
    return moncap


def get_fsnames_from_moncap(moncap):
    '''
    Extract FS names from MON cap and return all of them.
    '''
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


def assert_equal(first, second):
    msg = f'Variables are not equal.\nfirst -\n{first}\nsecond -\n{second}'
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


class MonCapTester:
    '''
    Tested that MON caps are enforced on client by checking if the name of
    CephFS, which client is authorized for, is listed in output of commmand
    "ceph fs ls".

    USAGE: Create object of this class at the beginning of the test method
    and when MON cap needs to be tested, call the method run_mon_cap_tests().
    '''

    def run_mon_cap_tests(self, fs, client_id):
        """
        Check that MON cap is enforced for a client by searching for a Ceph
        FS name in output of cmd "fs ls" executed with that client's caps.

        fs is any fs object so that ceph commands can be exceuted.
        """
        get_cluster_cmd_op = fs.mon_manager.raw_cluster_cmd
        keyring = get_cluster_cmd_op(args=f'auth get client.{client_id}')
        moncap = get_mon_cap_from_keyring(keyring)
        keyring_path = fs.admin_remote.mktemp(data=keyring)

        fsls = get_cluster_cmd_op(
            args=f'fs ls --id {client_id} -k {keyring_path}')
        log.info(f'output of fs ls cmd run by client.{client_id} -\n{fsls}')

        fsnames = get_fsnames_from_moncap(moncap)
        if fsnames == []:
            log.info('no FS name is mentioned in moncap, client has '
                     'permission to list all files. moncap -\n{moncap}')
            return

        log.info('FS names are mentioned in moncap. moncap -\n{moncap}')
        log.info('testing for presence of these FS names in output of '
                 '"fs ls" command run by client.')
        for fsname in fsnames:
            fsname_cap_str = f'name: {fsname}'
            assert_in(fsname_cap_str, fsls)


class MdsCapTester:
    """
    Test that MDS caps are enforced on the client by exercising read-write
    permissions.

    USAGE: Create object of this class at the beginning of the test method
    and when MDS cap needs to be tested, call the method run_mds_cap_tests().
    """

    def __init__(self, mount=None, path=''):
        self._create_test_files(mount, path)

    def _create_test_files(self, mount, path):
        """
        Exercising 'r' and 'w' access levels on a file on CephFS mount is
        pretty routine across all tests for caps. Adding to method to write
        that file will reduce clutter in these tests.

        This methods writes a fixed data in a file with a fixed name located
        at the path passed in testpath for the given list of mounts. If
        testpath is empty, the file is created at the root of the CephFS.
        """
        # CephFS mount where read/write test will be conducted.
        self.mount = mount
        # Path where out test file located.
        self.path = self._gen_test_file_path(path)
        # Data that out test file will contain.
        self.data = self._gen_test_file_data()

        self.mount.write_file(self.path, self.data)
        log.info(f'Test file has been created on FS '
                 f'"{self.mount.cephfs_name}" at path "{self.path}" with the '
                 f'following data -\n{self.data}')

    def _gen_test_file_path(self, path=''):
        # XXX: The reason behind path[1:] below is that the path is
        # supposed to contain a path inside CephFS (which might be passed as
        # an absolute path). os.path.join() deletes all previous path
        # components when it encounters a path component starting with '/'.
        # Deleting the first '/' from the string in path ensures that
        # previous path components are not deleted by os.path.join().
        if path:
            path = path[1:] if path[0] == '/' else path
        # XXX: passing just '/' messes up os.path.join() ahead.
        if path == '/':
            path = ''

        dirname, filename = 'testdir', 'testfile'
        dirpath = os_path_join(self.mount.hostfs_mntpt, path, dirname)
        self.mount.run_shell(f'mkdir {dirpath}')
        return os_path_join(dirpath, filename)

    def _gen_test_file_data(self):
        # XXX: the reason behind adding path, cephfs_name and both
        # mntpts is to avoid a test bug where we mount cephfs1 but what
        # ends up being mounted cephfs2. since self.path and self.data are
        # identical, how would tests figure otherwise that they are
        # accessing the right filename but on wrong CephFS.
        return dedent(f'''\
            self.path = {self.path}
            cephfs_name = {self.mount.cephfs_name}
            cephfs_mntpt = {self.mount.cephfs_mntpt}
            hostfs_mntpt = {self.mount.hostfs_mntpt}''')

    def run_mds_cap_tests(self, perm, mntpt=None):
        """
        Run test for read perm and, for write perm, run positive test if it
        is present and run negative test if not.
        """
        if mntpt:
            # beacaue we want to value of mntpt from test_set.path along with
            # slash that precedes it.
            mntpt = '/' + mntpt if mntpt[0] != '/' else mntpt
            # XXX: mntpt is path inside cephfs that serves as root for current
            # mount. Therefore, this path must me deleted from self.path.
            # Example -
            #   orignal path: /mnt/cephfs_x/dir1/dir2/testdir
            #   cephfs dir serving as root for current mnt: /dir1/dir2
            #   therefore, final path: /mnt/cephfs_x/testdir
            self.path = self.path.replace(mntpt, '')

        self.conduct_pos_test_for_read_caps()

        if perm == 'rw':
            self.conduct_pos_test_for_write_caps()
        elif perm == 'r':
            self.conduct_neg_test_for_write_caps()
        else:
            raise RuntimeError(f'perm = {perm}\nIt should be "r" or "rw".')

    def conduct_pos_test_for_read_caps(self):
        log.info(f'test read perm: read file {self.path} and expect data '
                 f'"{self.data}"')
        contents = self.mount.read_file(self.path)
        assert_equal(self.data, contents)
        log.info(f'read perm was tested successfully: "{self.data}" was '
                 f'successfully read from path {self.path}')

    def conduct_pos_test_for_write_caps(self):
        log.info(f'test write perm: try writing data "{self.data}" to '
                 f'file {self.path}.')
        self.mount.write_file(path=self.path, data=self.data)
        contents = self.mount.read_file(path=self.path)
        assert_equal(self.data, contents)
        log.info(f'write perm was tested was successfully: self.data '
                 f'"{self.data}" was successfully written to file '
                 f'"{self.path}".')

    def conduct_neg_test_for_write_caps(self, sudo_write=False):
        possible_errmsgs = ('permission denied', 'operation not permitted')
        cmdargs = ['echo', 'some random data', Raw('|')]
        cmdargs += ['sudo', 'tee'] if sudo_write else ['tee']

        # don't use data, cmd args to write are set already above.
        log.info('test absence of write perm: expect failure '
                 f'writing data to file {self.path}.')
        cmdargs.append(self.path)
        self.mount.negtestcmd(args=cmdargs, retval=1, errmsgs=possible_errmsgs)
        cmdargs.pop(-1)
        log.info('absence of write perm was tested successfully: '
                 f'failed to be write data to file {self.path}.')


class CapTester(MonCapTester, MdsCapTester):
    '''
    Test MON caps as well as MDS caps. For usage see docstrings of class
    MDSCapTester.

    USAGE: Create object of this class at the beginning of the test method
    and when MON and MDS cap needs to be tested, call the method
    run_cap_tests().
    '''

    def __init__(self, mount=None, path='', create_test_files=True):
        if create_test_files:
            self._create_test_files(mount, path)

    def run_cap_tests(self, fs, client_id, perm, mntpt=None):
        self.run_mon_cap_tests(fs, client_id)
        self.run_mds_cap_tests(perm, mntpt)
