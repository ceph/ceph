"""
Helper methods to test that MON and MDS caps are enforced properly.
"""
import logging

from tasks.cephfs.cephfs_test_case import CephFSTestCase

from teuthology.orchestra.run import Raw

log = logging.getLogger(__name__)

class CapsHelper(CephFSTestCase):

    def run_mon_cap_tests(self, moncap, keyring):
        keyring_path = self.fs.admin_remote.mktemp(data=keyring)

        fsls = self.get_ceph_cmd_stdout(f'fs ls --id {self.client_id} -k '
                                        f'{keyring_path}')

        # we need to check only for default FS when fsname clause is absent
        # in MON/MDS caps
        if 'fsname' not in moncap:
            self.assertIn(self.fs.name, fsls)
            return

        fss = (self.fs1.name, self.fs2.name) if hasattr(self, 'fs1') else \
            (self.fs.name,)
        for fsname in fss:
                if fsname in moncap:
                    self.assertIn('name: ' + fsname, fsls)
                else:
                    self.assertNotIn('name: ' + fsname, fsls)

    def run_mds_cap_tests(self, filepaths, filedata, mounts, perm):
        self.conduct_pos_test_for_read_caps(filepaths, filedata, mounts)
        self.conduct_pos_test_for_open_caps(filepaths, filedata, mounts)

        if perm == 'rw':
            self.conduct_pos_test_for_write_caps(filepaths, mounts)
        elif perm == 'r':
            self.conduct_neg_test_for_chown_caps(filepaths, filedata, mounts)
            self.conduct_neg_test_for_truncate_caps(filepaths, filedata, mounts)
            self.conduct_neg_test_for_write_caps(filepaths, mounts)
        else:
            raise RuntimeError(f'perm = {perm}\nIt should be "r" or "rw".')

    def conduct_pos_test_for_read_caps(self, filepaths, filedata, mounts, sudo_read=False):
        for mount in mounts:
            for path, data in zip(filepaths, filedata):
                # XXX: conduct tests only if path belongs to current mount; in
                # teuth tests client are located on same machines.
                if path.find(mount.hostfs_mntpt) != -1:
                    contents = mount.read_file(path, sudo_read)
                    self.assertEqual(data, contents)

    def conduct_pos_test_for_write_caps(self, filepaths, mounts):
        filedata = ('some new data on first fs', 'some new data on second fs')

        for mount in mounts:
            for path, data in zip(filepaths, filedata):
                if path.find(mount.hostfs_mntpt) != -1:
                    # test that write was successful
                    mount.write_file(path=path, data=data)
                    # verify that contents written was same as the one that was
                    # intended
                    contents1 = mount.read_file(path=path)
                    self.assertEqual(data, contents1)

    def conduct_neg_test_for_write_caps(self, filepaths, mounts):
        cmdargs = ['echo', 'some random data', Raw('|'), 'tee']

        for mount in mounts:
            for path in filepaths:
                if path.find(mount.hostfs_mntpt) != -1:
                    cmdargs.append(path)
                    mount.negtestcmd(args=cmdargs, retval=1,
                                     errmsg='permission denied')

    def get_mon_cap_from_keyring(self, client_name):
        keyring = self.get_ceph_cmd_stdout(f'auth get {client_name}')
        for line in keyring.split('\n'):
            if 'caps mon' in line:
                return line[line.find(' = "') + 4 : -1]

        raise RuntimeError('get_save_mon_cap: mon cap not found in keyring. '
                           'keyring -\n' + keyring)

    def _conduct_neg_test_for_root_squash_caps(self, _cmdargs, filepaths, filedata, mounts, sudo_write=False):
        cmdargs = ['sudo'] if sudo_write else ['']
        cmdargs += _cmdargs

        for mount in mounts:
            for path in filepaths:
                log.info(f'test absence of {_cmdargs[0]} perm: expect failure {path}.')

                # open the file and hold it. The MDS will issue CEPH_CAP_EXCL_*
                # to mount
                proc = mount.open_background(path, write=False)

                cmdargs.append(path)
                mount.negtestcmd(args=cmdargs, retval=1, errmsg="permission denied")
                cmdargs.pop(-1)

                mount._kill_background(proc)

                log.info(f'absence of {_cmdargs[0]} perm was tested successfully')

    def conduct_neg_test_for_chown_caps(self, filepaths, filedata, mounts, sudo_write=True):
        # flip ownership to nobody. assumption: nobody's id is 65534
        cmdargs = ['chown', '-h', '65534:65534']
        self._conduct_neg_test_for_root_squash_caps(cmdargs, filepaths, filedata, mounts, sudo_write)

    def conduct_neg_test_for_truncate_caps(self, filepaths, filedata, mounts, sudo_write=True):
        cmdargs = ['truncate', '-s', '10GB']
        self._conduct_neg_test_for_root_squash_caps(cmdargs, filepaths, filedata, mounts, sudo_write)

    def conduct_pos_test_for_open_caps(self, filepaths, filedata, mounts, sudo_read=True):
        self.conduct_pos_test_for_read_caps(filepaths, filedata, mounts, sudo_read)
