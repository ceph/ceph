import errno

from io import StringIO
from time import sleep
from logging import getLogger

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.caps_helper import (CapTester, gen_mon_cap_str,
                                      gen_osd_cap_str, gen_mds_cap_str)


log = getLogger(__name__)


class SwapHelper(CephFSTestCase):
    '''
    Helper methods for testing "ceph fs swap" command.
    '''

    MDSS_REQUIRED = 3
    CLIENTS_REQUIRED = 2
    client_id = 'testuser'
    client_name = f'client.{client_id}'

    def setUp(self):
        super(SwapHelper, self).setUp()
        self.fs1 = self.fs
        self.fs2 = self.mds_cluster.newfs(name='testcephfs2', create=True)
        self.fs1.getinfo()
        self.fs2.getinfo()
        self.orig_fs_id_name = {self.fs1.id: self.fs1.name,
                                self.fs2.id: self.fs2.name}

        self.mount_a.remount(cephfs_name=self.fs1.name)
        self.mount_b.remount(cephfs_name=self.fs2.name)

        self.captesters = (CapTester(self.mount_a), CapTester(self.mount_b))

        self.mount_a.umount_wait()
        self.mount_b.umount_wait()

    def tearDown(self):
        self.mount_a.umount_wait()
        self.mount_b.umount_wait()
        self.run_ceph_cmd(args=f'auth rm {self.client_name}')
        super(SwapHelper, self).tearDown()

    def _reauthorize_client(self):
        moncap = gen_mon_cap_str((("rw", self.fs1.name),
                                  ("rw", self.fs2.name)))
        osdcap = gen_osd_cap_str((("rw", self.fs1.name),
                                  ("rw", self.fs2.name)))
        mdscap = gen_mds_cap_str((("rw", self.fs1.name),
                                  ("rw", self.fs2.name)))
        self.run_ceph_cmd(args=f'auth add {self.client_name} mon "{moncap}" '
                               f'osd "{osdcap}" mds "{mdscap}"')

    def _remount_both_cephfss(self):
        keyring = self.fs.mon_manager.get_keyring(self.client_id) + '\n'
        keyring_path_a = self.mount_a.client_remote.mktemp(data=keyring)
        keyring_path_b = self.mount_b.client_remote.mktemp(data=keyring)

        self.mount_a.mount(client_id=self.client_id,
                           client_keyring_path=keyring_path_a,
                           cephfs_mntpt='/', cephfs_name=self.fs1.name)
        self.mount_b.mount(client_id=self.client_id,
                           client_keyring_path=keyring_path_b,
                           cephfs_mntpt='/', cephfs_name=self.fs2.name)

    def run_rw_tests(self):
        for captester in self.captesters:
            captester.conduct_pos_test_for_read_caps()
            captester.conduct_pos_test_for_write_caps()

    def check_pool_application_metadata_key_value(self, pool, app, key, value):
        output = self.get_ceph_cmd_stdout(
            'osd', 'pool', 'application', 'get', pool, app, key)
        self.assertEqual(str(output.strip()), value)

    def _check_fs_name_on_fs_pools(self, fss):
        for fs in fss:
            for pool in fs.get_data_pool_names(True):
                self.check_pool_application_metadata_key_value(pool, 'cephfs',
                    'data', fs.name)
            self.check_pool_application_metadata_key_value(
                fs.get_metadata_pool_name(), 'cephfs', 'metadata', fs.name)

    def _are_fsnames_and_fscids_together(self):
        '''
        Are FS names and FSIDs together on same the FS as they were before
        running "ceph fs swap" command?
        '''
        fs1_id_swapped = self.orig_fs_id_name[self.fs1.id] == self.fs1.name
        fs2_id_swapped = self.orig_fs_id_name[self.fs2.id] == self.fs2.name

        if fs1_id_swapped and fs2_id_swapped:
            return True
        elif not fs1_id_swapped and not fs2_id_swapped:
            return False
        else:
            raise RuntimeError(
                'Unexpected situation occured: FSID for one FS swapped but '
                'not for the other FS.')

    def _bring_both_cephfss_down(self):
        self.run_ceph_cmd(f'fs fail {self.fs1.name}')
        self.run_ceph_cmd(f'fs fail {self.fs2.name}')

    def _bring_both_cephfss_up(self):
        self.run_ceph_cmd(f'fs set {self.fs1.name} joinable true')
        self.run_ceph_cmd(f'fs set {self.fs2.name} joinable true')

    def _refuse_clients_for_both_cephfss(self):
        self.run_ceph_cmd(f'fs set {self.fs1.name} refuse_client_session true')
        self.run_ceph_cmd(f'fs set {self.fs2.name} refuse_client_session true')

    def _accept_clients_for_both_cephfss(self):
        self.run_ceph_cmd(f'fs set {self.fs1.name} refuse_client_session '
                           'false')
        self.run_ceph_cmd(f'fs set {self.fs2.name} refuse_client_session '
                           'false')


class TestSwap(SwapHelper):
    '''
    Tests for "ceph fs swap" command.
    '''

    def test_swap_fsnames_but_not_fscids(self):
        '''
        Test that "ceph fs swap --swap-fscids=no" swaps the FS names but not
        the FSCIDs.
        '''
        self._bring_both_cephfss_down()
        self._refuse_clients_for_both_cephfss()
        sleep(2)
        # log output to help debug test failures
        self.run_ceph_cmd('fs', 'dump')

        self.run_ceph_cmd(f'fs swap {self.fs1.name} {self.fs1.id} '
                          f'{self.fs2.name} {self.fs2.id} --swap-fscids=no '
                           '--yes_i_really_mean_it')

        self._bring_both_cephfss_up()
        self._accept_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        self.fs1.getinfo()
        self.fs2.getinfo()
        self._reauthorize_client()
        self._remount_both_cephfss()

        # FS names were swapped but not FSIDs, so both can't be together
        self.assertEqual(self._are_fsnames_and_fscids_together(), False)
        self._check_fs_name_on_fs_pools((self.fs1, self.fs2))
        self.run_rw_tests()

    def test_swap_fsnames_and_fscids(self):
        '''
        Test that "ceph fs swap --swap-fscids=yes" swaps the FS names as well
        as the FSCIDs.
        '''
        self._bring_both_cephfss_down()
        self._refuse_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        self.run_ceph_cmd(f'fs swap {self.fs1.name} {self.fs1.id} '
                          f'{self.fs2.name} {self.fs2.id} --swap-fscids=yes '
                           '--yes_i_really_mean_it')

        self._bring_both_cephfss_up()
        self._accept_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        # XXX: Let's ensure that FS mounted on a mountpoint is same before
        # and after swapping of FS name and FSCIDs. This ensures that data
        # available on mountpoints before and after the swap is same. This
        # prevents self.run_rw_tests() from breaking.
        #
        # At the beginning of test, testcephfs1 has data (let's say) 'abc1'
        # and testcephfs2 has data 'abc2'. self.fs1 is mapped to testcephfs1
        # and self.fs2 mapped to testcephfs2. After swap, data of testcephfs1
        # and testcephfs2 will be 'abc2' and 'abc1' respectively.
        #
        # However, running self.fs1.getinfo() after swap will map self.fs1 to
        # FS with FSCID 1 i.e. testcephfs1 and not testcephfs2. Thus, data
        # under self.fs1 will be different than data before swapping. This
        # breaks self.run_rw_tests() because self.fs1 is always mounted on
        # the mountpoint of self.mount_a.

        # To prevent this, therefore, make sure that data on
        # self.fs1/self.mount_a is same after and before the swap. To ensure
        # this, swap FS that is represented by self.fs1. Instead of
        # testcephfs1 it should be testcephfs2 because, after swap,
        # testcephfs2 containts the data of testcephfs1. This will ensure that
        # self.mount_rw_tests doesn't break.
        #
        # Same for self.fs2.
        self.fs1.id, self.fs2.id = None, None
        self.fs1.name, self.fs2.name = self.fs2.name, self.fs1.name
        self.fs1.getinfo()
        self.fs2.getinfo()
        self._reauthorize_client()
        self._remount_both_cephfss()

        # both FS name and FSIDs were swapped, so both must be together
        self.assertEqual(self._are_fsnames_and_fscids_together(), True)
        self._check_fs_name_on_fs_pools((self.fs1, self.fs2))
        self.run_rw_tests()

    def test_swap_without_confirmation_option(self):
        '''
        Test that "ceph fs swap --swap-fscids=yes" without the option
        "--yes-i-really-mean-it" fails.
        '''
        self._bring_both_cephfss_down()
        self._refuse_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        msg = ('This is a potentially disruptive operation, client\'s cephx '
               'credentials may need to be reauthorized to access the file '
               'systems and its pools. Add --yes-i-really-mean-it if you are '
               'sure you wish to continue.')
        self.negtest_ceph_cmd(f'fs swap {self.fs1.name} {self.fs1.id} '
                              f'{self.fs2.name} {self.fs2.id} '
                               '--swap-fscids=no',
            retval=errno.EPERM, errmsgs=msg)

        self._bring_both_cephfss_up()
        self._accept_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        self.fs1.getinfo()
        self.fs2.getinfo()
        self._reauthorize_client()
        self._remount_both_cephfss()

        # check that content of both CephFSs is unaffected by this failure.
        self.run_rw_tests()
        self._check_fs_name_on_fs_pools((self.fs1, self.fs2))
        # neither FS name nor FSIDs were swapped, so both must be together
        self.assertEqual(self._are_fsnames_and_fscids_together(), True)


class TestSwapFsAbsent(SwapHelper):
    '''
    Tests for "fs swap" when either FS name is false.
    '''

    def test_swap_when_fs1_is_absent(self):
        '''
        Test that "ceph fs swap <fs1name> <fs1id> <fs2name> <fs2id>" fails
        when there is no CephFS on cluster by the name "<fs1name>".
        '''
        self._bring_both_cephfss_down()
        self._refuse_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        absent_cephfs = 'random_fsname_654'
        msg = (f"File system '{absent_cephfs}' doesn't exist on this Ceph "
                "cluster")
        self.negtest_ceph_cmd(
            args=(f'fs swap {absent_cephfs} {self.fs1.id} {self.fs2.name} '
                  f'{self.fs2.id} --swap-fscids=no --yes_i_really_mean_it'),
            retval=errno.ENOENT, errmsgs=msg)

        self._bring_both_cephfss_up()
        self._accept_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        self.fs1.getinfo()
        self.fs2.getinfo()
        self._reauthorize_client()
        self._remount_both_cephfss()

        # check that content of both CephFSs is unaffected by this failure.
        self.run_rw_tests()
        self._check_fs_name_on_fs_pools((self.fs1, self.fs2))
        # neither FS name nor FSIDs were swapped, so both must be together
        self.assertEqual(self._are_fsnames_and_fscids_together(), True)

    def test_swap_when_fs2_is_absent(self):
        '''
        Test that "ceph fs swap <fs1name> <fs1id> <fs2name> <fs2id>" fails
        when there is no CephFS on cluster by the name "<fs2name>".
        '''
        self._bring_both_cephfss_down()
        self._refuse_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        absent_cephfs = 'random_fsname_654'
        msg = (f"File system '{absent_cephfs}' doesn't exist on this Ceph "
                "cluster")
        self.negtest_ceph_cmd(
            args=(f'fs swap {self.fs1.name} {self.fs2.id} {absent_cephfs} '
                  f'{self.fs2.id} --swap-fscids=no --yes_i_really_mean_it'),
            retval=errno.ENOENT, errmsgs=msg)

        self._bring_both_cephfss_up()
        self._accept_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        self.fs1.getinfo()
        self.fs2.getinfo()
        self._reauthorize_client()
        self._remount_both_cephfss()

        # check that content of both CephFSs is unaffected by this failure.
        self.run_rw_tests()
        self._check_fs_name_on_fs_pools((self.fs1, self.fs2))
        self.assertEqual(self._are_fsnames_and_fscids_together(), True)

    def test_swap_when_both_fss_are_absent(self):
        '''
        Test that "ceph fs swap <fs1name> <fs1id> <fs2name> <fs2id>" fails
        when there are no CephFSs on the cluster by the name "<fs1name>" and
        "<fs2name>".
        '''
        self._bring_both_cephfss_down()
        self._refuse_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        absent_cephfs1 = 'random_fsname_65'
        absent_cephfs2 = 'random_fsname_66'
        msg = (f"Neither file system '{absent_cephfs1}' nor file system "
               f"'{absent_cephfs2}' exists on this Ceph cluster")
        self.negtest_ceph_cmd(
            args=(f'fs swap {absent_cephfs1} 123 {absent_cephfs2} 1234 '
                   '--swap-fscids=no --yes_i_really_mean_it'),
            retval=errno.ENOENT, errmsgs=msg)

        self._bring_both_cephfss_up()
        self._accept_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        self.fs1.getinfo()
        self.fs2.getinfo()
        self._reauthorize_client()
        self._remount_both_cephfss()

        # check that content of both CephFSs is unaffected by this failure.
        self.run_rw_tests()
        self._check_fs_name_on_fs_pools((self.fs1, self.fs2))
        # neither FS name nor FSIDs were swapped, so both must be together
        self.assertEqual(self._are_fsnames_and_fscids_together(), True)


class TestSwapFscidWrong(SwapHelper):
    '''
    Tests for "fs swap" when either FSCID is wrong.
    '''

    def test_swap_when_fs1_id_is_wrong(self):
        '''
        Test that "ceph fs swap <fs1name> <fs1id> <fs2name> <fs2id>" fails
        when "<fs1id>" is not the FSCID of the CephFS named "<fs1nmae>".
        '''
        self._bring_both_cephfss_down()
        self._refuse_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        msg = (f"FSCID provided for '{self.fs1.name}' is incorrect.")
        self.negtest_ceph_cmd(
            args=(f'fs swap {self.fs1.name} 123 {self.fs2.name} '
                  f'{self.fs2.id} --swap-fscids=no --yes_i_really_mean_it'),
            retval=errno.EINVAL, errmsgs=msg)

        self._bring_both_cephfss_up()
        self._accept_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        self.fs1.getinfo()
        self.fs2.getinfo()
        self._reauthorize_client()
        self._remount_both_cephfss()

        # check that content of both CephFSs is unaffected by this failure.
        self.run_rw_tests()
        self._check_fs_name_on_fs_pools((self.fs1, self.fs2))
        # neither FS name nor FSIDs were swapped, so both must be together
        self.assertEqual(self._are_fsnames_and_fscids_together(), True)

    def test_swap_when_fs2_id_is_wrong(self):
        '''
        Test that "ceph fs swap <fs1name> <fs1id> <fs2name> <fs2id>" fails
        when "<fs2id>" is not the FSCID of the CephFS named "<fs2nmae>".
        '''
        self._bring_both_cephfss_down()
        self._bring_both_cephfss_down()
        self._refuse_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        msg = (f"FSCID provided for '{self.fs2.name}' is incorrect.")
        self.negtest_ceph_cmd(
            args=(f'fs swap {self.fs1.name} {self.fs1.id} {self.fs2.name} '
                  f'123 --swap-fscids=no --yes_i_really_mean_it'),
            retval=errno.EINVAL, errmsgs=msg)

        self._bring_both_cephfss_up()
        self._accept_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        self.fs1.getinfo()
        self.fs2.getinfo()
        self._reauthorize_client()
        self._remount_both_cephfss()

        # check that content of both CephFSs is unaffected by this failure.
        self.run_rw_tests()
        self._check_fs_name_on_fs_pools((self.fs1, self.fs2))
        self.assertEqual(self._are_fsnames_and_fscids_together(), True)

    def test_swap_when_both_fscids_are_wrong(self):
        '''
        Test that "ceph fs swap <fs1name> <fs1id> <fs2name> <fs2id>" fails
        when "<fs1id>" and "<fs2id>", respectively, are not the FSCIDs of the
        CephFSs named "<fs1name>" and "<fs2nmae>".
        '''
        self._bring_both_cephfss_down()
        self._refuse_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        msg = ('FSCIDs provided for both the CephFSs is incorrect.')
        self.negtest_ceph_cmd(
            args=(f'fs swap {self.fs1.name} 123 {self.fs2.name} 1234 '
                  f'--swap-fscids=no --yes_i_really_mean_it'),
            retval=errno.EINVAL, errmsgs=msg)

        self._bring_both_cephfss_up()
        self._accept_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        self.fs1.getinfo()
        self.fs2.getinfo()
        self._reauthorize_client()
        self._remount_both_cephfss()

        # check that content of both CephFSs is unaffected by this failure.
        self.run_rw_tests()
        self._check_fs_name_on_fs_pools((self.fs1, self.fs2))
        # neither FS name nor FSIDs were swapped, so both must be together
        self.assertEqual(self._are_fsnames_and_fscids_together(), True)

    def test_swap_when_user_swaps_fscids_in_cmd_args(self):
        '''
        Test that "ceph fs swap" fails and prints relevant error message when
        FSCIDs are exchange while writing the command. That is user write the
        command as -

        "ceph fs swap <fs1name> <fs2id> <fs2name> <fs1id>"

        instead of writing -

        "ceph fs swap <fs1name> <fs1id> <fs2name> <fs2id>"
        '''
        self._bring_both_cephfss_down()
        self._refuse_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        msg = ('FSCIDs provided in command arguments are swapped; perhaps '
               '`ceph fs swap` has been run before.')
        proc = self.run_ceph_cmd(
            args=(f'fs swap {self.fs1.name} {self.fs2.id} {self.fs2.name} '
                  f'{self.fs1.id} --swap-fscids=no --yes_i_really_mean_it'),
            stderr=StringIO())
        self.assertIn(msg.lower(), proc.stderr.getvalue().lower())

        self._bring_both_cephfss_up()
        self._accept_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        self.fs1.getinfo()
        self.fs2.getinfo()
        self._reauthorize_client()
        self._remount_both_cephfss()

        # check that content of both CephFSs is unaffected by this failure.
        self.run_rw_tests()
        self._check_fs_name_on_fs_pools((self.fs1, self.fs2))
        # neither FS name nor FSIDs were swapped, so both must be together
        self.assertEqual(self._are_fsnames_and_fscids_together(), True)


class TestSwapMirroringOn(SwapHelper):
    '''
    # Tests for "fs swap" when mirroring is enabled on FS
    '''

    def test_swap_when_mirroring_enabled_for_1st_FS(self):
        '''
        Test that "ceph fs swap <fs1name> <fs1id> <fs2name> <fs2id>" fails
        when mirroring is enabled for the CephFS named "<fs1name>".
        '''
        self.run_ceph_cmd(f'fs mirror enable {self.fs1.name}')
        self._bring_both_cephfss_down()
        self._refuse_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        msg = (f"Mirroring is enabled on file system '{self.fs1.name}'. "
                "Disable mirroring on the file system after ensuring it's "
                "OK to do so, and then re-try swapping.")
        self.negtest_ceph_cmd(
            args=(f'fs swap {self.fs1.name} {self.fs1.id} {self.fs2.name} '
                  f'{self.fs2.id} --swap-fscids=no --yes_i_really_mean_it'),
            retval=errno.EPERM, errmsgs=msg)

        self._bring_both_cephfss_up()
        self._accept_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')
        self.run_ceph_cmd(f'fs mirror disable {self.fs1.name}')

        self.fs1.getinfo()
        self.fs2.getinfo()
        self._reauthorize_client()
        self._remount_both_cephfss()

        # check that content of both CephFSs is unaffected by this failure.
        self.run_rw_tests()
        self._check_fs_name_on_fs_pools((self.fs1, self.fs2))
        # neither FS name nor FSIDs were swapped, so both must be together
        self.assertEqual(self._are_fsnames_and_fscids_together(), True)

    def test_swap_when_mirroring_enabled_for_2nd_FS(self):
        '''
        Test that "ceph fs swap <fs1name> <fs1id> <fs2name> <fs2id>" fails
        when mirroring is enabled for the CephFS named "<fs2name>".
        '''
        self.run_ceph_cmd(f'fs mirror enable {self.fs2.name}')
        self._bring_both_cephfss_down()
        self._refuse_clients_for_both_cephfss()
        sleep(2)

        self.run_ceph_cmd('fs', 'dump')
        msg = (f"Mirroring is enabled on file system '{self.fs2.name}'. "
                "Disable mirroring on the file system after ensuring it's "
                "OK to do so, and then re-try swapping.")
        self.negtest_ceph_cmd(
            args=(f'fs swap {self.fs1.name} {self.fs1.id} {self.fs2.name} '
                  f'{self.fs2.id} --swap-fscids=no --yes_i_really_mean_it'),
            retval=errno.EPERM, errmsgs=msg)

        self._bring_both_cephfss_up()
        self._accept_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')
        self.run_ceph_cmd(f'fs mirror disable {self.fs2.name}')

        self.fs1.getinfo()
        self.fs2.getinfo()
        self._reauthorize_client()
        self._remount_both_cephfss()

        # check that content of both CephFSs is unaffected by this failure.
        self.run_rw_tests()
        self._check_fs_name_on_fs_pools((self.fs1, self.fs2))
        # neither FS name nor FSIDs were swapped, so both must be together
        self.assertEqual(self._are_fsnames_and_fscids_together(), True)

    def test_swap_when_mirroring_enabled_for_both_FSs(self):
        '''
        Test that "ceph fs swap <fs1name> <fs1id> <fs2name> <fs2id>" fails
        when mirroring is enabled for both the CephFSs.
        '''
        self.run_ceph_cmd(f'fs mirror enable {self.fs1.name}')
        self.run_ceph_cmd(f'fs mirror enable {self.fs2.name}')
        self._bring_both_cephfss_down()
        self._refuse_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        msg = (f"Mirroring is enabled on file systems '{self.fs1.name}' and "
               f"'{self.fs2.name}'. Disable mirroring on both the file "
                "systems after ensuring it's OK to do so, and then re-try "
                "swapping.")
        self.negtest_ceph_cmd(
            args=(f'fs swap {self.fs1.name} {self.fs1.id} {self.fs2.name} '
                  f'{self.fs2.id} --swap-fscids=no --yes_i_really_mean_it'),
            retval=errno.EPERM, errmsgs=msg)

        self._bring_both_cephfss_up()
        self._accept_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')
        self.run_ceph_cmd(f'fs mirror disable {self.fs1.name}')
        self.run_ceph_cmd(f'fs mirror disable {self.fs2.name}')

        self.fs1.getinfo()
        self.fs2.getinfo()
        self._reauthorize_client()
        self._remount_both_cephfss()

        # check that content of both CephFSs is unaffected by this failure.
        self.run_rw_tests()
        self._check_fs_name_on_fs_pools((self.fs1, self.fs2))
        # neither FS name nor FSIDs were swapped, so both must be together
        self.assertEqual(self._are_fsnames_and_fscids_together(), True)


class TestSwapFsOnline(SwapHelper):
    '''
    Tests for "fs swap" when either FS is not down/failed.
    '''

    def test_swap_when_fs1_is_online(self):
        '''
        Test that "ceph fs swap <fs1name> <fs1id> <fs2name> <fs2id>" when
        CephFS named "<fs1name>" is online (i.e. is not failed).
        '''
        self.run_ceph_cmd(f'fs fail {self.fs2.name}')
        self._refuse_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        msg = (f"CephFS '{self.fs1.name}' is not offline. Before swapping "
                "CephFS names, both CephFSs should be marked as failed. "
                "See `ceph fs fail`.")
        self.negtest_ceph_cmd(
            args=(f'fs swap {self.fs1.name} {self.fs1.id} {self.fs2.name} '
                  f'{self.fs2.id} --swap-fscids=no --yes_i_really_mean_it'),
            retval=errno.EPERM, errmsgs=msg)

        self.run_ceph_cmd(f'fs set {self.fs2.name} joinable true')
        self._accept_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        self.fs1.getinfo()
        self.fs2.getinfo()
        self._reauthorize_client()
        self._remount_both_cephfss()

        # check that content of both CephFSs is unaffected by this failure.
        self.run_rw_tests()
        self._check_fs_name_on_fs_pools((self.fs1, self.fs2))
        # neither FS name nor FSIDs were swapped, so both must be together
        self.assertEqual(self._are_fsnames_and_fscids_together(), True)

    def test_swap_when_fs2_is_online(self):
        '''
        Test that "ceph fs swap <fs1name> <fs1id> <fs2name> <fs2id>" when
        CephFS named "<fs2name>" is online (i.e. is not failed).
        '''
        self.run_ceph_cmd(f'fs fail {self.fs1.name}')
        self._refuse_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        msg = (f"CephFS '{self.fs2.name}' is not offline. Before swapping "
                "CephFS names, both CephFSs should be marked as failed. "
                "See `ceph fs fail`.")
        self.negtest_ceph_cmd(
            args=(f'fs swap {self.fs1.name} {self.fs1.id} {self.fs2.name} '
                  f'{self.fs2.id} --swap-fscids=no --yes_i_really_mean_it'),
            retval=errno.EPERM, errmsgs=msg)

        self.run_ceph_cmd(f'fs set {self.fs1.name} joinable true')
        self._accept_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        self.fs1.getinfo()
        self.fs2.getinfo()
        self._reauthorize_client()
        self._remount_both_cephfss()

        # check that content of both CephFSs is unaffected by this failure.
        self.run_rw_tests()
        self._check_fs_name_on_fs_pools((self.fs1, self.fs2))
        # neither FS name nor FSIDs were swapped, so both must be together
        self.assertEqual(self._are_fsnames_and_fscids_together(), True)

    def test_swap_when_both_FSs_are_online(self):
        '''
        Test that "ceph fs swap <fs1name> <fs1id> <fs2name> <fs2id>" when
        both the CephFSs are online (i.e. is not failed).
        '''
        self._refuse_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        msg = (f"CephFSs '{self.fs1.name}' and '{self.fs2.name}' are not "
                "offline. Before swapping CephFS names, both CephFSs should "
                "be marked as failed. See `ceph fs fail`.")
        self.negtest_ceph_cmd(
            args=(f'fs swap {self.fs1.name} {self.fs1.id} {self.fs2.name} '
                  f'{self.fs2.id} --swap-fscids=no --yes_i_really_mean_it'),
            retval=errno.EPERM, errmsgs=msg)

        self._accept_clients_for_both_cephfss()
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        self.fs1.getinfo()
        self.fs2.getinfo()
        self._reauthorize_client()
        self._remount_both_cephfss()

        # check that content of both CephFSs is unaffected by this failure.
        self.run_rw_tests()
        self._check_fs_name_on_fs_pools((self.fs1, self.fs2))
        # neither FS name nor FSIDs were swapped, so both must be together
        self.assertEqual(self._are_fsnames_and_fscids_together(), True)


class TestSwapDoesntRefuseClients(SwapHelper):
    '''
    Tests for "fs swap" when either FS is offline.
    '''

    def test_swap_when_FS1_doesnt_refuse_clients(self):
        '''
        Test that the command "ceph fs swap" command fails when
        "refuse_client_session" is not set for the first of the two of FSs .
        '''
        self._bring_both_cephfss_down()
        self.run_ceph_cmd(f'fs set {self.fs2.name} refuse_client_session true')
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        msg = (f"CephFS '{self.fs1.name}' doesn't refuse clients. Before "
                "swapping CephFS names, flag 'refuse_client_session' must "
                "be set. See `ceph fs set`.")
        self.negtest_ceph_cmd(
            args=(f'fs swap {self.fs1.name} {self.fs1.id} {self.fs2.name} '
                  f'{self.fs2.id} --swap-fscids=no --yes_i_really_mean_it'),
            retval=errno.EPERM, errmsgs=msg)

        self._bring_both_cephfss_up()
        self.run_ceph_cmd(f'fs set {self.fs2.name} refuse_client_session '
                           'false')
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        self.fs1.getinfo()
        self.fs2.getinfo()
        self._reauthorize_client()
        self._remount_both_cephfss()

        # check that content of both CephFSs is unaffected by this failure.
        self.run_rw_tests()
        self._check_fs_name_on_fs_pools((self.fs1, self.fs2))
        # neither FS name nor FSIDs were swapped, so both must be together
        self.assertEqual(self._are_fsnames_and_fscids_together(), True)

    def test_swap_when_FS2_doesnt_refuse_clients(self):
        '''
        Test that the command "ceph fs swap" command fails when
        "refuse_client_session" is not set for the second of the two of FSs .
        '''
        self._bring_both_cephfss_down()
        self.run_ceph_cmd(f'fs set {self.fs1.name} refuse_client_session true')
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        msg = (f"CephFS '{self.fs2.name}' doesn't refuse clients. Before "
                "swapping CephFS names, flag 'refuse_client_session' must "
                "be set. See `ceph fs set`.")
        self.negtest_ceph_cmd(
            args=(f'fs swap {self.fs1.name} {self.fs1.id} {self.fs2.name} '
                  f'{self.fs2.id} --swap-fscids=no --yes_i_really_mean_it'),
            retval=errno.EPERM, errmsgs=msg)

        self._bring_both_cephfss_up()
        self.run_ceph_cmd(f'fs set {self.fs1.name} refuse_client_session '
                           'false')
        sleep(2)
        self.run_ceph_cmd('fs', 'dump')

        self.fs1.getinfo()
        self.fs2.getinfo()
        self._reauthorize_client()
        self._remount_both_cephfss()

        # check that content of both CephFSs is unaffected by this failure.
        self.run_rw_tests()
        self._check_fs_name_on_fs_pools((self.fs1, self.fs2))
        # neither FS name nor FSIDs were swapped, so both must be together
        self.assertEqual(self._are_fsnames_and_fscids_together(), True)

    def test_swap_when_both_FSs_do_not_refuse_clients(self):
        '''
        Test that the command "ceph fs swap" command fails when
        "refuse_client_session" is not set for both the CephFSs.
        '''
        self.run_ceph_cmd('fs', 'dump')
        self._bring_both_cephfss_down()
        sleep(2)
        msg = (f"CephFSs '{self.fs1.name}' and '{self.fs2.name}' do not "
                "refuse clients. Before swapping CephFS names, flag "
                "'refuse_client_session' must be set. See `ceph fs set`.")
        self.negtest_ceph_cmd(
            args=(f'fs swap {self.fs1.name} {self.fs1.id} {self.fs2.name} '
                  f'{self.fs2.id} --swap-fscids=no --yes_i_really_mean_it'),
            retval=errno.EPERM, errmsgs=msg)
        self._bring_both_cephfss_up()
        self.run_ceph_cmd('fs', 'dump')
        sleep(2)

        self.fs1.getinfo()
        self.fs2.getinfo()
        self._reauthorize_client()
        self._remount_both_cephfss()

        # check that content of both CephFSs is unaffected by this failure.
        self.run_rw_tests()
        self._check_fs_name_on_fs_pools((self.fs1, self.fs2))
        # neither FS name nor FSIDs were swapped, so both must be together
        self.assertEqual(self._are_fsnames_and_fscids_together(), True)
