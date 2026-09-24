'''
Test subvolume auto-upgrades from subvolume v2 to subvolume v3 with and without
IO load on subvolume.
'''
from logging import getLogger
from json import loads

from tasks.cephfs.helpers.subvolumes import SubvolAutoUpgrade
from tasks.cephfs.helpers.subvolumes import  SubvolV3, SubvolV2
from tasks.cephfs.helpers.gen_io_load import GenIoLoad


log = getLogger(__name__)


class TestBasic(SubvolAutoUpgrade):
    '''
    Test subvol upgrade from v2 to v3 without IO load.
    '''

    CLIENTS_REQUIRED = 2

    def test_subvol(self):
        '''
        Test subvol upgrade from v2 to v3 when subvol is located in the
        default subvol group.
        '''
        v2_subvol = SubvolV2(self.mon_manager, self.mount_a, self.volname)
        v2_subvol.custom_create()
        v2_subvol.sanity_test_custom_made_subvol()

        # this will trigger subvol auto-upgrade
        v3_sv_path = v2_subvol.getpath().strip()
        v3 = SubvolV3(subvol=v2_subvol)
        self.assertEqual(v3_sv_path, v3.get_mnt_path())

        v3.sanity_test_meta_file()
        v3.sanity_test_subvol_ls()
        v3.sanity_test_subvol_getpath()

        v3.rm()
        self.wait_till_trash_is_empty()

    def test_custom_group_subvol(self):
        '''
        Test subvol upgrade from v2 to v3 when subvol is located in a
        non-default subvol group.
        '''
        v2_subvol = SubvolV2(self.mon_manager, self.mount_a, self.volname,
                      grp_name=True)
        v2_subvol.custom_create()
        v2_subvol.sanity_test_custom_made_subvol()

        # this will trigger subvol auto-upgrade
        v3_sv_path = v2_subvol.getpath().strip()
        v3 = SubvolV3(subvol=v2_subvol)
        self.assertEqual(v3_sv_path, v3.get_mnt_path())

        v3.sanity_test_meta_file()
        v3.sanity_test_subvol_ls()
        v3.sanity_test_subvol_getpath()

        v3.rm()
        self.wait_till_trash_is_empty()

    def test_subvol_with_v2_snap(self):
        '''
        Test subvol upgrade from v2 to v3 when it has a snap.
        '''
        v2_subvol = SubvolV2(self.mon_manager, self.mount_a, self.volname)
        v2_subvol.custom_create()
        snap_name = v2_subvol.custom_create_snap()
        v2_subvol.sanity_test_custom_made_subvol()

        # this will trigger subvol auto-upgrade
        v3_sv_path = v2_subvol.getpath().strip()
        v3 = SubvolV3(subvol=v2_subvol)
        self.assertEqual(v3_sv_path, v3.get_mnt_path())

        v3.sanity_test_meta_file()
        v3.sanity_test_subvol_ls()
        v3.sanity_test_subvol_getpath()
        v3.sanity_test_snap_ls(snap_name)
        v3.sanity_test_snap_getpath(snap_name)

        v3.snap_rm(snap_name)
        v3.rm()
        self.wait_till_trash_is_empty()


class TestWithIoLoad(SubvolAutoUpgrade):
    '''
    Test subvol upgrade from v2 to v3 while IO is being performed on the subvol.
    '''

    CLIENTS_REQUIRED = 2
    INITIAL_WAIT = 5

    def test_with_fs_client(self):
        '''
        Test subvol upgrade from v2 to v3 when subvol is located in the
        default subvol group and the subvol is under IO load.
        '''
        v2_subvol = SubvolV2(self.mon_manager, self.mount_a, self.volname)
        v2_subvol.custom_create()
        v2_subvol.sanity_test_custom_made_subvol()

        writer = GenIoLoad(v2_subvol.mnt_path, self.mount_b, 'x1',
                           self.fs.data_pool_name,
                           initial_wait=self.INITIAL_WAIT)
        writer.start()

        # this will trigger subvol auto-upgrade
        v3_sv_path = v2_subvol.getpath().strip()
        # getting v3 object
        v3 = SubvolV3(subvol=v2_subvol)
        self.assertEqual(v3_sv_path, v3.get_mnt_path())

        v3.sanity_test_meta_file()
        v3.sanity_test_subvol_ls()
        v3.sanity_test_subvol_getpath()

        # upgrade was successful, stopping client workload
        writer.stop()
        # verifying if files were actually being written on the subvol
        writer.verify_num_of_files_written()

        v3.rm()
        self.wait_till_trash_is_empty()

    def test_with_subvol_client(self):
        '''
        Test subvol upgrade from v2 to v3 when subvol is located in the default
        subvol group and is under IO load via a client that has caps only for
        the data path of the subvol that is being upgraded.
        '''
        v2_subvol = SubvolV2(self.mon_manager, self.mount_a, self.volname)
        v2_subvol.custom_create()
        v2_subvol.sanity_test_custom_made_subvol()

        writer = GenIoLoad(v2_subvol.mnt_path, self.mount_b, 'x1',
                           self.fs.data_pool_name,
                           initial_wait=self.INITIAL_WAIT)
        writer.start()

        # this will trigger subvol auto-upgrade
        v3_sv_path = v2_subvol.getpath().strip()
        # getting v3 object
        v3 = SubvolV3(subvol=v2_subvol)
        self.assertEqual(v3_sv_path, v3.get_mnt_path())

        v3.sanity_test_meta_file()
        v3.sanity_test_subvol_ls()
        v3.sanity_test_subvol_getpath()

        # upgrade was successful, stopping client workload
        writer.stop()
        # verifying if files were actually being written on the subvol
        writer.verify_num_of_files_written()

        v3.rm()
        self.wait_till_trash_is_empty()

    def test_with_subvol_client_with_snap(self):
        '''
        Test subvol upgrade from v2 to v3 when subvol is located in the default
        subvol group, contains a snapshot and is under IO load via a client that
        has caps only for the data path of the subvolume that is being upgraded.
        '''
        v2_subvol = SubvolV2(self.mon_manager, self.mount_a, self.volname)
        v2_subvol.custom_create()
        snap_name = v2_subvol.custom_create_snap()
        v2_subvol.sanity_test_custom_made_subvol()
        v2_subvol.sanity_test_custom_made_snap(snap_name)

        writer = GenIoLoad(v2_subvol.mnt_path, self.mount_b, 'x1',
                           self.fs.data_pool_name,
                           initial_wait=self.INITIAL_WAIT)
        writer.start()

        # this will trigger subvol auto-upgrade
        v3_sv_path = v2_subvol.getpath().strip()
        # getting v3 object
        v3 = SubvolV3(subvol=v2_subvol)
        self.assertEqual(v3_sv_path, v3.get_mnt_path())

        # XXX confirm that writer threads haven't crash due to upgrade
        msg = ('thread writing on subvol via client crashed while subvol '
               f'{v3.name} was being upgraded from v2 to v3')
        self.assertEqual(writer.is_alive(), True, msg)

        v3.sanity_test_meta_file()
        v3.sanity_test_subvol_ls()
        v3.sanity_test_subvol_getpath()
        v3.sanity_test_snap_ls(snap_name)
        v3.sanity_test_snap_getpath(snap_name)

        # upgrade was successful, stopping client workload
        writer.stop()
        # verifying if files were actually being written on the subvol
        writer.verify_num_of_files_written()

        v3.snap_rm(snap_name)
        v3.rm()
        self.wait_till_trash_is_empty()
