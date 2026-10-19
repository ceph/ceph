'''
Test subvolume auto-upgrades from subvolume v1 to subvolume v3 with and without
IO load on subvolume.
'''
from logging import getLogger
from json import loads

from tasks.cephfs.helpers.subvolumes import SubvolAutoUpgrade
from tasks.cephfs.helpers.subvolumes import  SubvolV3, SubvolV1
from tasks.cephfs.helpers.gen_io_load import GenIoLoad


log = getLogger(__name__)


class TestBasic(SubvolAutoUpgrade):
    '''
    Test subvol upgrade from v1 to v3 without IO load.
    '''

    def test_subvol(self):
        '''
        Test subvol upgrade from v1 to v3 when subvol is located in the
        default subvol group.
        '''
        v1_subvol = SubvolV1(self.mon_manager, self.admin_mount, self.volname)
        v1_subvol.custom_create()
        v1_subvol.sanity_test_custom_made_subvol()

        # XXX: TRIGGERING AUTO-UPGRADE
        v3_sv_path = v1_subvol.getpath()

        # building v3 object, nothing else.
        v3_subvol = SubvolV3(subvol=v1_subvol)
        # verify the path returned by last getpath cmd
        self.assertEqual(v3_sv_path, v3_subvol.build_and_get_mnt_path())
        v3_subvol.sanity_test_meta_file()
        v3_subvol.sanity_test_subvol_ls()
        v3_subvol.sanity_test_subvol_getpath()

        v3_subvol.rm()
        self.wait_till_trash_is_empty()

    def test_custom_group_subvol(self):
        '''
        Test subvol upgrade from v1 to v3 when subvol is located in a
        non-default subvol group.
        '''
        v1_subvol = SubvolV1(self.mon_manager, self.admin_mount, self.volname,
                      grp_name=True)
        v1_subvol.custom_create()
        v1_subvol.sanity_test_custom_made_subvol()

        # XXX: TRIGGERING AUTO-UPGRADE
        v3_sv_path = v1_subvol.getpath()

        # building v3 object, nothing more.
        v3_subvol = SubvolV3(subvol=v1_subvol)
        # verify the path returned by last getpath cmd
        self.assertEqual(v3_sv_path, v3_subvol.build_and_get_mnt_path())
        v3_subvol.sanity_test_meta_file()
        v3_subvol.sanity_test_subvol_ls()
        v3_subvol.sanity_test_subvol_getpath()

        v3_subvol.rm()
        self.wait_till_trash_is_empty()

    def test_subvol_with_v1_snap(self):
        '''
        Test subvol upgrade from v1 to v3 when it has a snap.
        '''
        v1_subvol = SubvolV1(self.mon_manager, self.admin_mount, self.volname)
        v1_subvol.custom_create()
        snap_name = v1_subvol.custom_create_snap()
        v1_subvol.sanity_test_custom_made_subvol()

        # XXX: TRIGGERING AUTO-UPGRADE
        v3_sv_path = v1_subvol.getpath()

        # building v3 object, nothing more.
        v3_subvol = SubvolV3(subvol=v1_subvol)
        # verify the path returned by last getpath cmd
        self.assertEqual(v3_sv_path, v3_subvol.build_and_get_mnt_path())
        v3_subvol.sanity_test_meta_file()
        v3_subvol.sanity_test_subvol_ls()
        v3_subvol.sanity_test_subvol_getpath()
        v3_subvol.sanity_test_snap_ls(snap_name)
        v3_subvol.sanity_test_snap_getpath(snap_name)

        v3_subvol.snap_rm(snap_name)
        v3_subvol.rm()
        self.wait_till_trash_is_empty()


class TestWithIoLoad(SubvolAutoUpgrade):
    '''
    Test subvol upgrade from v1 to v3 while IO is being performed on the subvol.
    '''

    INITIAL_WAIT = 5
    CLIENT_ID = 'x1'
    IO_CRASHED_MSG = ('writer threads crashed without causing test to fail! '
                      'this should be impossible!')

    def test_with_fs_client(self):
        '''
        Test subvol upgrade from v1 to v3 when subvol is located in the
        default subvol group and the subvol is under IO load.
        '''
        v1_subvol = SubvolV1(self.mon_manager, self.admin_mount, self.volname)
        v1_subvol.custom_create()
        v1_subvol.sanity_test_custom_made_subvol()

        writer = GenIoLoad(v1_subvol.build_and_get_mnt_path(), self.user_mount,
                           self.CLIENT_ID, self.fs.data_pool_name,
                           initial_wait=self.INITIAL_WAIT,
                           raise_method=self.raise_method)
        writer.start()

        # XXX: TRIGGERING AUTO-UPGRADE
        v3_sv_path = v1_subvol.getpath()

        self.assertEqual(writer.is_alive(), True, self.IO_CRASHED_MSG)
        # building v3 object, nothing more.
        v3_subvol = SubvolV3(subvol=v1_subvol)
        # verify the path returned by last getpath cmd
        self.assertEqual(v3_sv_path, v3_subvol.build_and_get_mnt_path())
        v3_subvol.sanity_test_meta_file()
        v3_subvol.sanity_test_subvol_ls()
        v3_subvol.sanity_test_subvol_getpath()

        writer.stop()
        # verify number of files written to ensure nothing was lost
        writer.verify_num_of_files_written()

        v3_subvol.rm()
        self.wait_till_trash_is_empty()

    def test_with_subvol_client(self):
        '''
        Test subvol upgrade from v1 to v3 when subvol is located in the default
        subvol group and is under IO load via a client that has caps only for
        the data path of the subvol that is being upgraded.
        '''
        v1_subvol = SubvolV1(self.mon_manager, self.admin_mount, self.volname)
        v1_subvol.custom_create()
        v1_subvol.sanity_test_custom_made_subvol()

        writer = GenIoLoad(v1_subvol.build_and_get_mnt_path(), self.user_mount,
                           self.CLIENT_ID, self.fs.data_pool_name,
                           initial_wait=self.INITIAL_WAIT,
                           raise_method=self.raise_method)
        writer.start()

        # XXX: TRIGGERING AUTO-UPGRADE
        v3_sv_path = v1_subvol.getpath().strip()

        # building v3 object, nothing more.
        self.assertEqual(writer.is_alive(), True, self.IO_CRASHED_MSG)
        v3_subvol = SubvolV3(subvol=v1_subvol)
        # verify the path returned by last getpath cmd
        self.assertEqual(v3_sv_path, v3_subvol.build_and_get_mnt_path())
        v3_subvol.sanity_test_meta_file()
        v3_subvol.sanity_test_subvol_ls()
        v3_subvol.sanity_test_subvol_getpath()

        writer.stop()
        # verify number of files written to ensure nothing was lost
        writer.verify_num_of_files_written()

        v3_subvol.rm()
        self.wait_till_trash_is_empty()

    def test_with_subvol_client_with_snap(self):
        '''
        Test subvol upgrade from v1 to v3 when subvol is located in the default
        subvol group, contains a snapshot and is under IO load via a client that
        has caps only for the data path of the subvolume that is being upgraded.
        '''
        v1_subvol = SubvolV1(self.mon_manager, self.admin_mount, self.volname)
        v1_subvol.custom_create()
        snap_name = v1_subvol.custom_create_snap()
        v1_subvol.sanity_test_custom_made_subvol()
        v1_subvol.sanity_test_custom_made_snap(snap_name)

        writer = GenIoLoad(v1_subvol.build_and_get_mnt_path(), self.user_mount,
                           self.CLIENT_ID, self.fs.data_pool_name,
                           initial_wait=self.INITIAL_WAIT,
                           raise_method=self.raise_method)
        writer.start()

        # XXX: TRIGGERING AUTO-UPGRADE
        v3_sv_path = v1_subvol.getpath().strip()

        # building v3 object, nothing more.
        self.assertEqual(writer.is_alive(), True, self.IO_CRASHED_MSG)
        v3_subvol = SubvolV3(subvol=v1_subvol)
        # verify the path returned by last getpath cmd
        self.assertEqual(v3_sv_path, v3_subvol.build_and_get_mnt_path())
        v3_subvol.sanity_test_meta_file()
        v3_subvol.sanity_test_subvol_ls()
        v3_subvol.sanity_test_subvol_getpath()
        v3_subvol.sanity_test_snap_ls(snap_name)
        v3_subvol.sanity_test_snap_getpath(snap_name)

        writer.stop()
        # verify number of files written to ensure nothing was lost
        writer.verify_num_of_files_written()

        v3_subvol.snap_rm(snap_name)
        v3_subvol.rm()
        self.wait_till_trash_is_empty()
