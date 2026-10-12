from logging import getLogger
from time import sleep

from tasks.cephfs.helpers.subvolumes import SubvolV2, SubvolV3
from tasks.cephfs.test_volumes import VolumesHelper


log = getLogger(__name__)


class TestBasic(VolumesHelper):
    '''
    Test subvol upgrade from v2 to v3 without IO load.
    '''

    client_id = 'x1'
    CLIENTS_REQUIRED = 2

    def test_subvol(self):
        '''
        Test subvol upgrade from v2 to v3 when subvol is located in the
        default subvol group.
        '''
        v2 = SubvolV2(tco=self)
        v2.custom_create()
        v2.sanity_test_subvol()

        # this will trigger subvol auto-upgrade
        v3_sv_path = v2.getpath()
        v3 = SubvolV3(v2=v2)
        msg = (f'subvol upgrade for {v2.name} from v2 to v3 passed (because '
               'there was no crash) but output of getpath cmd is incorrect')
        self.assertEqual(v3_sv_path, f'/{v3.mnt_path}', msg)

        v3.verify_meta_file()
        v3.sanity_test_subvol()
        # TODO
        return

        v3.remove()
        self._wait_for_trash_empty()

    def test_custom_group_subvol(self):
        '''
        Test subvol upgrade from v2 to v3 when subvol is located in a
        non-default subvol group.
        '''
        v2 = SubvolV2(tco=self, grp_name=True)
        v2.custom_create()
        v2.sanity_test_subvol()

        # this will trigger subvol auto-upgrade
        v3_sv_path = v2.getpath()
        v3 = SubvolV3(v2=v2)
        msg = (f'subvol upgrade for {v2.name} from v2 to v3 passed (because '
               'there was no crash) but output of getpath cmd is incorrect')
        self.assertEqual(v3_sv_path, f'/{v3.mnt_path}', msg)

        v3.verify_meta_file()
        v3.sanity_test_subvol()
        # TODO
        return

        v3.remove()
        self._wait_for_trash_empty()

    def test_subvol_with_snap(self):
        pass

    def test_subvol_with_retained_snap(self):
        pass

    # TODO: test that v2 incar dir with 2 snaps isn't deleted until last snap is
    # not deleted.
    def test_subvol_with_v2_snap(self):
        '''
        Test subvol upgrade from v2 to v3 when it has a snap.
        '''
        v2 = SubvolV2(tco=self, grp_name=True, snap_name=True)
        v2.custom_create()
        v2.sanity_test_subvol()

        # this will trigger subvol auto-upgrade
        v3_sv_path = v2.getpath()
        v3 = SubvolV3(v2=v2)
        msg = (f'subvol upgrade for {v2.name} from v2 to v3 passed (because '
               'there was no crash) but output of getpath cmd is incorrect')
        self.assertEqual(v3_sv_path, f'/{v3.mnt_path}', msg)

        v3.verify_meta_file()
        v3.sanity_test_subvol()
        # rishabh, start here: because v2 snap is not listed by "snap ls" cmd
        v3.sanity_test_v2_snap()
        # TODO
        return

        v3.remove_snap()
        v3.remove()
        self._wait_for_trash_empty()

    def _test_subvol_with_retained_v2_snap(self):
        v2 = SubvolV2(tco=self, grp_name=True, snap_name=True,
                      retained=True)
        v2.custom_create()
        v2.sanity_test_subvol()

        # this will trigger subvol auto-upgrade
        v3_sv_path = v2.getpath()
        v3 = SubvolV3(v2=v2)
        msg = (f'subvol upgrade for {v2.name} from v2 to v3 passed (because '
               'there was no crash) but output of getpath cmd is incorrect')
        self.assertEqual(v3_sv_path, f'/{v3.mnt_path}', msg)

        v3.verify_meta_file()
        v3.sanity_test_subvol()
        v3.sanity_test_retained_subvol()
        v3.sanity_test_retained_snap()

        v3.remove_snap()
        v3.remove()
        self._wait_for_trash_empty()


class TestWithIoLoad(VolumesHelper):
    '''
    Test subvol upgrade from v2 to v3 while IO is being performed on the subvol.
    '''

    CLIENTS_REQUIRED = 2

    def test_with_fs_client(self):
        '''
        Test subvol upgrade from v2 to v3 when subvol is located in the
        default subvol group and the subvol is under IO load.
        '''
        v2 = SubvolV2(tco=self)
        v2.custom_create()
        v2.sanity_test_subvol()

        v2.gen_io_load('x1')

        # this will trigger subvol auto-upgrade
        v3_sv_path = v2.getpath()
        v3 = SubvolV3(v2=v2)
        msg = (f'subvol upgrade for {v2.name} from v2 to v3 passed (because '
               'there was no crash) but output of getpath cmd is incorrect')
        self.assertEqual(v3_sv_path, f'/{v3.mnt_path}', msg)

        # XXX writer threads shouldn't die or be affected due to upgrade
        msg = ('thread writing on subvol via client crashed while subvol '
               f'{v3.name} was being upgraded from v2 to v3')
        self.assertEqual(v3.writer.is_alive(), True, msg)

        v3.verify_meta_file()
        v3.sanity_test_subvol()

        # upgrade was successful, stopping client workload
        v3.writer.stop()
        # avoids unnecessary failure in case writer threads takes some time to
        # stop
        sleep(5)
        msg = ('upgrade was successful but writer thread didnt stop despite '
               'signaling stop')
        self.assertEqual(v3.writer.is_alive(), False, msg)

        # verifying if files were actually being written on the subvol
        v3.writer.verify_num_of_files_written()

        v3.remove()
        self._wait_for_trash_empty()

    def test_with_subvol_client(self):
        '''
        Test subvol upgrade from v2 to v3 when subvol is located in the
        default subvol group and the subvol is under IO load.
        '''
        v2 = SubvolV2(tco=self)
        v2.custom_create()
        v2.sanity_test_subvol()

        v2.gen_io_load('x1')

        # this will trigger subvol auto-upgrade
        v3_sv_path = v2.getpath()
        v3 = SubvolV3(v2=v2)
        msg = (f'subvol upgrade for {v2.name} from v2 to v3 passed (because '
               'there was no crash) but output of getpath cmd is incorrect')
        self.assertEqual(v3_sv_path, f'/{v3.mnt_path}', msg)

        # XXX writer threads shouldn't die or be affected due to upgrade
        msg = ('thread writing on subvol via client crashed while subvol '
               f'{v3.name} was being upgraded from v2 to v3')
        self.assertEqual(v3.writer.is_alive(), True, msg)

        v3.verify_meta_file()
        v3.sanity_test_subvol()

        # upgrade was successful, stopping client workload
        v3.writer.stop()
        # avoids unnecessary failure in case writer threads takes some time to
        # stop
        sleep(5)
        msg = ('upgrade was successful but writer thread didnt stop despite '
               'signaling stop')
        self.assertEqual(v3.writer.is_alive(), False, msg)

        # verifying if files were actually being written on the subvol
        v3.writer.verify_num_of_files_written()

        v3.remove()
        self._wait_for_trash_empty()
