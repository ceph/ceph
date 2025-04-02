'''
Test v3-specific features.
'''

from uuid import UUID
from errno import ENOENT, EEXIST
from os.path import join
from logging import getLogger

from tasks.cephfs.helpers.misc import gen_subvol_snap_name
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.helpers.subvolumes import SubvolV3Helper, gen_uuid, \
                                            verify_uuid

from teuthology.exceptions import CommandFailedError


log = getLogger(__name__)


class TestV3Features(CephFSTestCase):
    '''
    Test v3 subvol features: multiple incarnations, separate mount path, meta
    file, snap path, etc. per incarnation, meta content for v3, etc
    '''

    def wait_for_trash_dir_to_be_empty(self, timeout=60):
        self.mount_a.wait_for_dir_empty('volumes/_deleting', timeout)

    def test_basic_subvol(self):
        sv = SubvolV3Helper(self.mon_manager, self.mount_a, self.fs.name)
        sv.create()

        sv.verify_uuid()
        sv.verify_layout()
        sv.verify_meta_file()
        sv.verify_subvol_ls_cmd()
        sv.verify_subvol_getpath_cmd()

        # test writing files on this path.
        sv.write_files(num=5)
        subvol_mnt_path = sv.getpath()
        for i in range(5):
            file_path = join(subvol_mnt_path, f'file-{i}')
            self.mount_a.verify_path_is_reg(file_path)

        sv.rm()
        self.mount_a.negtestcmd(f'ls ./{subvol_mnt_path}', retval=ENOENT)
        self.wait_for_trash_dir_to_be_empty()

    def test_subvol_with_snap(self):
        sv = SubvolV3Helper(self.mon_manager, self.mount_a, self.fs.name)
        sv.create()
        subvol_mnt_path = sv.getpath()

        sv.verify_uuid()

        snap_name = sv.snap_create()
        snap_path = sv.snap_getpath(snap_name)

        self.mount_a.run_shell(f'ls ./{snap_path}')
        sv.verify_snap_ls_cmd(snap_name)
        sv.verify_snap_getpath_cmd(snap_name)

        sv.snap_rm(snap_name)
        sv.rm()
        self.mount_a.negtestcmd(f'ls ./{subvol_mnt_path}', retval=ENOENT)
        self.wait_for_trash_dir_to_be_empty()

    def test_subvol_with_1_retained_incar(self):
        sv = SubvolV3Helper(self.mon_manager, self.mount_a, self.fs.name)
        sv.create()
        sv.verify_uuid()
        subvol_mnt_path = sv.getpath()
        sv.write_files(num=5)
        snap_name = sv.snap_create()
        snap_path = sv.snap_getpath(snap_name)
        sv.rm(opts='--retain-snapshots')

        #sv.verify_uuid()
        sv.verify_layout()
        sv.verify_meta_file()
        sv.verify_subvol_ls_cmd()
        #sv.verify_subvol_getpath_cmd()
        sv.verify_snap_ls_cmd(snap_name)
        sv.verify_snap_getpath_cmd(snap_name)

        sv.snap_rm(snap_name)
        self.mount_a.negtestcmd(f'ls ./{sv.gen_base_path()}', retval=ENOENT)
        self.wait_for_trash_dir_to_be_empty()

    def test_subvol_with_2nd_incar(self):
        sv = SubvolV3Helper(self.mon_manager, self.mount_a, self.fs.name)
        for i in range(2):
            sv.create()
            sv.write_files(num=5, ifx='at-creation')
            sv.snap_create()
            if i != 1:
                sv.rm(opts='--retain-snapshots')

        sv.verify_uuid()
        # checks layout not just fot current incar but also previous incars
        sv.verify_layout()
        sv.verify_meta_file()
        sv.verify_subvol_ls_cmd()
        sv.verify_subvol_getpath_cmd()
        sv.verify_snap_ls_for_all_incars()
        sv.verify_snap_getpath_for_all_incars()

        sv.snap_rm(sv.all_incars[0].snap_names[0])
        sv.snap_rm(sv.all_incars[0].snap_names[0])
        self.mount_a.negtestcmd(f'ls ./{sv.gen_base_path()}', retval=ENOENT)
        self.wait_for_trash_dir_to_be_empty()

    def verify_everything(self, sv):
        sv.verify_uuid()
        # checks layout not just fot current incar but also previous incars
        sv.verify_layout()
        sv.verify_meta_file()
        sv.verify_subvol_ls_cmd()
        sv.verify_subvol_getpath_cmd()
        sv.verify_snap_ls_for_all_incars()
        sv.verify_snap_getpath_for_all_incars()

    def test_subvol_with_3rd_incar(self):
        sv = SubvolV3Helper(self.mon_manager, self.mount_a, self.fs.name)
        for i in range(3):
            sv.create()
            sv.write_files(num=5, ifx='at-creation')
            sv.snap_create()
            self.verify_everything(sv)
            if i != 2:
                sv.rm(opts='--retain-snapshots')

        self.verify_everything(sv)
        snap_name = sv.all_incars[-1].snap_names[-1]
        self.verify_everything(sv)

        sv.snap_rm(sv.all_incars[0].snap_names[0])
        self.verify_everything(sv)
        sv.snap_rm(sv.all_incars[0].snap_names[0])
        self.verify_everything(sv)
        sv.snap_rm(sv.all_incars[0].snap_names[0])

        self.mount_a.negtestcmd(f'ls ./{sv.gen_base_path()}', retval=ENOENT)
        self.wait_for_trash_dir_to_be_empty()

    def test_subvol_with_7_incars(self):
        '''
        Test subvol with 7 incar so that we can tests removing first last and an
        incar in middle.
        '''
        # chance of producing duplicate name is higher if names are generated
        # one by one by subvol helper, prevent collision.
        all_snap_names = gen_subvol_snap_name(7)
        sv = SubvolV3Helper(self.mon_manager, self.mount_a, self.fs.name)

        # create 7 incars
        for i in range(7):
            sv.create()
            sv.write_files(num=5, ifx='at-creation')
            snap_name = sv.snap_create(all_snap_names[i])
            log.info(f'i = {i} sv.uuid = {sv.uuid} snap_name = {snap_name}')
            snap_path = sv.snap_getpath(snap_name)

            self.verify_everything(sv)
            if i != 6:
                sv.rm(opts='--retain-snapshots')

        # verify snaps cannot be created if a snap with same name already exists
        # in a different incarnation.
        for s in all_snap_names:
            self.negtest_ceph_cmd(f'fs subvolume snapshot create {sv.volname} '
                                  f'{sv.name} {s}', retval=EEXIST,
                                  errmsgs='already exists')

        # remove snap from first, last amd incar in middle, corresponding incar
        # dir mmust also be removed.
        for i in (0, -1, 3):
            uuid = sv.all_incars[i].uuid
            snap_name = sv.all_incars[i].snap_names[0]
            log.info(f'i = {i} uuid = {uuid} snap_name = {snap_name}')

            sv.snap_rm(snap_name)
            self.verify_everything(sv)
            incar_path = sv.gen_incar_path(uuid)
            # stat returns 1 instead of 2 when target is absent
            self.mount_a.negtestcmd(f'stat -c "" .{incar_path}', retval=1,
                                    errmsgs='no such file or dir')

            all_snap_names.remove(snap_name)

        for i in range(len(all_snap_names)):
            sv.snap_rm(all_snap_names[0])
            if len(sv.all_incars) == 0:
                self.verify_everything(sv)
            all_snap_names.pop(0)

        self.assertEqual(all_snap_names, [])
        self.mount_a.negtestcmd(f'ls ./{sv.gen_base_path()}', retval=ENOENT)
        self.assertIn(sv.name, str(sv.ls()))
        self.wait_for_trash_dir_to_be_empty()
