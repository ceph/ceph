from logging import getLogger
from os.path import join, basename, dirname
from uuid import uuid4, UUID
from time import sleep as time_sleep
from json import loads as json_loads
from textwrap import dedent

from tasks.cephfs.test_volumes import VolumesHelper


log = getLogger(__name__)


uuid_ = '4f50c332-30a6-4871-b69d-9edd2ea529c0'


def validate_uuid(uuid):
    assert type(uuid) is str

    try:
        UUID(uuid, version=4)
    except:
        # just being explicit about exception being raised here
        raise


class SubvolHelper:
    '''
    Base class for v0, v1, v2, and v3 subvol helper classes.
    '''

    def __init__(self, tco=None, volname=None, name=None, uuid=None,
                 grp_name=None, snap_name=None, retained=False, writer=None):
        '''
        tco -> test class object

        - grp_name and snap_name can be str, None or True.
            - None implies no group and no snap
            - True implies gen random str and use it as name
            - str implies use it as the name

        - retained can be True or False
            - has subvol been removed and snap has been retained

        - rest can be str or None
        '''
        self.tco = tco
        self._get_tco_members()

        self.volname = volname
        self.name = name
        self.uuid = uuid
        self.grp_name = grp_name
        self.snap_name = snap_name
        self.retained = retained
        self.writer = writer

        # NOTE subclasses should call these methods at end of their __init__()
        #self._process_basic_attrs()
        #self._define_std_paths()
        #self._verify_attrs()
        return

    def _process_basic_attrs(self):
        self.volname = self.volname if self.volname else self.tco.volname
        self.name = self.name if self.name else self._gen_subvol_name()

        # these attrs can be str, None or True
        attrs = {'grp_name': self.grp_name, 'snap_name': self.snap_name}
        for k, v in attrs.items():
            if v is True:
                if k == 'grp_name':
                    v = self._gen_subvol_grp_name()
                elif k == 'snap_name':
                    v = self._gen_subvol_snap_name()

                setattr(self, k, v)
            elif v is None or type(k) is str:
                setattr(self, k, v)
            else:
                raise RuntimeError(f'{k} can be only str, True or None. '
                                   f'k = {v} type(k) = {type(v)}')

    def _get_tco_members(self):
        '''
        Get (only) needed members. This make it easy to use them as they are
        located within a namespace within object namespace and "only" so that
        ths helper classes are not croweded with unnecessary members.
        '''
        # members from CephFSTestCase
        self.fs = self.tco.fs
        self.mount_a = self.tco.mount_a
        self.mount_b = self.tco.mount_b
        self.create_client = self.tco.create_client

        # members from CephTestCase
        self.run_ceph_cmd = self.tco.run_ceph_cmd
        self.get_ceph_cmd_stdout = self.tco.get_ceph_cmd_stdout

        # members from VolumesHelper
        self.volname = self.tco.volname
        self._gen_name = self.tco._gen_name
        self._gen_subvol_name = self.tco._gen_subvol_name
        self._gen_subvol_snap_name = self.tco._gen_subvol_snap_name
        self._gen_subvol_grp_name = self.tco._gen_subvol_grp_name

        # members from unittest
        self.assertIn = self.tco.assertIn
        self.assertEqual = self.tco.assertEqual

    def _verify_attrs(self):
        msg = f'self.volname = {self.volname}'
        assert isinstance(self.volname, str), msg
        msg = f'self.grp_name = {self.grp_name}'
        assert self.grp_name is None or type(self.grp_name) is str, msg
        msg = f'self.name = {self.name}'
        assert isinstance(self.name, str), msg

        msg = f'self.snap_name = {self.snap_name}'
        assert self.snap_name is None or type(self.snap_name) is str, msg
        msg = f'self.retained = {self.retained}'
        assert self.retained in (True, False), msg

    def _define_std_paths(self):
        if self.grp_name is None:
            grp_name_in_path = '_nogroup'
        elif type(self.grp_name) is str:
            grp_name_in_path = self.grp_name
        else:
            raise RuntimeError('self.grp_name should be str or None by this '
                               f'point. self.grp_name = {self.grp_name}')

        self.subvol_path = join('volumes', grp_name_in_path, self.name)

    def get_uuid(self):
        raise NotImplementedError()

    def remove(self, wait=False):
        sv_rm_cmd = f'fs subvolume rm {self.volname} {self.name}'
        if self.grp_name:
            sv_rm_cmd += f' --group-name {self.grp_name}'

        self.run_ceph_cmd(sv_rm_cmd)

    def sanity_test_subvol(self):
        sv_ls_cmd = f'fs subvolume ls {self.volname}'
        if self.grp_name:
            sv_ls_cmd += f' --group-name {self.grp_name}'

        subvols = self.get_ceph_cmd_stdout(sv_ls_cmd)
        subvols = json_loads(subvols)
        assert {'name': self.name} in subvols

    def sanity_test_v2_snap(self):
        assert self.snap_name

        ss_ls_cmd = (f'fs subvolume snapshot ls {self.volname} {self.name}')
        if self.grp_name:
            ss_ls_cmd += f' --group-name {self.grp_name}'

        snap_names = self.get_ceph_cmd_stdout(ss_ls_cmd)
        snap_names = json_loads(snap_names)
        self.assertIn({'name': self.snap_name}, snap_names)

        v2_snap_path = f'volumes/{self.grp_name}/{self.name}/.snap'
        self.mount_a.run_shell(f'stat {v2_snap_path}')

    def sanity_test_retained_subvol(self):
        assert self.retained

        raise NotImplementedError()

    def sanity_test_retained_snap(self):
        assert self.retained

        raise NotImplementedError()

    def remove_snap(self):
        snap_rm_cmd = (f'fs subvolume snapshot rm {self.volname} {self.name} '
                       f'{self.snap_name}')
        if self.grp_name:
            snap_rm_cmd += f' --group-name {self.grp_name}'

        self.run_ceph_cmd(snap_rm_cmd)

    def authorize(self):
        authorize_cmd = (f'fs subvolume authorize {self.volname} {self.name} '
                         f'{self.client_id}')
        if self.grp_name:
            authorize_cmd += f' --group-name {self.grp_name}'

        self.run_ceph_cmd(authorize_cmd)

    def create_subvol_client_and_remount(self, path):
        # right now only v2 path is supported
        assert dirname(path) == self.subvol_path

        if path[0] != '/':
            path = '/' + path
        v3_path = join(dirname(path), 'roots', uuid_, 'mnt')

        keyring = self.create_client(
                self.client_id, moncap='allow r',
                osdcap=f'allow rw pool={self.fs.data_pool_name}',
                mdscap=f'allow rw path=/{path}, allow rw path=/{v3_path}')
        #keyring = self.get_ceph_cmd_stdout(f'auth get client.{self.client_id}')

        key_path = self.mount_b.client_remote.mktemp(
                suffix=f'ceph.client.{self.client_id}.keyring', data=keyring)

        self.mount_b.remount(client_id=self.client_id,
                             client_keyring_path=key_path, cephfs_mntpt=path)

    def gen_io_load_via_fs_client(self):
        self.writer = self.mount_b.gen_io_load('/')

    def gen_io_load_via_subvol_client(self, path):
        self.create_subvol_client_and_remount(path)
        self.writer = self.mount_b.gen_io_load('/')


class SubvolV2Helper(SubvolHelper):
    '''
    Helper class for v2 subvolumes.
    '''

    def __init__(self, tco, volname=None, name=None, uuid=None, grp_name=None,
                 snap_name=None, retained=False, writer=None):
        super().__init__(tco=tco, volname=volname, name=name, uuid=uuid,
                         grp_name=grp_name, snap_name=snap_name, writer=writer,
                         retained=retained)

        self.uuid = uuid_
        #self.uuid = uuid if uuid else str(uuid4())

        self._process_basic_attrs()
        self._define_std_paths()
        self._verify_attrs()

    def _define_std_paths(self):
        super()._define_std_paths()

        self.meta_path = join(self.subvol_path, '.meta')

        self.uuid_path = join(self.subvol_path, self.uuid)
        self.snap_base_path = join(self.uuid_path, '.snap')
        if self.snap_name:
            self.snap_path = join(self.snap_base_path, self.snap_name)

    def _verify_attr(self):
        super()._verify_attrs()

        msg = f'self.uuid = {self.uuid}'
        assert isinstance(self.uuid, str), msg

        msg = f'self.snap_base_path = {self.snap_base_path}'
        assert type(self.snap_base_path) is str, msg

    def custom_create(self):
        '''
        Create mock v2 subvol for testing upgrade.
        '''
        self.mount_a.run_shell(f'mkdir -p {self.uuid_path}')

        self.mount_a.write_file(self.meta_path, dedent(f'''\
            [GLOBAL]
            version = 2
            type = subvolume
            path = /{self.uuid_path}
            state = complete'''), sudo=True)

        # so that files can accessed without unnecessary hassles
        self.mount_a.run_shell(f'chmod -R 755 {self.uuid_path}')

        if self.snap_name:
            self.mount_a.run_libcephfs_pybind_code(dedent(f"""
            from time import sleep
            sleep(2)
            cephfs.mksnap('{self.uuid_path}', '{self.snap_name}', 0o755)
            """))

            # verify that snap was created
            self.mount_a.run_shell(f'stat {self.snap_path}')

        if self.retained:
            raise NotImplementedError()

    def getpath(self):
        cmd = f'fs subvolume getpath {self.volname} {self.name}'
        if self.grp_name:
            cmd += f' --group-name {self.grp_name}'

        return self.get_ceph_cmd_stdout(cmd).strip()


class SubvolV3Helper(SubvolHelper):
    '''
    Helper class for v3 subvolumes.
    '''

    def __init__(self, tco=None, volname=None, name=None, uuid=None,
                 grp_name=None, snap_name=None, retained=False, writer=None,
                 has_v2_snaps=False, v2=None):
        if v2:
            assert not (tco or volname or name or uuid or grp_name or snap_name
                        or retained or writer or has_v2_snaps)
            assert type(v2) is SubvolV2Helper
            super().__init__(tco=v2.tco, volname=v2.volname, name=v2.name,
                             uuid=uuid, grp_name=v2.grp_name,
                             snap_name=v2.snap_name, retained=v2.retained,
                             writer=v2.writer)
            self.has_v2_snaps = True if v2.snap_name else False
        else:
            super().__init__(tco=tco, volname=volname, name=name, uuid=uuid,
                             grp_name=grp_name, snap_name=snap_name,
                             retained=retained, writer=writer)
            self.has_v2_snaps = False

        self._process_basic_attrs()
        self._define_std_paths(v2)
        self._verify_attrs(v2)

    def _set_uuid(self, v2):
        if v2:
            if self.has_v2_snaps:
                # if v2 subvol had snaps, auto-upgraded subvol v3 will preserve v2
                # incar as it is due snaps in it and proceed to create a UUID dir
                # with a new UUID, therefore uuid has to fetched.
                self.uuid = self.fetch_v3_uuid()
            else:
                self.uuid = v2.uuid
        else:
            if not self.uuid:
                self.uuid = self.fetch_v3_uuid()

    def _define_std_paths(self, v2):
        super()._define_std_paths()

        self.roots_path = join(self.subvol_path, 'roots')
        self.meta_slink_path = join(self.subvol_path, '.meta')

        # self.meta_path is defined by super()._define_std_paths() and uuid
        # can't be found without it, so unfortunately this has to here.  :(
        self._set_uuid(v2)

        self.meta_path = join(self.subvol_path, f'.meta.{self.uuid}')
        self.uuid_path = join(self.roots_path, self.uuid)
        self.mnt_path = join(self.uuid_path, 'mnt')
        self.snap_base_path = join(self.uuid_path, '.snap')
        if self.snap_name:
            self.snap_path = join(self.snap_base_path, self.snap_name)

    def _verify_attrs(self, v2):
        super()._verify_attrs()

        msg = f'self.uuid = {self.uuid}'
        assert isinstance(self.uuid, str), msg
        msg = f'self.snap_base_path = {self.snap_base_path}'
        assert type(self.snap_base_path) is str, msg

        if v2:
            if self.has_v2_snaps:
                assert self.uuid != v2.uuid
            else:
                assert self.uuid == v2.uuid

        msg = (f'self.has_v2_snaps = {self.has_v2_snaps} v2.snap_name = '
               f'{v2.snap_name}')
        assert ((self.has_v2_snaps is True and  type(v2.snap_name) is str) or
                (self.has_v2_snaps is False and v2.snap_name is None))

    def fetch_v3_uuid(self):
        curr_incar_meta_file_name = self.mount_a.get_shell_stdout(
                f'readlink {self.meta_slink_path}').strip()
        uuid = curr_incar_meta_file_name.replace('.meta.', '')
        validate_uuid(uuid)
        return uuid

    def verify_meta_file(self):
        '''
        Verify that subvolume and its metadata file are how they should be for
        a v3 subvolume.
        '''
        meta_content = self.mount_a.get_shell_stdout(f'sudo cat {self.meta_path}')

        # splitting ensures we compare line by line, which prevents any
        # accidental matching due to edge cases
        meta_content = meta_content.split('\n')

        self.assertIn('version = 3', meta_content)
        self.assertIn(f'path = /{self.mnt_path}', meta_content)
        self.assertIn('state = complete', meta_content)
        self.assertIn('type = subvolume', meta_content)

        if self.has_v2_snaps:
            self.assertIn('has_v2_snaps = True', meta_content)


class TestBasic(VolumesHelper):
    '''
    Test subvol upgrade from v2 to v3.
    '''

    client_id = 'x1'
    CLIENTS_REQUIRED = 2

    def test_regular_basic_subvol(self):
        '''
        Test subvol upgrade from v2 to v3 when subvol is located in the
        default subvol group.
        '''
        v2 = SubvolV2Helper(tco=self)
        v2.custom_create()
        v2.sanity_test_subvol()

        # causes subvol to auto-upgrade
        v3_sv_path = v2.getpath()
        v3 = SubvolV3Helper(v2=v2)
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
        v2 = SubvolV2Helper(tco=self, grp_name=True)
        v2.custom_create()
        v2.sanity_test_subvol()

        # causes subvol to auto-upgrade
        v3_sv_path = v2.getpath()
        v3 = SubvolV3Helper(v2=v2)
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
        v2 = SubvolV2Helper(tco=self, grp_name=True, snap_name=True)
        v2.custom_create()
        v2.sanity_test_subvol()

        # causes subvol to auto-upgrade
        v3_sv_path = v2.getpath()
        v3 = SubvolV3Helper(v2=v2)
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
        v2 = SubvolV2Helper(tco=self, grp_name=True, snap_name=True,
                            retained=True)
        v2.custom_create()
        v2.sanity_test_subvol()

        v3 = self.cause_auto_upgrade(v2)
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

    def test_regular_basic_subvol_with_workload_via_fs_client(self):
        '''
        Test subvol upgrade from v2 to v3 when subvol is located in the
        default subvol group and the subvol is under IO load.
        '''
        v2 = SubvolV2Helper(tco=self)
        v2.custom_create()
        v2.sanity_test_subvol()

        v2.gen_io_load_via_fs_client()
        log.info('giving 60 seconds for background threads for writing...')
        time_sleep(20)

        v3 = self.cause_auto_upgrade(v2)
        msg = ('thread writing on subvol via client crashed while subvol '
               f'{v3.name} was being upgraded from v2 to v3')
        # XXX writer threads shouldn't die or be affected due to upgrade
        self.assertEqual(v3.writer.is_alive(), True, msg)

        v3.verify_meta_file()
        v3.sanity_test_subvol()

        # upgrade was successful, stopping client workload
        v3.writer.stop()
        # avoids unnecessary failure in case writer threads takes some time to
        # stop
        time_sleep(5)
        msg = ('upgrade was successful but writer thread didnt stop despite '
               'signaling stop')
        self.assertEqual(v3.writer.is_alive(), False, msg)

        # verifying if files were actually being written on the subvol
        v3.writer.verify_num_of_files_written()

        v3.remove()
        self._wait_for_trash_empty()

    def test_regular_basic_subvol(self):
        '''
        Test subvol upgrade from v2 to v3 when subvol is located in the
        default subvol group and the subvol is under IO load.
        '''
        v2 = SubvolV2Helper(tco=self)
        v2.custom_create()
        v2.sanity_test_subvol()

        v2.gen_io_load_via_subvol_client(v2.uuid_path)
        log.info('giving 60 seconds for background threads for writing...')
        time_sleep(5)

        # will trigger subvol auto-upgrade
        v3_sv_path = v2.getpath()
        v3 = SubvolV3Helper(v2=v2)
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
        time_sleep(5)
        msg = ('upgrade was successful but writer thread didnt stop despite '
               'signaling stop')
        self.assertEqual(v3.writer.is_alive(), False, msg)

        # verifying if files were actually being written on the subvol
        v3.writer.verify_num_of_files_written()

        v3.remove()
        self._wait_for_trash_empty()
