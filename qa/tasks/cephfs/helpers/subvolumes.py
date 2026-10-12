from logging import getLogger
from os.path import join
from uuid import uuid4, UUID
from textwrap import dedent
from json import loads as json_loads


log = getLogger(__name__)


def verify_uuid(uuid):
    assert type(uuid) is str

    try:
        UUID(uuid, version=4)
    except:
        # just being explicit about exception being raised here
        raise


class SubvolBase:
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

    @property
    def data_path(self):
        '''
        Path where subvol users are supposed to store data can be different
        depenidng on the subvol version. This method, through overriding, allows
        writing code for subvolumes regardless of different data path for subvol
        version.
        '''
        raise NotImplementedError('shoold\'ve been overriden')

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

    def authorize(self, client_id):
        auth_cmd = (f'fs subvolume authorize {self.volname} {self.name} '
                         f'{client_id}')
        if self.grp_name:
            auth_cmd += f' --group-name {self.grp_name}'

        self.run_ceph_cmd(auth_cmd)

    # TODO: default value for 'initial_wait' should be 60 sec
    def gen_io_load(self, client_id=None, initial_wait=5):
        self.writer = self.mount_b.gen_io_load(self.data_path, client_id,
                                               self.fs.data_pool_name,
                                               initial_wait)


class SubvolV2(SubvolBase):
    '''
    Helper class for v2 subvolumes.
    '''

    def __init__(self, tco, volname=None, name=None, uuid=None, grp_name=None,
                 snap_name=None, retained=False, writer=None):
        super().__init__(tco=tco, volname=volname, name=name, uuid=uuid,
                         grp_name=grp_name, snap_name=snap_name, writer=writer,
                         retained=retained)

        self.uuid = uuid if uuid else str(uuid4())

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

    @property
    def data_path(self):
        return self.uuid_path

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


class SubvolV3(SubvolBase):
    '''
    Helper class for v3 subvolumes.
    '''

    def __init__(self, tco=None, volname=None, name=None, uuid=None,
                 grp_name=None, snap_name=None, retained=False, writer=None,
                 has_v2_snaps=False, v2=None):
        if v2:
            assert not (tco or volname or name or uuid or grp_name or snap_name
                        or retained or writer or has_v2_snaps)
            assert type(v2) is SubvolV2
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

    @property
    def data_path(self):
        return self.mnt_path

    def fetch_v3_uuid(self):
        curr_incar_meta_file_name = self.mount_a.get_shell_stdout(
                f'readlink {self.meta_slink_path}').strip()
        uuid = curr_incar_meta_file_name.replace('.meta.', '')
        verify_uuid(uuid)
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
