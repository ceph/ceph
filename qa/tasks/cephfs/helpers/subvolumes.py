'''
Helper code for operating on subvolume v3 and v2.
'''
from logging import getLogger
from os.path import join
from uuid import uuid4, UUID
from textwrap import dedent
from json import loads
from typing import Final

from tasks.cephfs.helpers.misc import (gen_name, gen_subvol_name,
                                       gen_subvol_grp_name,
                                       gen_subvol_snap_name)
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.ceph_manager import RunCephCmd


log = getLogger(__name__)


DEFAULT_GRP_NAME = '_nogroup'


def verify_uuid(uuid):
    assert type(uuid) is str

    try:
        UUID(uuid, version=4)
    except:
        # just being explicit about exception being raised here
        raise


class SubvolAutoUpgrade(CephFSTestCase):
    '''
    Helpers for testing subvol auto-upgrades.
    '''

    def setUp(self):
        super().setUp()
        self.volname = self.fs.name

    def tearDown(self):
        try:
            if hasattr(self, '_outcome') and hasattr(self._outcome, 'result'):
                result = self._outcome.result
                for test, _ in result.errors + result.failures:
                    if test == self:
                        failed = True
                        break
                else:
                    failed = False

            if failed:
                self.mount_a.run_shell('ls -lR ./')
                self.mount_a.run_shell('tree ./')
        except:
            # TODO: eventually throw try-exception clause but keep the code
            # above, as it will understanding subvol dir heirarchy during
            # failure.
            # prevent unnecessary test execution abortion by ignoring
            # exceptions raised by code block above
            pass

        super().tearDown()

    def wait_till_trash_is_empty(self, timeout=60):
        self.mount_a.wait_for_dir_empty('volumes/_deleting', timeout)


class SubvolOps(RunCephCmd):
    '''
    Helper methods for running 'ceph fs subvolume' commands.
    '''

    def __init__(self, mon_manager):
        super().__init__(mon_manager)


    # ----- helpers for subvol ops -----


    def create(self, volname, subvol_name, grp_name=None, opts=''):
        cmd = f'fs subvolume create {volname} {subvol_name}'
        if grp_name:
            cmd += f' --group-name {grp_name}'
        if opts:
            cmd += f' {opts}'

        return self.run_ceph_cmd(cmd)

    def rm(self, volname, subvol_name, grp_name=None, opts=''):
        cmd = f'fs subvolume rm {volname} {subvol_name}'
        if grp_name:
            cmd += f' --group-name {grp_name}'
        if opts:
            cmd += f' {opts}'

        return self.run_ceph_cmd(cmd)

    def getpath(self, volname, subvol_name, grp_name=None):
        cmd = f'fs subvolume getpath {volname} {subvol_name}'
        if grp_name:
            cmd += f' --group-name {grp_name}'

        return self.get_ceph_cmd_stdout(cmd).strip()

    def ls(self, volname, grp_name=None):
        cmd = f'fs subvolume ls {volname}'
        if grp_name:
            cmd += f' --group-name {grp_name}'

        return loads(self.get_ceph_cmd_stdout(cmd))

    def auth(self, volname, subvol_name, client_id, grp_name=None):
        cmd = f'fs subvolume authorize {volname} {subvol_name} {client_id}'
        if grp_name:
            cmd += f' --group-name {grp_name}'

        self.run_ceph_cmd(cmd)


    # ----- helpers for subvol snap ops -----


    def snap_create(self, volname, subvol_name, snap_name=None, grp_name=None):
        snap_name = snap_name if snap_name else gen_subvol_snap_name()
        cmd = (f'fs subvolume snapshot create {volname} {subvol_name} '
               f'{snap_name}')
        if grp_name:
            cmd += f' --group-name {grp_name}'

        self.run_ceph_cmd(cmd)
        return snap_name

    def snap_rm(self, volname, subvol_name, snap_name, grp_name=None):
        cmd = f'fs subvolume snapshot rm {volname} {subvol_name} {snap_name}'
        if grp_name:
            cmd += f' --group-name {grp_name}'

        return self.run_ceph_cmd(cmd)

    def snap_getpath(self, volname, subvol_name, snap_name, grp_name=None):
        cmd = (f'fs subvolume snapshot getpath {volname} {subvol_name} '
               f'{snap_name}')
        if grp_name:
            cmd += f' --group-name {grp_name}'

        return self.get_ceph_cmd_stdout(cmd).strip()

    def snap_ls(self, volname, subvol_name, grp_name=None):
        cmd = f'fs subvolume snapshot ls {volname} {subvol_name}'
        if grp_name:
            cmd += f' --group-name {grp_name}'

        return loads(self.get_ceph_cmd_stdout(cmd))


class SubvolBase(SubvolOps):
    '''
    Base class for v0, v1, v2, and v3 subvol helper classes.
    '''


    # ----- init and related methods -----


    def __init__(self, mon_manager=None, mount=None, volname=None, name=None,
                 uuid=None, grp_name=None, snaps=[], retained=False):
        '''
        :params grp_name: None or True, None implies no/default group and True
                          implies gen random str
        '''
        self.mon_manager = mon_manager
        self.mount = mount
        super().__init__(self.mon_manager)

        self.volname = volname
        self.grp_name = gen_subvol_grp_name() if grp_name is True else grp_name
        self.name = name if name else gen_subvol_name()
        self.uuid = uuid if uuid else str(uuid4())
        self.snaps = snaps
        self.retained = retained

        # NOTE subclasses should call these methods at end of their __init__()
        # self._reset_paths()
        # self._verify_attrs()
        return

    def _verify_attrs(self):
        assert type(self.volname) is str, \
            f'self.volname = {self.volname}'
        assert type(self.grp_name) in (type(None), str), \
            f'self.grp_name = {self.grp_name}'
        assert type(self.name) is str, \
            f'self.name = {self.name}'

        assert type(self.snaps) is list, \
            f'self.snaps = {self.snaps}'
        assert type(self.retained) is bool, \
            f'self.retained = {self.retained}'

    def _reset_paths(self):
        self._base_path = None
        self._meta_path = None


    # ----- subvol basics -----


    @property
    def version(self):
        raise NotImplementedError('should\'ve been overridden')

    @property
    def base_path(self):
        if not self._base_path:
            grp_name = self.grp_name if self.grp_name else DEFAULT_GRP_NAME
            self._base_path = join('/volumes', grp_name, self.name)
        return self._base_path

    @property
    def mnt_path(self):
        '''
        Path where subvol users are supposed to store data can be different
        depenidng on the subvol version. This method, through overriding, allows
        writing code for subvolumes regardless of different data path for subvol
        version.
        '''
        raise NotImplementedError('should have been overriden classes for '
                                  'specific subvol versions')


    # ----- helpers for subvol ops -----


    def create(self, opts=''):
        if self.create_opts:
            opts = opts + ' ' + self.create_opts
            if 'mode' not in opts:
                opts = opts + ' ' + ' --mode=755'

        return super().create(self.volname, self.name, self.grp_name, opts)

    def rm(self, opts=''):
        return super().rm(self.volname, self.name, self.grp_name, opts)

    def getpath(self):
        return super().getpath(self.volname, self.name, self.grp_name).\
                strip()

    def ls(self):
        return super().ls(self.volname, self.grp_name)

    def auth(self, client_id):
        return super().auth(self.volname, self.name, client_id, self.grp_name)


    # ----- helpers for subvol snap ops -----


    def snap_create(self, snap_name=None):
        return super().snap_create(self.volname, self.name, snap_name,
                                   self.grp_name)

    def snap_rm(self, snap_name):
        return super().snap_rm(self.volname, self.name, snap_name,
                               self.grp_name)

    def snap_getpath(self, snap_name):
        return super().snap_getpath(self.volnmae, self.name, snap_name,
                                    self.grp_name).strip()

    def snap_ls(self):
        return super().snap_ls(self.volname, self.name, self.grp_name)


    # ----- helpers for custom create -----


    def handle_create_opts(self):
        if self.create_opts is None:
            return

        assert type(self.create_opts) is str
        self.create_opts = self.create_opts.split()
        xattrs = {}

        if '--size' in self.create_opts:
            size = self.create_opts[self.create_opts.index('--size') + 1]
            # testing that value passed to "--size" is indeed a number
            size = int(size)
            xattrs['ceph.quota.max_bytes'] = str(size)

        if '--namespace-isolated' in self.create_opts:
            grp_name = self.grp_name if self.grp_name else self.DEFAULT_GRP_NAME
            xattrs['ceph.dir.layout.pool_namespace'] = \
            f'fsvolumens_{grp_name}_{self.name}'

        for k, v in xattrs.items():
            self.mount.run_shell(f'setfattr {k} {v} {self.mnt_path}')


    # ----- helpers for sanity testing ------


    # XXX: do _not_ open subvol using volumes plugin as it will trigger
    # auto-upgrade for the subvol.
    def sanity_test_custom_made_subvol(self):
        ls_output = self.ls()
        assert ls_output[0]['name'] == self.name, \
            f'snap_name = {self.name} ls_output = {ls_output}'

    # XXX: do _not_ open subvol using volumes plugin as it will trigger
    # auto-upgrade for the subvol.
    def sanity_test_custom_made_snap(self, snap_name):
        snap_path = self.build_and_get_snap_path(snap_name)
        self.mount.run_shell(f'stat {snap_path[1:]}')

    def sanity_test_subvol_ls(self):
        ls_output = self.ls()
        assert ls_output[0]['name'] == self.name,  \
            f'snap_name = {self.name} ls_output = {ls_output}'

    def sanity_test_subvol_getpath(self):
        sv_path = self.getpath()
        assert sv_path == self.mnt_path, \
            f'sv_path = {sv_path} self.mnt_path = {self.mnt_path}'

    def sanity_test_snap_ls(self, snap_name):
        ls_output = self.snap_ls()
        assert ls_output[0]['name'] == snap_name, \
            f'snap_name = {snap_name} ls_output = {ls_output}'

    def sanity_test_snap_getpath(self, snap_name):
        exp_snap_path = self.get_snap_path(snap_name)
        getpath = self.snap_getpath(snap_name)
        assert getpath == exp_snap_path


class SubvolV2(SubvolBase):
    '''
    Helper class for v2 subvolumes.
    '''

    def __init__(self, mon_manager=None, mount=None, volname=None, name=None,
                 uuid=None, grp_name=None, snaps=[], retained=False):
        super().__init__(mon_manager=mon_manager, mount=mount, volname=volname,
                         name=name, uuid=uuid, grp_name=grp_name, snaps=snaps,
                         retained=retained)

        self.uuid = uuid if uuid else str(uuid4())

        self._reset_paths()
        self._verify_attrs()

    def _reset_paths(self):
        super()._reset_paths()

        self._uuid_path = None
        self._snap_base_path = None

    def _verify_attrs(self):
        super()._verify_attrs()

        assert type(self.uuid) is str, \
            f'self.uuid = {self.uuid}'


    # ----- subvol basics -----


    @property
    def version(self):
        return 2

    @property
    def mnt_path(self):
        return self.uuid_path


    # ----- v2 helpers -----


    @property
    def uuid_path(self):
        if not self._uuid_path:
            self._uuid_path = join(self.base_path, self.uuid)
        return self._uuid_path

    @property
    def meta_path(self):
        if not self._meta_path:
            self._meta_path = join(self.base_path, '.meta')
        return self._meta_path

    def get_snap_base_path(self):
        if not self._snap_base_path:
            self._snap_base_path = join(self.base_path, '.snap')
        return self._snap_base_path

    def build_and_get_snap_path(self, snap_name):
        return join(self.get_snap_base_path(), snap_name)


    # ----- create stuff without create command ------


    def custom_create(self):
        '''
        For testing subvol auto-upgrade, Create v2 subvol without using
        'subvolume create' command.
        '''
        self.mount.run_shell(f'mkdir -p {self.mnt_path[1:]}')
        self.handle_create_opts()
        # 755 so that files can accessed without unnecessary hassles
        self.mount.run_shell(f'chmod -R 755 {self.mnt_path[1:]}')

        self.mount.write_file(self.meta_path[1:], dedent(f'''\
            [GLOBAL]
            version = 2
            type = subvolume
            path = {self.mnt_path}
            state = complete'''), sudo=True)

        for i in range(1, 6):
            path = f'{self.mnt_path[1:]}/file-{i}'
            self.mount.run_shell(f'dd if=/dev/zero of={path} bs=1M count=1')

    def custom_create_snap(self, snap_name=None):
        '''
        For testing subvol auto-upgrade, create v1 snap subvol without using
        'subvolume snapshot create' command.
        '''
        snap_name = snap_name if snap_name else gen_subvol_snap_name()

        self.mount.run_libcephfs_pybind_code(dedent(f"""
        cephfs.mksnap('{self.get_snap_base_path()}', '{snap_name}', 0o755)
        """))

        snap_path = self.build_and_get_snap_path(snap_name)
        # verify that snap was created
        self.mount.run_shell(f'stat {snap_path[1:]}')

        self.snaps.append(snap_name)
        return snap_name


class SubvolV3(SubvolBase):
    '''
    Helper class for v3 subvolumes.
    '''

    INCAR_BASE_DIR = 'roots'
    MNT_DIR = 'mnt'


    # ----- init and its helpers -----


    def __init__(self, mon_manager=None, mount=None, volname=None, name=None,
                 uuid=None, grp_name=None, snaps=[], retained=False,
                 subvol=None):
        self._reset_paths()

        if subvol:
            assert not (mon_manager or mount or volname or name or uuid or
                        grp_name or snaps or retained)
            assert type(subvol) is SubvolV2

            super().__init__(mon_manager=subvol.mon_manager, mount=subvol.mount,
                             volname=subvol.volname, name=subvol.name,
                             uuid=subvol.uuid, grp_name=subvol.grp_name,
                             snaps=subvol.snaps, retained=subvol.retained)
            self.has_v2_snaps = True if subvol.snaps else False
            self.uuid = self.fetch_uuid() if self.has_v2_snaps else subvol.uuid
        else:
            super().__init__(mon_manager, mount=mount, volname=volname, name=name,
                             uuid=uuid, grp_name=grp_name, snaps=snaps,
                             retained=retained)
            self.has_v2_snaps = False
            self.uuuid = str(uuid4())

        self._verify_attrs()

    def _reset_paths(self):
        super()._reset_paths()

        self._incar_base_path = None
        self._meta_symlink_path = None
        self._meta_file_name = None

    def _verify_attrs(self):
        super()._verify_attrs()

        assert type(self.uuid) is str, \
                f'self.uuid = {self.uuid}'
        assert type(self.has_v2_snaps) is bool, \
                f'self.has_v2_snaps = {self.has_v2_snaps}'


    # ----- subvol helper -----


    @property
    def version(self):
        return 3

    @property
    def mnt_path(self):
        return self.get_mnt_path()


    # ----- v3 helpers -----


    @property
    def incar_base_path(self):
        if not self._incar_base_path:
            self._incar_base_path = join(self.base_path, self.INCAR_BASE_DIR)
        return self._incar_base_path

    @property
    def meta_path(self):
        if not self._meta_path:
            self._meta_path = join(self.base_path, self.meta_file_name)
        return self._meta_path

    @property
    def meta_file_name(self):
        if not self._meta_file_name:
            self._meta_file_name = f'.meta.{self.uuid}'
        return self._meta_file_name

    @property
    def meta_symlink_path(self):
        if not self._meta_symlink_path:
            self._meta_symlink_path = join(self.base_path, '.meta')
        return self._meta_symlink_path

    def get_incar_path(self, uuid=None):
        uuid = uuid if uuid else self.uuid
        return join(self.incar_base_path, self.uuid)

    def get_mnt_path(self, uuid=None):
        return join(self.get_incar_path(uuid), self.MNT_DIR)

    def get_snap_base_path(self, uuid=None):
        return join(self.get_incar_path(uuid), '.snap')

    def get_snap_path(self, snap_name, uuid=None):
        return join(self.get_snap_base_path(uuid), snap_name)

    def build_and_get_v2_snap_path(self, snap_name, uuid=None):
        return join(self.base_path, '.snap', snap_name)

    def sanity_test_snap_getpath(self, snap_name):
        if self.has_v2_snaps:
            exp_snap_path = self.build_and_get_v2_snap_path(snap_name)

        snap_path = self.snap_getpath(snap_name)
        assert snap_path == exp_snap_path, \
                f'snap_path = {snap_path} exp_snap_path = {exp_snap_path}'


    # ---- misc methods -----


    def fetch_uuid(self):
        meta_file_name = self.mount.get_shell_stdout(
                f'readlink {self.meta_symlink_path[1:]}').strip()
        uuid = meta_file_name.replace('.meta.', '')
        verify_uuid(uuid)
        return uuid

    def sanity_test_meta_file(self):
        meta_content = self.mount.get_shell_stdout('sudo cat '
                                                    f'{self.meta_path[1:]}')
        # splitting ensures we compare line by line, which prevents any
        # accidental matching due to edge cases
        meta_content = meta_content.split('\n')

        assert 'version = 3' in meta_content
        assert f'path = {self.get_mnt_path()}' in meta_content
        assert f'state = complete' in meta_content
        assert f'type = subvolume' in meta_content

        if self.has_v2_snaps:
            assert 'has_v2_snaps = True' in meta_content
