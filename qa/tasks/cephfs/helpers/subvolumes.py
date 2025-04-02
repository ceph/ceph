'''
Helper code for operating on subvolume v3.
'''
from logging import getLogger
from errno import ENOENT
from os.path import join, basename, dirname
from uuid import uuid4, UUID
from json import loads

from tasks.cephfs.helpers.misc import (gen_subvol_name, gen_subvol_grp_name,
                                       gen_subvol_snap_name)
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.ceph_manager import RunCephCmd


log = getLogger(__name__)


VOL_BASE_PATH = '/volumes'
DEFAULT_GRP_NAME = '_nogroup'
META_FILE_NAME = '.meta'
SNAP_BASE_DIR = '.snap'


def gen_uuid():
    return str(uuid4())


def verify_uuid(uuid):
    assert type(uuid) is str, \
        f'uuid = {uuid} type(uuid) = {type(uuid)}'

    try:
        UUID(uuid, version=4)
    except:
        # just being explicit about exception being raised here
        raise


class SubvolAutoUpgrade(CephFSTestCase):
    '''
    Helpers for testing subvol auto-upgrades.
    '''

    CLIENTS_REQUIRED = 2

    def setUp(self):
        super().setUp()

        # mount through which tests will create, auto-upgrade and run sanity
        # tests for subvol.
        self.admin_mount = self.mount_a
        # mount through which tests will mimic subvol users by constantly
        # writing files to the subvol
        self.user_mount = self.mount_b

        # removing these default names to avoid confusion reader and potential
        # mixup which can be fatal for test code.
        del self.mount_a, self.mount_b

    def tearDown(self):
        self.show_cephfs_dir_tree()
        if hasattr(self, 'writer'):
            self.writer.stop()

        super().tearDown()

    def show_cephfs_dir_tree(self):
        '''
        Incase of test failure, log dir tree for mount since it will help
        understand failure.
        '''
        if hasattr(self, '_outcome') and hasattr(self._outcome, 'result'):
            result = self._outcome.result
            for test, _ in result.errors + result.failures:
                if test == self:
                    failed = True
                    break
            else:
                failed = False

        if failed:
            self.user_mount.run_shell('tree -d ./')
            self.user_mount.run_shell('tree -L 2 ./')

    def raise_method(self, msg):
        '''
        Pass this method to GenIOLoad instance to cause tests too to fail when
        writer threads fail.
        '''
        log.error(msg)
        raise RuntimeError(msg)

    def wait_till_trash_is_empty(self, timeout=60):
        self.admin_mount.wait_for_dir_empty('volumes/_deleting', timeout)


class SubvolOpsHelper(RunCephCmd):
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


class SubvolBase(SubvolOpsHelper):
    '''
    Base class for v3 subvol helper classes.
    '''


    # ----- init and related methods -----


    def __init__(self, mon_manager=None, mount=None, volname=None, name=None,
                 uuid=None, grp_name=None, snaps=[], retained=False):
        '''
        :param grp_name: should be None or True.
                            * True implies set self.grp_name to generated name
                            * None/False implies set self.grp_name to no/default group
        :param uuid: should be str, None, or True.
                          * str implies set self.uuid to passed str,
                          * True implies set self.uuid to generated uuid
                          * None/False implies set self.uuid to None
        '''
        self.mon_manager = mon_manager
        self.mount = mount
        super().__init__(self.mon_manager)

        self.volname = volname
        self.grp_name = gen_subvol_grp_name() if grp_name is True else grp_name
        self.name = name if name else gen_subvol_name()

        self.uuid = uuid
        if self.uuid is True:
            self.uuid = gen_uuid()
        elif self.uuid in (None, False):
            self.uuid = None
        elif type(self.uuid) is str:
            self.uuid = uuid
        else:
            assert False

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

    def _reset_paths(self):
        self._base_path = None
        self._meta_path = None


    # ----- subvol basics -----


    @property
    def version(self):
        raise NotImplementedError('should\'ve been overridden')

    def gen_base_path(self):
        grp_name = self.grp_name if self.grp_name else DEFAULT_GRP_NAME
        return join(VOL_BASE_PATH, grp_name, self.name)


    # ----- helpers for subvol ops -----


    def create(self, opts=''):
        if 'mode' not in opts:
            opts = opts + ' ' + ' --mode=777'

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
        snap_name = super().snap_create(self.volname, self.name, snap_name,
                                        self.grp_name)
        return snap_name

    def snap_rm(self, snap_name):
        super().snap_rm(self.volname, self.name, snap_name, self.grp_name)

    def snap_getpath(self, snap_name):
        return super().snap_getpath(self.volname, self.name, snap_name,
                                    self.grp_name).strip()

    def snap_ls(self):
        return super().snap_ls(self.volname, self.name, self.grp_name)


    # ----- helpers for operating on subvol/subvol mnt path -----


    def write_files(self, path=None, data='00000', num=1, ifx=''):
        if path:
            if self.gen_mnt_path() not in path:
                path = path[1:] if path[0] == '/' else path
                path = join('/', self.gen_mnt_path(), path)
        else:
            path = './' + self.gen_mnt_path()

        for i in range(num):
            file_name = f'file-{ifx}-{i}' if ifx else f'file-{i}'
            file_path = join(path, file_name)
            self.mount.write_file(file_path, data, sudo=True)


    # ----- helpers for custom create -----


    def apply_create_options(self, options=''):
        if not options:
            return

        assert type(options) is str
        options = options.split()
        xattrs = {}

        if '--size' in options:
            size = options[options.index('--size') + 1]
            # testing that value passed to "--size" is indeed a number
            size = int(size)
            xattrs['ceph.quota.max_bytes'] = str(size)

        if '--namespace-isolated' in options:
            grp_name = self.grp_name if self.grp_name else DEFAULT_GRP_NAME
            xattrs['ceph.dir.layout.pool_namespace'] = \
            f'fsvolumens_{grp_name}_{self.name}'

        for k, v in xattrs.items():
            self.mount.run_shell(f'setfattr {k} {v} ./{self.gen_mnt_path()}')


    # ----- helpers for sanity testing ------


    # XXX: do _NOT_ open subvol via volumes plugin commands.
    #
    # this is duplication to prevent accidental subvol opening as it will
    # trigger auto-upgrade for the subvol before it's desired.
    def verify_custom_made_subvol(self):
        ls_stdout = self.ls()
        assert self.name in str(ls_stdout), \
            f'snap_name = {self.name} ls_stdout = {ls_stdout}'

    # XXX: do _NOT_ open subvol via volumes plugin commands.
    #
    # this is duplication to prevent accidental subvol opening as it will
    # trigger auto-upgrade for the subvol before it's desired.
    def verify_custom_made_snap(self, snap_name):
        snap_path = self.gen_snap_path(snap_name)
        self.mount.run_shell(f'stat {snap_path[1:]}')

    def verify_subvol_ls_cmd(self):
        ls_stdout = self.ls()
        assert self.name in str(ls_stdout), \
            f'snap_name = {self.name} ls_stdout = {ls_stdout}'

    def verify_subvol_getpath_cmd(self):
        if self.retained:
            self.negtest_ceph_cmd(f'fs subvolume getpath {self.volname} '
                                  f'{self.name}', retval=ENOENT)
            return

        sv_path = self.getpath()
        assert sv_path == self.gen_mnt_path(), \
            f'sv_path = {sv_path} self.gen_mnt_path() = {self.gen_mnt_path()}'

    def verify_snap_ls_cmd(self, snap_name):
        ls_stdout = self.snap_ls()
        assert snap_name in str(ls_stdout), \
            f'snap_name = {snap_name} ls_stdout = {ls_stdout}'

    def verify_snap_getpath_cmd(self, snap_name):
        exp_snap_path = self.gen_snap_path(snap_name)
        reported_snap_path = self.snap_getpath(snap_name)

        assert reported_snap_path == exp_snap_path, \
            (f'reported_snap_path = {reported_snap_path} '
             f'exp_snap_path = {exp_snap_path}')


class V3IncarHelper:
    '''
    Helps test/deal with v3 incarnations.
    '''

    INCAR_BASE_DIR = 'roots'
    MNT_DIR = 'mnt'
    RETAINED_DIR = '.retained'

    def __init__(self, subvol_name, uuid, snap_names=None, retained=False,
                 grp_name=None):
        self.subvol_name = subvol_name
        self.uuid = uuid
        self.grp_name = grp_name
        self.retained = retained

        self._base_path = None
        self._meta_path = None
        self._incar_path = None

        self.snap_names = snap_names
        if type(self.snap_names) is str:
            self.snap_names = [self.snap_names, ]
        elif type(self.snap_names) is list:
            pass
        elif type(self.snap_names) is tuple:
            self.snap_names = list(self.snap_names)
        elif self.snap_names in (None, [], (), ''):
            self.snap_names = []
        else:
            assert False, f'self.snap_names = {self.snap_names}'


class SubvolV3Helper(SubvolBase):
    '''
    Helper class for v3 subvolumes.
    '''

    INCAR_BASE_DIR = 'roots'
    MNT_DIR = 'mnt'
    RETAINED_DIR = '.retained'


    # ----- init and its helpers -----


    def __init__(self, mon_manager=None, mount=None, volname=None, name=None,
                 uuid=None, grp_name=None, snaps=[], retained=False,
                 subvol=None):
        self._reset_paths()
        self.all_incars = []

        assert mount and volname and not subvol

        super().__init__(mon_manager, mount=mount, volname=volname, name=name,
                         uuid=uuid if uuid else False, grp_name=grp_name,
                         snaps=snaps, retained=retained)

        #if self.snaps:
        #     self.add_incar(self.uuid, self.snaps, self.retained)
        self.retained = None
        self.snaps = None

        self._verify_attrs()

    def _reset_paths(self):
        super()._reset_paths()

        self._incar_base_path = None
        self._meta_symlink_path = None
        self._meta_file_name = None

    def _verify_attrs(self):
        super()._verify_attrs()

        assert type(self.uuid) in (str, type(None)), \
            f'self.uuid = {self.uuid}'
        assert self.snaps is None , \
            f'self.snaps = {self.snaps}'
        assert self.retained is None , \
            f'self.retained = {self.retained}'

        assert type(self.all_incars) is list, \
            f'self.all_incars = {self.all_incars}'


    # ----- subvol helper -----


    @property
    def version(self):
        return 3


    # ----- v3 helpers -----


    def get_uuid_from_path(self, path):
        if self.MNT_DIR == basename(path):
            return basename(dirname(self.gen_mnt_path()))
        elif SNAP_BASE_DIR == basename(path):
            return basename(dirname(self.gen_snap_base_path()))
        elif SNAP_BASE_DIR in path:
            return basename(dirname(dirname(self.gen_snap_base_path())))
        else:
            assert False, 'unknown type of path'

    def gen_meta_symlink_path(self):
        if not self._meta_symlink_path:
            self._meta_symlink_path = join(self.gen_base_path(), META_FILE_NAME)
        return self._meta_symlink_path

    def gen_meta_file_name(self, uuid=None):
        uuid = uuid if uuid else self.uuid
        return f'.meta.{uuid}'

    def gen_meta_path(self, uuid=None):
        return join(self.gen_base_path(), self.gen_meta_file_name(uuid))

    def gen_incar_base_path(self):
        if not self._incar_base_path:
            self._incar_base_path = join(self.gen_base_path(),
                                         self.INCAR_BASE_DIR)
        return self._incar_base_path

    def gen_incar_path(self, uuid=None):
        uuid = uuid if uuid else self.uuid
        return join(self.gen_incar_base_path(), uuid)

    def gen_mnt_path(self, uuid=None):
        return join(self.gen_incar_path(uuid), self.MNT_DIR)

    def gen_retained_path(self, uuid=None):
        return join(self.gen_incar_path(uuid), self.RETAINED_DIR)

    def gen_snap_base_path(self, uuid=None):
        return join(self.gen_incar_path(uuid), SNAP_BASE_DIR)

    def gen_snap_path(self, snap_name, uuid=None):
        return join(self.gen_snap_base_path(uuid), snap_name)

    def fetch_uuid(self):
        meta_file_name = self.mount.get_shell_stdout(
                f'readlink ./{self.gen_meta_symlink_path()}').strip()
        uuid = meta_file_name.replace('.meta.', '')
        verify_uuid(uuid)
        return uuid


    # ----- incar helper -----


    def add_incar(self, uuid, snap_names=None, retained=False):
        incar = self.get_incar(uuid)
        assert not incar, \
            'incar already present in self.all_incars'

        self.all_incars.append(V3IncarHelper(self.name, uuid, snap_names,
                                             retained))

    def get_incar(self, uuid=None, snap_name=None):
        assert not snap_name or not uuid

        if snap_name:
            for incar in self.all_incars:
                if snap_name in incar.snap_names:
                    return incar
        else:
            verify_uuid(uuid)
            uuid = uuid if uuid else self.uuid
            for incar in self.all_incars:
                if incar.uuid == uuid:
                    return incar

    def rm_incar(self, uuid=None, incar=None):
        if incar:
            self.all_incars.remove(incar)
        elif incar := self.get_incar(uuid):
            self.all_incars.remove(incar)
        else:
            assert False, 'attempted to remove an incar that does not exist'


    # ----- override subvol ops methods -----


    def create(self, opts=''):
        super().create(opts)

        self.uuid = self.fetch_uuid()
        if self.uuid in self.all_incars:
            raise RuntimeError(f'uuid is not unique, self.uuid = {self.uuid}, '
                               f'self.all_incars = {self.all_incars}')

        if incar := self.get_incar(self.uuid):
            incar.retained = False
        else:
            self.add_incar(self.uuid)

    def rm(self, opts=''):
        incar = self.get_incar(self.uuid)
        assert incar, \
            'incar is being removed but it is absent in self.all_incars!'

        super().rm(opts)

        if incar and '--retain-snapshots' in opts:
            assert incar.retained == False
            incar.retained = True

    def snap_create(self, snap_name=None):
        snap_name = super().snap_create(snap_name)

        incar = self.get_incar(self.uuid)
        incar.snap_names.append(snap_name)
        return snap_name

    def snap_rm(self, snap_name):
        incar = self.get_incar(snap_name=snap_name)

        super().snap_rm(snap_name)

        incar.snap_names.remove(snap_name)
        if incar.retained and len(incar.snap_names) == 0:
            self.rm_incar(incar=incar)


    # ----- testing subvol -----


    def verify_uuid(self, subvol_mnt_path=None):
        if not subvol_mnt_path:
            subvol_mnt_path = self.getpath()

        uuid = self.get_uuid_from_path(subvol_mnt_path)
        verify_uuid(uuid)
        self.uuid = self.fetch_uuid()

    def verify_layout(self):
        self.mount.verify_path_is_dir(self.gen_base_path())
        self.mount.verify_path_is_symlink(self.gen_meta_symlink_path())
        self.mount.verify_path_is_reg(self.gen_meta_path())
        self.mount.verify_path_is_dir(self.gen_incar_base_path())

        for incar in self.all_incars:
            self.mount.verify_path_is_reg(self.gen_meta_path(incar.uuid))
            self.mount.verify_path_is_dir(self.gen_incar_path(incar.uuid))
            if incar.retained:
                self.mount.verify_path_is_dir(self.gen_retained_path(incar.uuid))
            else:
                self.mount.verify_path_is_dir(self.gen_mnt_path(incar.uuid))

            if not incar.snap_names:
                return
            for snap_name in incar.snap_names:
                self.mount.verify_path_is_dir(self.gen_snap_path(snap_name,
                                                                 incar.uuid))

    def verify_meta_file(self):
        meta_content = self.mount.get_shell_stdout('sudo cat '
                                                   f'./{self.gen_meta_path()}')
        # splitting ensures we compare line by line, which prevents any
        # accidental matching due to edge cases
        meta_content = meta_content.split('\n')

        assert 'version = 3' in meta_content
        assert 'type = subvolume' in meta_content


        incar = self.get_incar(self.uuid)
        if incar:
            if incar.retained:
                assert f'path = {self.gen_retained_path()}' in meta_content, \
                    f'self.gen_retained_path() = {self.gen_retained_path()}'
                assert 'state = snapshot-retained' in meta_content
            else:
                assert f'path = {self.gen_mnt_path()}' in meta_content, \
                    f'self.gen_mnt_path() = {self.gen_mnt_path()}'
                assert 'state = complete' in meta_content

    def verify_subvol_getpath_cmd(self):
        assert self.getpath() == self.gen_mnt_path()

    def verify_snap_getpath_cmd(self, snap_name, uuid=None, version=3):
        if version == 3:
            exp_snap_path = self.gen_snap_path(snap_name, uuid)
        else:
            assert False, \
                f'version = {version}'
 
        reported_snap_path = self.snap_getpath(snap_name)

        assert reported_snap_path == exp_snap_path, \
            (f'reported_snap_path = {reported_snap_path} '
             f'exp_snap_path = {exp_snap_path}')

    def verify_snap_ls_for_all_incars(self):
        all_snap_names = str(self.snap_ls())

        for incar in self.all_incars:
            for s in incar.snap_names:
                assert s in all_snap_names, \
                    f's = {s} curr_snap_names = {all_snap_names}'

    def verify_snap_getpath_for_all_incars(self):
        for incar in self.all_incars:
            for s in incar.snap_names:
                self.verify_snap_getpath_cmd(s, incar.uuid, 3)
