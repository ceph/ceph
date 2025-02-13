from errno import *
from os.path import basename
from logging import getLogger

from cephfs import Error, InvalidValue

from .subvolume_v2 import SubvolumeV2
from .subvolume_attrs import SubvolumeStates
from .metadata_manager import MetadataManager
from .auth_metadata import AuthMetadataManager
from ..trash import create_trashcan, open_trashcan
from ...utils import gen_uuid, verify_uuid, safe_join, to_utf8, to_str
from ...fs_util import listdir, path_exists
from ...exception import VolumeException


log = getLogger(__name__)


class PreV3Helper:
    '''
    Attritbutes/methods that makes SubvolumeV3 code compatible with SubvolumeV2,
    SubvolumeV1 and SubvolumeBase.
    '''

    @property
    def vol_spec(self):
        return self.spec

    @property
    def subvolname(self):
        return self.name

    @property
    def metadata_mgr(self):
        return self.md

    @property
    def auth_mdata_mgr(self):
        return self.auth_md

    @property
    def base_path(self):
        return self.subvol_path

    @property
    def config_path(self):
        return self.meta_path

    @property
    def trash_dir(self):
        raise RuntimeError('method trash_dir() shouldn\'t be called in '
                           'subvol v3 codebase, since it doesn\'t have a '
                           'in-subvol trash dir (which is named ".trash" in'
                           'subvol v2)')

    def create_trashcan(self):
        raise RuntimeError('method create_trashcan() shouldn\'t be called in '
                           'subvol v3 codebase, since it doesn\'t have a '
                           'in-subvol trash dir (which is named ".trash" in'
                           'subvol v2)')

    # TODO: base dir should be deleted in subvol v3 too when no snaps are
    # retained on any incarnation, right?
    def trash_base_dir(self):
        # code under _trash_subvol_path can be move here technically but this
        # extra layer of call has been added to indicate that in subvol v3
        # terms
        self.trash_subvol_dir()


class SubvolHelper:
    '''
    Convenient helpers for fs_util.py functions.
    '''

    def list_dirs(self, path):
        return listdir(self.fs, path)

    def path_exists(self, path):
        return path_exists(self.fs, path)


class SubvolumeV3(SubvolumeV2):
    '''
    Code for v3 subvolume.

    /volumes/_nogroup/subvol123/roots/<UUIDs>/mnt
                                                ^ self.get_incar_mnt_path()
                                         ^ self.get_incar_uuid_path()
                                 ^ self.roots_path
                         ^ self.subvol_path

    /volumes/_nogroup/subvol123/.meta
                                 ^ self.meta_symlink_path, points to current
                                   incar's meta

    /volumes/_nogroup/subvol123/.meta.<UUID>
                                  ^ self.meta_path

    /volumes/_nogroup/subvol123/roots/<UUIDs>/.snap/snap123
                                                      ^ self.get_incar_snap_path()
                                                ^ self.get_incar_snap_base_path()

    /volumes/_nogroup/subvol123/<UUID>/.snap/snap123
                                              ^ self.v2.get_snap_path()
                                        ^ self.v2.snap_base_path
    '''

    @staticmethod
    def version():
        # this way the chance of accidentally modifying the version number is
        # literally zero.
        return 3


    # ----- init and its helpers methods -----


    def __init__(self, mgr, fs, spec, group, name, uuid=None):
        self.mgr = mgr
        self.fs = fs
        self.spec = spec
        self.group = group

        self.name = name
        self.uuid = uuid
        if self.uuid:
            verify_uuid(self.uuid)
        else:
            # will be set in _create()
            self.uuid = None

    def _define_basic_paths(self):
        self.subvol_path = safe_join(self.spec.subvol_base_path,
                                     self.group.name, self.name)
        self.meta_symlink_path = safe_join(self.subvol_path, '.meta')
        self.roots_path = safe_join(self.subvol_path, 'roots')

        self.meta_file_name = to_utf8(f'.meta.{self.uuid}')
        self.meta_path = safe_join(self.subvol_path, self.meta_file_name)

    def _define_md_attrs(self):
        log.debug(f'for subvol {self.name} loading meta {self.meta_path}')
        self.md = MetadataManager(self.fs, self.meta_path, 0o640)
        log.debug(f'meta of subvol {self.name}, self.md = {self.md}')
        self.auth_md = AuthMetadataManager(self.fs)

        if not self.path_exists(self.subvol_path):
            # can be removed?
            self.md.refresh()


    # ----- basic helper methods for v3 incars -----


    # incar = incarnation
    def get_incar_path(self, uuid=None):
        uuid = uuid if uuid else self.uuid
        return safe_join(self.roots_path, uuid)

    def get_incar_mnt_path(self, uuid=None):
        uuid = uuid if uuid else self.uuid
        return safe_join(self.get_incar_path(uuid), 'mnt')

    # deact = deactivated
    def get_incar_deact_path(self, uuid=None):
        uuid = uuid if uuid else self.uuid
        return safe_join(self.get_incar_path(uuid), '.deactivated')

    def get_incar_snap_base_path(self, uuid=None):
        uuid = uuid if uuid else self.uuid
        return safe_join(self.get_incar_path(uuid), self.spec.snap_base_dir)

    def get_incar_snap_path(self, snap_name, uuid=None):
        uuid = uuid if uuid else self.uuid
        return safe_join(self.get_incar_snap_base_path(uuid), snap_name)

    def get_v3_incars(self):
        return self.list_dirs(self.roots_path)


    # ----- methods for subvol creation and opening/discovery -----


    def _remove_on_failure(self, retained):
        # current incarnation path
        curr_incar_path = self.get_incar_path()

        if not retained:
            log.info(f'removing subvol: {self.name}')
            self.remove(internal_cleanup=True)

        try:
            log.info(f'removing subvol incar path: {to_str(curr_incar_path)}')
            self.fs.rmdir(curr_incar_path)
        except Error as e:
            raise VolumeException(e)

    def mark_subvolume(self):
        '''
        Set vxattr ceph.dir.subvolume to 1 on the incar/UUID path.
        '''
        xattr = 'ceph.dir.subvolume'
        try:
            # MDS treats this as a no-op for already marked subvolume
            self.fs.setxattr(self.get_incar_path(), xattr, '1', 0)
        except InvalidValue:
            raise VolumeException(EINVAL, f'invalid value for "{xattr}"')
        except Error as e:
            raise VolumeException(e)

    def set_meta_symlink(self):
        if not self.path_exists(self.meta_path):
            assert False, \
                (f'meta file for current incar is missing, it is to be created '
                 f'first. self.meta_path = {self.meta_path}')

        assert self.uuid in self.meta_file_name, \
            (f'self.meta_file_name = {self.meta_file_name} '
             f'self.uuid = {self.uuid}')

        assert self.meta_file_name == basename(self.meta_path), \
            (f'self.meta_file_name = {self.meta_file_name} '
             f'self.meta_path = {self.meta_path}')

        if self.path_exists(self.meta_symlink_path, follow_symlink=False):
            self.fs.unlink(self.meta_symlink_path)
        self.fs.symlink(self.meta_file_name, self.meta_symlink_path[1:])

    def create_or_update_meta_file(self, subvol_type):
        SubvolumeV2.create_or_update_meta_file(self, subvol_type)

        self.set_meta_symlink()

    def _create(self, mode, attrs, subvol_type, auth=True):
        assert self.uuid is None
        self.uuid = gen_uuid()
        self._define_basic_paths()
        self._define_md_attrs()

        if not self.path_exists(self.group.path):
            self.fs.mkdirs(self.group.path, self.spec.DEFAULT_MODE)
        self.fs.mkdirs(self.get_incar_mnt_path(), mode)

        self.mark_subvolume()
        self.set_attrs(self.get_incar_mnt_path(), attrs)

        self.create_or_update_meta_file(subvol_type)
        if auth:
            # Create the subvolume metadata file which manages auth-ids if it
            # doesn't exist
            self.auth_md.create_subvolume_metadata_file(self.group.name,
                                                        self.name)


    # ----- methods for subvol removal -----


    def trash_subvol_dir(self):
        create_trashcan(self.fs, self.spec)

        with open_trashcan(self.fs, self.spec) as trashcan:
            trashcan.dump(self.subvol_path)

    @property
    def has_pending_purges(self):
        # since there is not in-subvol ".trash" dir in subvol v3, this method
        # should always return False
        return False
