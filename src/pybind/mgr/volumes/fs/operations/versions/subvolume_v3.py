from os.path import basename
from logging import getLogger
from typing import Final

from .subvolume_v2 import SubvolumeV2
from .metadata_manager import MetadataManager
from .auth_metadata import AuthMetadataManager
from ...utils import verify_uuid, safe_join, to_utf8
from ...fs_util import listdir, path_exists


log = getLogger(__name__)


META_FILE_NAME: Final = to_utf8('.meta')
# incar = incarnation
INCAR_BASE_DIR: Final = to_utf8('roots')
RETAINED_DIR: Final = to_utf8('retained')
MNT_DIR: Final = to_utf8('mnt')


class PreV3Helper:
    '''
    Attritbutes/methods that makes SubvolumeV3 code compatible with SubvolumeV2,
    SubvolumeV1 and SubvolumeBase.
    '''

    def __init__(self, fs, spec):
        self.fs = fs
        self.spec = spec

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
    def config_path(self):
        return self.meta_path


class SubvolHelper:
    '''
    Convenient helpers for fs_util.py functions.
    '''

    def __init__(self, fs, spec):
        self.fs = fs
        self.spec = spec

    def list_dirs(self, path):
        return listdir(self.fs, path)

    def path_exists(self, path):
        return path_exists(self.fs, path)


class SubvolumeV3(SubvolumeV2):
    '''
    Code for v3 subvolumes.
    '''

    VERSION: Final = 3


    # ----- init and related helper methods -----


    def __init__(self, mgr, fs, spec, group, name, uuid=None):
        super().__init__(fs, spec)

        self.mgr = mgr
        self.fs = fs
        self.spec = spec
        self.group = group

        self.name = name
        self.uuid = uuid
        if self.uuid:
            verify_uuid(self.uuid)

        self._reset_basic_attrs()

    def _reset_basic_attrs(self):
        self._reset_basic_paths()
        self._reset_md_attrs()

    def _reset_basic_paths(self):
        self._base_path = None
        self._incar_base_path = None
        self._meta_symlink_path = None

        self._meta_file_name = None
        self._meta_path = None

    def _reset_md_attrs(self):
        self._md = None
        self._auth_md = None


    # ----- basic subvol helper -----


    @property
    def base_path(self):
        if not self._base_path:
            self._base_path = safe_join(self.spec.VOL_BASE_PATH,
                                        self.group.name, self.name)
        return self._base_path

    @property
    def incar_base_path(self):
        if not self._incar_base_path:
            self._incar_base_path = safe_join(self.base_path, INCAR_BASE_DIR)
        return self._incar_base_path

    @property
    def meta_file_name(self):
        if not self._meta_file_name:
            self._meta_file_name = META_FILE_NAME + to_utf8('.') + self.uuid
        return self._meta_file_name

    @property
    def meta_path(self):
        if not self._meta_path:
            self._meta_path = safe_join(self.base_path, self.meta_file_name)
        return self._meta_path

    @property
    def meta_symlink_path(self):
        if not self._meta_symlink_path:
            self._meta_symlink_path = safe_join(self.base_path, META_FILE_NAME)
        return self._meta_symlink_path

    @property
    def md(self):
        if not self._md:
            log.debug(f'for subvol {self.name} loading meta {self.meta_path}')
            self._md = MetadataManager(self.fs, self.meta_path, 0o640)
        return self._md

    @property
    def auth_md(self):
        if not self._auth_md:
            log.debug(f'meta of subvol {self.name}, self.md = {self.md}')
            self._auth_md = AuthMetadataManager(self.fs)
        return self._auth_md


    # ----- basic helper methods for v3 incars -----


    def get_incar_path(self, uuid=None):
        uuid = uuid if uuid else self.uuid
        return safe_join(self.incar_base_path, uuid)

    def get_meta_file_name(self, uuid=None):
        uuid = uuid if uuid else self.uuid
        return META_FILE_NAME + to_utf8('.') + uuid

    def get_meta_file_path(self, uuid=None):
        return safe_join(self.base_path, self.get_incar_meta_file_name(uuid))

    def get_mnt_path(self, uuid=None):
        return safe_join(self.get_incar_path(uuid), MNT_DIR)

    def get_retained_path(self, uuid=None):
        return safe_join(self.get_incar_path(uuid), RETAINED_DIR)

    def get_snap_base_path(self, uuid=None):
        return safe_join(self.get_incar_path(uuid), self.spec.SNAP_BASE_DIR)

    def get_snap_path(self, snap_name, uuid=None):
        return safe_join(self.get_snap_base_path(uuid), snap_name)

    def get_v3_incars(self):
        return self.list_dirs(self.incar_base_path)
