from os import path
from logging import getLogger

from cephfs import Error

from .subvolume_v2 import SubvolumeV2
from .metadata_manager import MetadataManager
from .auth_metadata import AuthMetadataManager


log = getLogger(__name__)


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

    def __init__(self, mgr, fs, spec, group, name, uuid=None):
        self.mgr = mgr
        self.fs = fs
        self.spec = spec
        self.group = group

        self.name = name
        self.uuid = uuid
        if self.uuid:
            validate_uuid(self.uuid)
        else:
            self.uuid = gen_uuid()

        self._define_basic_paths()
        self._define_md_attrs()

    def _define_basic_paths(self):
        self.subvol_path = safe_join(self.spec.subvol_base_path,
                                     self.group.name, self.name)
        self.meta_slink_path = safe_join(self.subvol_path, '.meta')
        self.roots_path = safe_join(self.subvol_path, 'roots')

        self.meta_file_name = f'.meta.{self.uuid}'.encode('utf-8')
        self.meta_path = safe_join(self.subvol_path, self.meta_file_name)

    def _define_md_attrs(self):
        log.debug(f'for subvol {self.name} loading meta {self.meta_path}')
        self.md = MetadataManager(self.fs, self.meta_path, 0o640)
        log.debug(f'meta of subvol {self.name} - self.md = {self.md}')
        self.auth_md = AuthMetadataManager(self.fs)

        if not self.path_exists(self.subvol_path):
            # can be removed?
            self.md.refresh()

    def get_incar_path(self, uuid=None):
        uuid = uuid if uuid else self.uuid
        return safe_join(self.roots_path, uuid)

    def get_incar_mnt_path(self, uuid=None):
        uuid = uuid if uuid else self.uuid
        return safe_join(self.get_incar_path(uuid), 'mnt')

    def get_incar_unlinked_path(self, uuid=None):
        uuid = uuid if uuid else self.uuid
        return safe_join(self.get_incar_path(uuid), '.unlinked')

    def get_incar_snap_base_path(self, uuid=None):
        uuid = uuid if uuid else self.uuid
        return safe_join(self.get_incar_path(uuid), self.spec.snap_base_dir)

    def get_incar_snap_path(self, snap_name, uuid=None):
        uuid = uuid if uuid else self.uuid
        return safe_join(self.get_incar_snap_base_path(uuid), snap_name)

    def get_v3_incars(self):
        return self.list_dirs(self.roots_path)

    @staticmethod
    def version():
        # this way there is literally zero chance to accidentally modify version
        # number
        return 3
