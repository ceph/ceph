from errno import *
from os.path import basename, dirname
from logging import getLogger

from cephfs import Error, InvalidValue

from .subvolume_v2 import SubvolumeV2
from .subvolume_attrs import SubvolumeStates
from .metadata_manager import MetadataManager
from .auth_metadata import AuthMetadataManager
from ..trash import create_trashcan, open_trashcan
from ...utils import gen_uuid, validate_uuid, safe_join, to_bytes
from ...fs_util import (listdir, list_snaps, statx, get_all_xattrs,
                        set_all_xattrs, path_exists, is_dir_empty)
from ...exception import (VolumeException, MetadataMgrException,
                          SubvolUpgradeError)


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

    def snapshot_base_path(self):
        return self.get_incar_snap_base_path()

    def snapshot_path(self, snap_name):
        return self.get_incar_snap_path(snap_name)

    def snapshot_data_path(self, snap_name):
        if snap_path := self.get_snap_path(snap_name):
            return snap_path

    def list_snapshots(self):
        '''
        :return: list of snap names
        :rtype: list of str
        '''
        return self.get_snap_names()

        # TODO
        # v2 raises exception if the snapshot path do not exist so do the same
        # to prevent any bugs due to difference in behaviour.
        #
        # not raising exception indeed leads to a bug: the volumes plugin fails
        # when exception is not raised by this method when it is calld by
        # do_clone() method of async_cloner.py. this is made to happen by a test
        # by deleting snapshot after running the snapshot clone cmd but before
        # the clone operation actually begins. this is done by a adding a delay
        # using mgr/volumes/snapshot_clone_delay config option.
        raise VolumeException(ENOENT, f'snapshot {snap_name} doesnt exist')

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


class SubvolHelper:
    '''
    Basic stuff for subvolumes
    '''

    def list_dirs(self, path):
        return listdir(self.fs, path)

    def is_dir_empty(self, path):
        return is_dir_empty(self.fs, path)

    def list_snaps(self, path):
        return list_snaps(self.fs, self.spec, path)

    def statx(self, path, fields):
        return statx(self.fs, path, fields)

    def path_exists(self, path):
        return path_exists(self.fs, path)

    def get_all_xattrs(self, path):
        return get_all_xattrs(self.fs, self.v2.uuid_path)

    def set_all_xattrs(self, path, xattrs):
        return set_all_xattrs(self.fs, self.path, xattrs)


class V2Helper(SubvolHelper, PreV3Helper):
    '''
    For helping SubvoolV3 with its v2 incarnation
    '''

    def __init__(self, fs=None, spec=None, subvol_path=None, uuid=None):
        self.fs = fs
        self.spec = spec
        self.subvol_path = subvol_path

        self.uuid = uuid
        if self.uuid is False:
            pass
        elif self.uuid is None:
            self.uuid = self.fetch_uuid()

        validate_uuid(self.uuid)

        self.uuid_path = safe_join(self.subvol_path, self.uuid)
        self.snap_base_path = safe_join(self.uuid_path, self.spec.snap_base_dir)

    def fetch_uuid(self):
        dentries = self.list_dirs(self.subvol_path)
        dentries.remove(b'roots')
        assert len(dentries) == 1
        validate_uuid(dentries[0])
        return dentries[0]

    def get_snap_names(self):
        if self.uuid is False:
            return []

        return self.list_snaps(self.snap_base_path)

    def get_snap_path(self, snap_name):
        if self.uuid is False:
            return None

        if snap_name in self.get_snap_names():
            return safe_join(self.snap_base_path, snap_name)

    def has_snap(self, snap_name=None):
        if self.uuid is False:
            return False

        if snap_name:
            return snap_name in self.get_snap_names()
        else:
            return not self.is_dir_empty(self.snap_base_path)


class SubvolumeV3(SubvolHelper, PreV3Helper, SubvolumeV2):
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

    def __init__(self, mgr, fs, spec, group, name, uuid=None,
                 disc_version=None):
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

        # can be removed?
        self.creating = not self.path_exists(self.subvol_path)

        if self.version() == disc_version:
            self._define_md_attrs()
            self.v2 = self._get_v2_helper()
        else:
            self._auto_upgrade(disc_version)

    def _define_basic_paths(self):
        self.subvol_path = safe_join(self.spec.subvol_base_path,
                                     self.group.name, self.name)
        self.meta_symlink_path = safe_join(self.subvol_path, '.meta')
        self.roots_path = safe_join(self.subvol_path, 'roots')

    def _define_md_attrs(self):
        self.meta_file_name = to_bytes(f'.meta.{self.uuid}')
        self.meta_path = safe_join(self.subvol_path, self.meta_file_name)

        log.debug(f'for subvol {self.name} loading meta {self.meta_path}')
        self.md = MetadataManager(self.fs, self.meta_path, 0o640)
        log.debug(f'meta of subvol {self.name}, self.md = {self.md}')
        self.auth_md = AuthMetadataManager(self.fs)

        if not self.creating:
            # can be removed?
            self.md.refresh()

    def _get_v2_helper(self, uuid=None):
        if type(uuid) in (bytes str):
            if has_v2_snaps := self.md.get_global_option('has_v2_snaps', None):
                assert has_v2_snaps is True
                v2 = V2Helper(self.fs, self.spec, self.subvol_path, uuid)
                assert v2.uuid == uuid
        elif uuid is None:
                return V2Helper(self.fs, self.spec, self.subvol_path)
        elif uuid is False:
            return V2Helper(uuid=False)
        else:
            assert False


    # ----- basic v3 stuff -----


    def get_incar_path(self, uuid=None):
        if self.v2.uuid == uuid:
            return self.v2.uuid_path

        uuid = uuid if uuid else self.uuid
        return safe_join(self.roots_path, uuid)

    def get_incar_mnt_path(self, uuid=None):
        if self.v2.uuid == uuid:
            assert False, 'v2 incar can\'t have mnt dir/path'

        uuid = uuid if uuid else self.uuid
        return safe_join(self.get_incar_path(uuid), 'mnt')

    def get_incar_unlinked_path(self, uuid=None):
        if self.v2.uuid == uuid:
            assert False, 'v2 incar can\'t have unlinked dir/path'

        uuid = uuid if uuid else self.uuid
        return safe_join(self.get_incar_path(uuid), '.unlinked')

    def get_incar_snap_base_path(self, uuid=None):
        if self.v2.uuid == uuid:
            return self.v2.get_snap_base_path

        uuid = uuid if uuid else self.uuid
        return safe_join(self.get_incar_path(uuid), self.spec.snap_base_dir)

    def get_incar_snap_path(self, snap_name, uuid=None):
        if self.v2.uuid == uuid:
            return self.v2.get_snap_path(snap_name)

        uuid = uuid if uuid else self.uuid
        return safe_join(self.get_incar_snap_base_path(uuid), snap_name)

    def get_v3_incars(self):
        return self.list_dirs(self.roots_path)

    @staticmethod
    def version():
        # this way there is literally zero chance to accidentally modify version
        # number
        return 3


    # ----- methods for subvol creation and opening/discovery -----


    def set_subvol_xattr(self):
        subvol_xattr = 'ceph.dir.subvolume'

        try:
            # MDS treats this as a no-op for already marked subvolume
            self.fs.setxattr(self.get_incar_path(), subvol_xattr, b'1', 0)
        except InvalidValue:
            raise VolumeException(EINVAL, f'invalid value for {subvol_xattr}')
        except Error as e:
            raise VolumeException(-e.args[0], e.args[1])

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
        super(SubvolumeV3, self).create_or_update_meta_file(subvol_type)

        self.set_meta_symlink()

    def _create(self, mode, attrs, subvol_type, auth=True):
        if not self.path_exists(self.group.path):
            self.fs.mkdirs(self.group.path, self.spec.DEFAULT_MODE)
        self.fs.mkdirs(self.get_incar_mnt_path(), mode)

        self.set_subvol_xattr()
        self.set_attrs(self.get_incar_mnt_path(), attrs)

        self.create_or_update_meta_file(subvol_type)
        self._define_md_attrs()
        if auth:
            # Create the subvolume metadata file which manages auth-ids if it
            # doesn't exist
            self.auth_md.create_subvolume_metadata_file(self.group.name,
                                                        self.name)


    # ----- methods for subvol deletion -----


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

    def trash_subvol_dir(self):
        create_trashcan(self.fs, self.vol_spec)

        with open_trashcan(self.fs, self.vol_spec) as trashcan:
            trashcan.dump(self.subvol_path)

    # TODO: base dir should be deleted in subvol v3 too when no snaps are
    # retained on any incarnation, right?
    def trash_base_dir(self):
        # code under _trash_subvol_path can be move here technically but this
        # extra layer of call has been added to indicate that in subvol v3
        # terms
        self.trash_subvol_dir()

    def trash_subvol_dir(self):
        create_trashcan(self.fs, self.spec)

        with open_trashcan(self.fs, self.spec) as trashcan:
            trashcan.dump(self.subvol_path)

    def trash_uuid_dir(self, uuid):
        create_trashcan(self.fs, self.spec)

        with open_trashcan(self.fs, self.spec) as trashcan:
            trashcan.dump(self.get_incar_path(uuid))

    @property
    def has_pending_purges(self):
        # since there is not in-subvol ".trash" dir in subvol v3, this method
        # should always return False
        return False

    def update_meta_file_after_retain(self):
        self.md.remove_section(MetadataManager.USER_METADATA_SECTION)

        self.md.update_global_section('path', self.unlinked_path)
        self.md.update_global_section('state',
                                      SubvolumeStates.STATE_RETAINED.value)

        self.md.flush()


    # ----- helper methods for snap code -----


    def get_snap_names(self, v3_uuid=None, v2=None, v3=None):
        assert v2 in (True, None) and v3 in (True, None)
        if v2 and v3_uuid and v3 is None:
            assert False
        if v2 is None and v3 is None:
            v2 = False if v3_uuid else True
            v3 = True

        names = []
        if v2 and v3:
            # doesn't matter if v2 is True or False since v3_uuid is passed.
            if v3_uuid and v3:
                return self.get_snap_names(v3_uuid)
            else:
                if self.v2:
                    names += self.v2.get_snap_names()
                names += self.get_snap_names(v3=True)
                return names
        elif v2 and not v3:
            if v3_uuid:
                # getting v2 snap names from v3 incar/uuid makes no sense
                assert False
            else:
                if self.v2:
                    return self.v2.get_snap_names()
        elif not v2 and v3:
            if v3_uuid:
                path = self.get_incar_snap_base_path(v3_uuid)
                return self.list_snaps(path)
            else:
                v3_snap_names = []
                for uuid in self.get_v3_incars():
                    path = self.get_incar_snap_base_path(uuid)
                    v3_snap_names += self.list_snaps(path)
                return v3_snap_names
        else:
            # should've never reached here
            assert False

        return []

    # Listing all snaps can be expensive due to multiple snaps in multiple
    # incarnations. So, don't list all snaps unnecessarily, use this instead.
    def has_snap(self, snap_name=None, v3_uuid=None, v2=None, v3=None):
        assert v2 in (True, None) and v3 in (True, None)
        if v2 and v3_uuid and v3 is None:
            assert False
        if v2 is None and v3 is None:
            v2 = False if v3_uuid else True
            v3 = True

        if v2 and v3:
            if snap_name and v3_uuid:
                return self.has_snap(snap_name, v3_uuid)
        elif v2 and not v3:
            if not self.v2:
                # no v2 incar, so no snaps in it
                return False

            if v3_uuid:
                # v2 snap cant be in v3 incar
                assert False

            # doesn't matter is snap_name is None or not, underlying method will
            # handle it.
            return self.v2.has_snap(snap_name)
        elif not v2 and v3:
            if snap_name and v3_uuid:
                path = self.get_incar_snap_path(v3_uuid, snap_name)
                return self.path_exists(path)
            elif snap_name and not v3_uuid:
                v3_uuid = self.get_incar_for_snap_name(snap_name)
                path = self.get_incar_snap_path(v3_uuid, snap_name)
                return self.path_exists(path)
            elif not snap_name and v3_uuid:
                path = self.get_incar_snap_base_path(v3_uuid)
                return not self.dir_is_empty(path)
            elif not snap_name and not v3_uuid:
                for v3_uuid in self.get_v3_incars():
                    path = self.get_incar_snap_base_path(v3_uuid)
                    return not self.dir_is_empty(path)
            else:
                # shouldn't have reached here
                assert False

        return False

    def get_incar_for_snap_name(self, snap_name, v2=None, v3=None):
        assert v2 in (True, None) and v3 in (True, None)
        if v2 is None and v3 is None:
            v2 = v3 = True

        if v2 and v3:
            if uuid := self.get_incar_for_snap_name(snap_name, v2=True):
                return uuid
            elif uuid := self.get_incar_for_snap_name(snap_name, v3=True):
                return uuid
        elif v2 and not v3:
            return self.v2.uuid if self.has_snap(snap_name, v2=True) else None
        elif not v2 and v3:
            for uuid in self.get_v3_incars():
                path = self.get_incar_snap_base_path(uuid)
                if snap_name in self.list_dirs(path):
                    return uuid

        return None

    # ----- methods for subvol snaps creation, getpath and removal -----


    # XXX: self.uuid can be none if snap is absent but don't raise any exception
    # in this case since command's behaviour is expected to be idempotent.
    def get_snap_path(self, snap_name):
        '''
        Gets snap path regardless of where it's present: v3 or v2.
        '''
        snap_path = None
        if uuid := self.get_incar_for_snap_name(snap_name, v2=True):
            snap_path = self.v2.get_snap_path(snap_name)
        elif uuid := self.get_incar_for_snap_name(snap_name, v3=True):
            snap_path = self.get_incar_snap_path(uuid, snap_name)

        return snap_path if self.path_exists(snap_path) else None

    def create_snapshot(self, snap_name):
        if self.has_snap(snap_name):
            raise VolumeException(EEXIST,
                                  f'subvol snap {snap_name} already exists')

        super(SubvolumeV3, self).create_snapshot(snap_name)

    # TODO, XXX: check if v2 incar snap was deleted and therefore v2 incar has
    # been deleted completely
    def remove_snapshot(self, snap_name, force):
        if not (snap_path := self.get_snap_path(snap_name)):
            raise VolumeException(ENOENT,
                                  f'subvol snap {snap_name} doesnt exist')

        vol_exc = VolumeException(ESTALE, 'release lock and queue async purge '
                                          'job')
        super(SubvolumeV2, self).remove_snapshot(snap_name, force, snap_path)
        if self.retained:
            if not self.has_snap():
                self.trash_base_dir()
                raise vol_exc
        else:
            uuid = basename(dirname(dirname(snap_path)))
            if not self.has_snap(uuid, v2=True):
                self.trash_uuid_dir(uuid)
                self.md.remove_global_option('has_v2_snaps')
                self.md.flush()
                self.v2 = V2Helper(uuid=False)
                raise vol_exc
            elif not self.has_snap(uuid, v3=True):
                self.trash_uuid_dir(uuid)
                raise vol_exc
        return False

    def update_meta_file_after_retain(self):
        self.metadata_mgr.remove_section(MetadataManager.USER_METADATA_SECTION)

        self.metadata_mgr.update_global_section('state', self.unlinked_path)
        self.metadata_mgr.update_global_section('path',
                                                SubvolumeStates.STATE_RETAINED.value)

        self.metadata_mgr.flush()

    def deactivate_curr_incar(self):
        self.fs.rename(self.get_mnt_path(), self.unlinked_dir)

    def remove_but_retain_snaps(self):
        assert self.state != SubvolumeStates.STATE_RETAINED

        try:
            self.update_meta_file_after_retain()
            self.deactivate_curr_incar()

            self.auth_md.delete_subvolume_metadata_file(self.group.name,
                                                        self.name)
        except MetadataMgrException as e:
            log.error(f"failed to write config: {e}")
            raise VolumeException(e.args[0], e.args[1])


    # ----- methods for subvol snapshot clone operations -----


    # TOOD: remove this method?
    @property
    def purgeable(self):
        return False if not self.retained or self.get_snap_names() else True


    # ----- methods for subvol auto-upgrade -----


    def _auto_upgrade(self, disc_version):
        if disc_version == 2:
            self._upgrade_from_v2()
        else:
            assert False, \
                f'disc_version = {disc_version}'


    # ----- methods for subvol auto-upgrade from v2 -----


    def _upgrade_v2_to_v3_layout(self):
        log.info(f'upgrading subvol {self.name} from v2 to v3, '
                 'upgrading its layout...')

        try:
            uid, gid, mode = self.statx(self.v2.uuid_path, ('uid', 'gid',
                                                            'mode'))
            sv_xattrs = self.get_all_xattrs(self.v2.uuid_path)

            self.fs.mkdirs(self.uuid_path, 0o755)
            if self.v2.has_snaps:
                self.fs.mkdir(self.mnt_path, 0o755)
            else:
                self.fs.rename(self.v2.uuid_path, self.mnt_path)

            self.fs.chown(self.mnt_path, uid, gid)
            self.fs.chmod(self.mnt_path, mode)

            if sv_xattrs:
                self.set_all_xattrs(self.mnt_path, sv_xattrs)

            self.fs.rename(self.v2.meta_path, self.meta_path)
            self.fs.symlink(self.meta_file_name, self.meta_symlink_path[1:])

            for path in (self.meta_path, self.meta_symlink_path):
                self.fs.chown(path, 0, 0)
                self.fs.chmod(path, 644)
        except Error as e:
            raise SubvolUpgradeError(-e.args[0],
                                     f'error upgrading subvol {self.name} '
                                     f'from v2 to v3. exception raised: {e}')

        log.info(f'layout upgrade for subvol {self.name} was successful, '
                 'updating its metadata file...')

    def _update_v2_to_v3_meta(self):
        log.info(f'updating meta since subvol {self.name} has been '
                 'auto-upgraded from v2 to v2')

        try:
            self.md.refresh()

            self.md.update_global_section('version', self.version())
            self.md.update_global_section('path', self.mnt_path)
            if self.v2.has_snap():
                self.md.update_global_section('has_v2_snaps', True)

            self.md.flush()
        except MetadataMgrException as e:
            raise VolumeException(-e.args[0],
                                  'error updating subvol metadata during '
                                  'subvol upgrade from v2 to v3')

        log.info(f'updating meta for subvol {self.name} after auto-upgrade '
                 'was successful')

    def _upgrade_from_v2(self):
        self.v2 = self._get_v2_helper(self.uuid)
        if self.v2.has_snap():
            self.uuid = gen_uuid()

        self._upgrade_v2_to_v3_layout()
        self._define_md_attrs()
        self._update_v2_to_v3_meta()

        # re-define v2 helper if v2 incar is moved.
        if self.uuid == self.v2.uuid:
            self.v2 = self._get_v2_helper(False)
        self.clean_stale_snapshot_metadata()
