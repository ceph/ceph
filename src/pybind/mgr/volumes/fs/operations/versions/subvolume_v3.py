from errno import EINVAL, EAGAIN, ENOTEMPTY, ENOENT
from os.path import basename, dirname
from logging import getLogger
from typing import Final
from datetime import datetime

from cephfs import Error, InvalidValue, ObjectNotFound

from .subvolume_v2 import SubvolumeV2
from .subvolume_attrs import SubvolumeTypes, SubvolumeStates
from .metadata_manager import MetadataManager
from .auth_metadata import AuthMetadataManager
from .op_sm import SubvolumeOpSm
from ..trash import create_trashcan, open_trashcan
from ..clone_index import open_clone_index
from ..snapshot_util import mksnap, rmsnap
from ..template import SubvolumeOpType
from ...utils import gen_uuid, verify_uuid, safe_join, to_utf8b, to_str
from ...fs_util import listdir, path_exists, listsnaps, is_inherited_snap
from ...exception import (VolumeException, MetadataMgrException, OpSmException,
                          IndexException)


log = getLogger(__name__)


META_FILE_NAME: Final = to_utf8b('.meta')
# incar = incarnation
INCAR_BASE_DIR: Final = to_utf8b('roots')
RETAINED_DIR: Final = to_utf8b('.retained')
MNT_DIR: Final = to_utf8b('mnt')


class SubvolHelper:
    '''
    Convenient helpers for fs_util.py functions.
    '''

    def __init__(self, fs, spec):
        self.fs = fs
        self.spec = spec

    def list_dirs(self, path):
        return to_utf8b(listdir(self.fs, path))

    def path_exists(self, path, follow_symlink=True):
        return path_exists(self.fs, path)

    def list_snaps(self, path):
        return to_utf8b(listsnaps(self.fs, self.spec, path))

    def mksnap(self, snap_path):
        return mksnap(self.fs, snap_path)

    def rmsnap(self, snap_path):
        return rmsnap(self.fs, snap_path)


class SubvolumeV3(SubvolHelper, SubvolumeV2):
    '''
    Code for v3 subvolumes.
    '''

    VERSION = 3


    # ----- init and related helper methods -----


    def __init__(self, mgr, fs, spec, group, name, uuid=None):
        super().__init__(fs, spec)

        self.mgr = mgr
        self.fs = fs
        self.spec = spec
        self.group = group

        self.name = to_utf8b(name)
        self.uuid = to_utf8b(uuid) if uuid else gen_uuid()
        verify_uuid(self.uuid)

        self.cmode = None
        self.user_id = None
        self.group_id = None

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
            self._meta_file_name = META_FILE_NAME + '.' + self.uuid
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


    def gen_meta_file_name(self, uuid=None):
        uuid = uuid if uuid else self.uuid
        return META_FILE_NAME + '.' + uuid

    def gen_meta_path(self, uuid=None):
        return safe_join(self.base_path, self.gen_meta_file_name(uuid))

    def gen_incar_path(self, uuid=None):
        uuid = uuid if uuid else self.uuid
        return safe_join(self.incar_base_path, uuid)

    def gen_mnt_path(self, uuid=None):
        return safe_join(self.gen_incar_path(uuid), MNT_DIR)

    def gen_retained_path(self, uuid=None):
        return safe_join(self.gen_incar_path(uuid), RETAINED_DIR)

    def gen_snap_base_path(self, uuid=None):
        return safe_join(self.gen_incar_path(uuid), self.spec.SNAP_BASE_DIR)

    def gen_snap_path(self, snap_name, uuid=None):
        return safe_join(self.gen_snap_base_path(uuid), snap_name)

    def gen_snap_data_path(self, snap_name, uuid=None):
        return safe_join(self.gen_snap_base_path(uuid), snap_name, MNT_DIR)

    def get_incars(self):
        return self.list_dirs(self.incar_base_path)


    # ----- methods for subvol creation, opening and removal -----


    def stat_subvol_mnt_path(self):
        try:
            self.fs.stat(self.gen_mnt_path())
        except ObjectNotFound:
            # when subvol is present but it has no incars
            self.fs.stat(self.base_path)

    def mark_subvolume(self):
        '''
        Set vxattr ceph.dir.subvolume to 1 on the incar/UUID path.
        '''
        # if subvol has not incar ATM
        if not self.path_exists(self.gen_incar_path()):
            return

        subvol_xattr = 'ceph.dir.subvolume'
        try:
            # MDS treats this as a no-op for already marked subvolume
            self.fs.setxattr(self.gen_incar_path(), subvol_xattr, '1', 0)
        except InvalidValue:
            raise VolumeException(EINVAL, f'invalid value for "{subvol_xattr}"')
        except Error as e:
            raise VolumeException(exception=e)

    def _cleanup_on_creation_failure(self, exc):
        assert self.uuid and self.creating

        try:
            self.trash_incar_dir(self.uuid)
            assert len(self.get_incars()) == 0
            self.trash_base_dir()
            #if self.path_exists(self.gen_meta_path()):
            #    self.fs.unlink(self.gen_meta_path())
        except Exception as e:
            log.error('cleanup after failed subvol creation failure also '
                      'failed')
            raise VolumeException(exception=e)

        log.info('cleanup after failed subvol creation passed, raising '
                  'exception that cause subvol creation to fail...')
        if isinstance(exc, VolumeException):
            raise exc
        elif isinstance(exc, MetadataMgrException):
            raise VolumeException(EINVAL,
                                  f'exception in subvolume metadata: {exc}')
        elif isinstance(exc, Error):
            raise VolumeException(exception=exc)
        else:
            assert False, 'unexpected exception'

    def set_meta_symlink(self, old_uuid):
        if old_uuid:
            assert self.path_exists(self.gen_meta_path(old_uuid)), \
                ('meta file for previous incar is missing, it is to be '
                 f'created first. self.meta_path = {self.meta_path}')
        else:
            assert self.path_exists(self.gen_meta_path()), \
                ('meta file for current incar is missing, it is to be '
                 f'created first. self.meta_path = {self.meta_path}')

        assert self.uuid in self.meta_file_name, \
            (f'self.meta_file_name = {self.meta_file_name} '
             f'self.uuid = {self.uuid}')

        assert self.meta_file_name == basename(self.meta_path), \
            (f'self.meta_file_name = {self.meta_file_name} '
             f'self.meta_path = {self.meta_path}')

        if self.path_exists(self.meta_symlink_path, follow_symlink=False):
            self.fs.unlink(self.meta_symlink_path)
        self.fs.symlink(self.meta_file_name, self.meta_symlink_path[1:])

        if old_uuid:
            log.debug(f'subvol {self.name}: meta link updated successfully')
        else:
            log.debug(f'subvol {self.name}: meta link set successfully')

    def _write_meta_file(self, subvol_type):
        assert self.creating

        try:
            initial_state = SubvolumeOpSm.get_init_state(subvol_type)
        except OpSmException as e:
            if subvol_type == SubvolumeTypes.TYPE_NORMAL:
                raise VolumeException(EINVAL,
                                      'subvol creation failed, exception that '
                                      f'was raised: {e}')
            if subvol_type == SubvolumeTypes.TYPE_CLONE:
                raise VolumeException(EINVAL,
                                      f'clone failed, exception raised: {e}')

        if self.creating or len(self.get_incars()) == 1:
            self.init_config(self.VERSION, subvol_type, self.gen_mnt_path(),
                              initial_state)

        else:
            self._set_incarnation_metadata(subvol_type, self.gen_mnt_path(),
                                           initial_state)

    def _create(self, attrs, subvol_type, write_auth_md):
        assert self.creating

        old_uuid = None
        if self.uuid and self.path_exists(self.gen_retained_path()):
            old_uuid = self.uuid
            self.uuid = gen_uuid()
        self._reset_basic_paths()
        self._reset_md_attrs()

        mode = attrs.pop('mode')
        if not self.path_exists(self.group.path):
            self.fs.mkdirs(self.group.path, self.spec.DEFAULT_MODE)
        self.fs.mkdirs(self.gen_mnt_path(), mode)

        self.mark_subvolume()
        log.info(f'mark123 attrs = {attrs}')
        self.set_attrs(self.gen_mnt_path(), attrs)

        self._write_meta_file(subvol_type)
        self.set_meta_symlink(old_uuid)
        if old_uuid:
            self._reset_md_attrs()

        if write_auth_md:
            # Create the subvolume metadata file which manages auth-ids if it
            # doesn't exist
            self.auth_md.create_subvolume_metadata_file(self.group.name,
                                                        self.name)
        log.debug(f'subvol {self.name}: created successfully')

    def create(self, attrs):
        self.creating = True

        try:
            self._create(attrs, SubvolumeTypes.TYPE_NORMAL, write_auth_md=True)
        except (VolumeException, MetadataMgrException, Error) as e:
            self._cleanup_on_creation_failure(e)

    def remove(self, should_retain_snaps=False):
        if self.has_snap():
            if not should_retain_snaps:
                raise VolumeException(ENOTEMPTY,
                                      f'subvolume {self.name} has snapshots')

            self.retain_mnt_dir_content()
        else:
            if not (self.md and
                    self.safe_to_remove_subvolume_clone(self.state)):
                raise VolumeException(EAGAIN,
                                      f'{self.name} clone in-progress, please '
                                      'cancel the clone and retry')

            if len(self.get_incars()) == 1:
                self.trash_base_dir()

            # Delete the volume meta file, if it's not already deleted
            self.auth_md.delete_subvolume_metadata_file(self.group.name,
                                                        self.name)

    def trash_base_dir(self):
        create_trashcan(self.fs, self.spec)

        with open_trashcan(self.fs, self.spec) as trashcan:
            # it's incorrect to call this method if there are more than one
            # incars.
            assert len(self.get_incars()) in (0, 1)
            assert self.has_snap() is False
            trashcan.dump(self.base_path)

    def trash_incar_dir(self, uuid):
        create_trashcan(self.fs, self.spec)
        with open_trashcan(self.fs, self.spec) as trashcan:
            trashcan.dump(self.gen_incar_path(uuid))


    # ----- helper methods for snap code -----


    def get_v3_snap_names(self):
        snap_names = []
        for uuid in self.get_incars():
            path = self.gen_snap_base_path(uuid)
            snap_names += self.list_snaps(path)
        return snap_names

    def get_v3_incar_of_snap(self, snap_name):
        for uuid in self.get_incars():
            path = self.gen_snap_base_path(uuid)
            if snap_name in self.list_snaps(path):
                return uuid

    # Listing all snaps to find one can be unncessary and also expensive due to
    # possibility of multiple snaps in multiple incarnations. Therefore, use
    # this method where, instead, listing is stoppped as soon as given snap
    # name is found instead.
    def has_v3_snap(self, snap_name=None):
        if snap_name:
            uuid = self.get_v3_incar_of_snap(snap_name)
            path = self.gen_snap_path(snap_name, uuid)
            return self.path_exists(path)

        for uuid in self.get_incars():
            path = self.gen_snap_base_path(uuid)
            if not self.is_dir_empty(path):
                return True
        else:
            return False


    # ----- methods for snaps -----


    def create_snap(self, snap_name):
        if self.has_snap(snap_name):
            return

        grp_snap_path = safe_join(self.group.path, self.spec.SNAP_BASE_DIR,
                                  snap_name)
        if self.path_exists(grp_snap_path):
            raise VolumeException(EINVAL,
                                  f'subvolumegroup snapshot "{snap_name}" '
                                  'already exists, can not create subvolume '
                                  'snapshot with same name')

        self.mksnap(self.gen_snap_path(snap_name))

    def _rm_base_dir_after_snap_removal(self, snap_path):
        should_purge = False

        if len(self.get_incars()) == 0:
            self.trash_base_dir()
            should_purge = True

        return should_purge

    def _rm_incar_dir_after_snap_removal(self, snap_path):
        should_purge = False

        uuid = basename(dirname(dirname(snap_path)))
        verify_uuid(uuid)
        if self.is_incar_retained(uuid) and not self.has_v3_snap(uuid):
            self.trash_uuid_dir(uuid)
            should_purge = True

        return should_purge

    def _rm_snap(self, snap_name, snap_path, force):
        try:
            self.md.remove_section(self.get_snap_section_name(snap_name))
            self.md.flush()

            self.rmsnap(snap_path)
        except MetadataMgrException as e:
            if force:
                log.info('Due to "--force", allowing snapshot removal despite '
                         f'metadata removal failure snap={snap_name} subvol='
                         f'{self.name} group={self.group.name} Exception: {e}')
            else:
                msg = (f'Failed to remove snapshot metadata. snap={snap_name} '
                       f'subvol={self.subvol_name} group={self.group_name} '
                       f'Exception: {e}')
                log.error(msg)
                raise VolumeException(EAGAIN, msg)

    def check_before_removing_snap(self, snap_name):
        if self.has_pending_clones(snap_name):
            raise VolumeException(EAGAIN,
                                  f'snap "{snap_name}" has pending clones')

        snap_path = self.get_snap_path(snap_name)
        if not snap_path:
            raise VolumeException(ENOENT, f'snap "{snap_name}" does not exist')

        return snap_path

    def rm_snap(self, snap_name, force):
        snap_path = self.check_before_removing_snap(snap_name)

        self._rm_snap(snap_name, snap_path, force)

        should_purge = self._rm_incar_dir_after_snap_removal(snap_path)
        if should_purge:
            should_purge = self._rm_base_dir_after_snap_removal(snap_path)

        return should_purge

    def has_snap(self, snap_name=None, uuid=None):
        return self.has_v3_snap(snap_name, uuid)

    def get_snap_names(self):
        return self.get_v3_snap_names()

    def get_snap_path(self, snap_name):
        '''
        Return path of an existing snapshot. Raise exception if the snapshot
        does not exist.
        '''
        if uuid := self.get_v3_incar_of_snap(snap_name):
            return self.gen_snap_path(snap_name, uuid)

    def get_snap_data_path(self, snap_name):
        '''
        Return path where the actual data of the snapshot resides. Raise
        exception if the snapshot does not exist.
        '''
        if uuid := self.get_v3_incar_of_snap(snap_name):
            return self.gen_snap_data_path(snap_name, uuid)

    def get_pending_clones_info(self, snap_name):
        try:
            if not self.has_pending_clones(snap_name):
                return {'has_pending_clones': 'no'}
            else:
                pending_track_id_list = self.md.filter_keys('clone snaps',
                                                            snap_name)
        except MetadataMgrException as e:
            if e.errno != -ENOENT:
                raise VolumeException(exception=e)
            else:
                pending_track_id_list = []

        try:
            with open_clone_index(self.fs, self.vol_spec) as index:
                index_path = to_str(index.path)
        except IndexException as e:
            log.warning(f'failed to open clone index "{e}" for snapshot '
                        f'"{snap_name}"')
            raise VolumeException(EINVAL, 'failed to open clone index')

        pending_clone_list = []
        orphan_clones_count = 0
        for track_id in pending_track_id_list:
            try:
                track_id_path = safe_join(index_path, track_id)
                snap_data_path = to_str(self.fs.readlink(track_id_path, 4096))
                self.remove_non_existent_pending_clones(track_id_path,
                                                        snap_data_path,
                                                        track_id)
            except Error as e:
                if abs(e.errno) == ENOENT:
                    orphan_clones_count += 1
                    continue
                else:
                    raise VolumeException(exception=e)

            subvol_base_path = dirname(dirname(dirname(snap_data_path)))
            clone_subvol_name = basename(subvol_base_path)
            group_name = basename(dirname(subvol_base_path))

            details = {'name': clone_subvol_name}
            if group_name != self.group.NO_GROUP_NAME:
                details['target_group'] = group_name
            pending_clone_list.append(details)

        pending_clones_info = {}
        if len(pending_clone_list) > 0:
            pending_clones_info['has_pending_clones'] = 'yes'
            pending_clones_info['pending_clones'] = \
                    pending_clone_list # type: ignore
        else:
            pending_clones_info['has_pending_clones'] = 'no'

        if orphan_clones_count > 0:
            pending_clones_info['orphan_clones_count'] = \
                orphan_clones_count # type: ignore

        return pending_clones_info

    def get_snap_info(self, snap_name):
        if not self.has_snap(snap_name):
            raise VolumeException(ENOENT,
                                  f'snapshot "{snap_name}" does not exist')
        if is_inherited_snap(snap_name):
            raise VolumeException(EINVAL,
                                  f'snapshot name "{snap_name}" is invalid')

        snap_path = self.get_snap_data_path(snap_name)
        try:
            created_at = self.fs.getxattr(snap_path, 'ceph.snap.btime')
            created_at = datetime.fromtimestamp(float(created_at))
            created_at = str(created_at)
            data_pool = self.fs.getxattr(snap_path, 'ceph.dir.layout.pool')
            data_pool = to_str(data_pool)

            info_dict = {'created_at': created_at, 'data_pool': data_pool}
            info_dict.update(self.get_pending_clones_info(snap_name))

            return info_dict
        except Exception as e:
            raise VolumeException(exception=e)


    # ----- methods for subvol removal while retaining snaps -----


    def allowed_ops_by_state(self, vol_state):
        if vol_state == SubvolumeStates.STATE_COMPLETE:
            return {op_type for op_type in SubvolumeOpType}
        elif vol_state == SubvolumeStates.STATE_RETAINED:
            return {
                    SubvolumeOpType.CREATE,
                    SubvolumeOpType.REMOVE,
                    SubvolumeOpType.REMOVE_FORCE,
                    SubvolumeOpType.LIST,
                    SubvolumeOpType.INFO,
                    SubvolumeOpType.SNAP_CREATE,
                    SubvolumeOpType.SNAP_REMOVE,
                    SubvolumeOpType.SNAP_REMOVE_FORCE,
                    SubvolumeOpType.SNAP_LIST,
                    SubvolumeOpType.SNAP_GETPATH,
                    SubvolumeOpType.SNAP_INFO,
                    SubvolumeOpType.SNAP_PROTECT,
                    SubvolumeOpType.SNAP_UNPROTECT,
                    SubvolumeOpType.CLONE_SOURCE,
                    SubvolumeOpType.CLONE_FAILED
            }
        else:
            return {
                    SubvolumeOpType.REMOVE_FORCE,
                    SubvolumeOpType.LIST,
                    SubvolumeOpType.CLONE_CREATE,
                    SubvolumeOpType.CLONE_STATUS,
                    SubvolumeOpType.CLONE_CANCEL,
                    SubvolumeOpType.CLONE_INTERNAL,
                    SubvolumeOpType.CLONE_SOURCE,
                    SubvolumeOpType.CLONE_FAILED
            }

    def is_incar_retained(self, uuid):
        if self.creating:
            return False

        if self.path_exists(self.gen_retained_path(uuid)):
            return True
        elif self.path_exists(self.gen_mnt_path(uuid)):
            return False
        else:
            assert False, \
                ('inar dir contains neither "mnt" nor "retained" dir, this '
                 'should be impossible')

    def update_meta_file_after_retain(self):
        self.md.remove_section(self.md.USER_METADATA_SECTION)

        self.md.update_global_section('path', self.gen_retained_path())
        self.md.update_global_section('state',
                                      SubvolumeStates.STATE_RETAINED.value)

        self.md.flush()

    def retain_mnt_dir_content(self):
        assert self.state != SubvolumeStates.STATE_RETAINED

        try:
            self.update_meta_file_after_retain()
            self.fs.rename(self.gen_mnt_path(), self.gen_retained_path())
        except MetadataMgrException as e:
            log.error(f"failed to write config: {e}")
            raise VolumeException(exception=e)


    # ==================================================================
    # Methods that makes code of this class compatible with SubvolumeV2,
    # SubvolumeV1 and SubvolumeBase.
    # ==================================================================


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

    @property
    def retained(self):
        assert False, \
            ('v2 can be retained but in v3 incars can be retained, so this '
             'method doesn\'t work as it is')

    @property
    def has_pending_purges(self):
        # since v3 does not have in-subvol ".trash" dir, this method must always
        # return False.
        return False

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

    def snapshot_base_path(self):
        return self.gen_snap_base_path()

    def snapshot_path(self, snap_name):
        if snap_path := self.get_snap_path(snap_name):
            return snap_path

        raise VolumeException(ENOENT, f'snap "{snap_name}" does not exist')

    def snapshot_data_path(self, snap_name):
        if snap_data_path := self.get_snap_data_path(snap_name):
            return snap_data_path

        raise VolumeException(ENOENT, f'snap "{snap_name}" does not exist')

    def list_snapshots(self):
        '''
        :return: list of snap names
        :rtype: list of str
        '''
        return self.get_snap_names()

    def create_snapshot(self, snap_name):
        return self.create_snap(snap_name)

    def remove_snapshot(self, snap_name, force=False):
        return self.rm_snap(snap_name, force)

    def snapshot_info(self, snap_name):
        return self.get_snap_info(snap_name)
