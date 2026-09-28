from errno import EINVAL, EAGAIN, ENOTEMPTY, ENOENT
from os.path import basename, dirname
from logging import getLogger
from typing import Final
from datetime import datetime
from time import sleep

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
from ...fs_util import (listdir, path_exists, listsnaps, is_inherited_snap,
                        statx, is_dir_empty, get_all_xattrs, set_all_xattrs)
from ...exception import (VolumeException, MetadataMgrException, OpSmException,
                          IndexException, V2UpgradeError)


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

    def statx(self, path, fields=None):
        return statx(self.fs, path, fields)

    def is_dir_empty(self, path):
        return is_dir_empty(self.fs, path)

    def get_all_xattrs(self, path):
        return get_all_xattrs(self.fs, path)

    def set_all_xattrs(self, path, xattrs):
        return set_all_xattrs(self.fs, path, xattrs)


class V2Incar(SubvolHelper):
    '''
    For helping SubvolV3 with its v2 incarnation.
    '''

    def __init__(self, fs, spec, base_path, md, creating_v3):
        self.fs = fs
        self.spec = spec
        self.base_path: Final = base_path
        self.md = md
        self.reset_attrs()

        if creating_v3:
            self.empty = True
            return

        self.v3_incar_base_path = safe_join(self.base_path, INCAR_BASE_DIR)
        if uuid := self.fetch_uuid():
            self.empty = False
            self._uuid = uuid
        else:
            self.empty = True

    def reset_attrs(self):
        self._uuid = None
        self._meta_path = None
        self._uuid_path = None
        self._snap_base_path = None

    def mark_empty(self):
        assert not self.empty
        self.empty = True

        self.fs = None
        self.spec = None
        self._meta_path = None
        self._uuid_path = None

    def mark_upgraded(self):
        assert self.upgrading
        self.upgrading = False
        self._meta_path = None

    def is_upgraded(self):
        if self.upgrading:
            return False
        else:
            return self.path_exists(self.v3_incar_base_path)

    def fetch_uuid(self):
        if self.is_upgraded():
            self.md.refresh()
            return self.md.get_pre_v3_uuid(def_val=None)

        subdirs = self.list_dirs(self.base_path)
        if INCAR_BASE_DIR in subdirs:
            subdirs.remove(INCAR_BASE_DIR)
        if subdirs:
            verify_uuid(subdirs[0])
            return subdirs[0]

    @property
    def uuid(self):
        if not self._uuid:
            self._uuid = self.fetch_uuid()
        return self._uuid

    @property
    def uuid_path(self):
        if not self._uuid_path:
            self._uuid_path = safe_join(self.base_path, self.uuid)
        return self._uuid_path

    @property
    def meta_path(self):
        if not self.uuid:
            return

        if not self._meta_path:
            self._meta_path = safe_join(self.base_path, '.meta')
        return self._meta_path

    def get_snap_base_path(self):
        if not self.uuid:
            return

        if not self._snap_base_path:
            self._snap_base_path = safe_join(self.base_path,
                                             self.spec.SNAP_BASE_DIR)
        return self._snap_base_path

    def get_snap_names(self):
        if not self.uuid:
            return []

        return self.list_snaps(self.get_snap_base_path())

    def get_snap_path(self, snap_name):
        if not self.uuid:
            return None

        if snap_name in self.get_snap_names():
            return safe_join(self.get_snap_base_path(), snap_name)

    def get_snap_data_path(self, snap_name):
        if self.empty:
            return

        if snap_name in self.get_snap_names():
            return safe_join(self.get_snap_base_path(), snap_name, self.uuid)

    def has_snap(self, snap_name=None, uuid=None):
        if not self.uuid:
            return False

        if uuid and uuid != self.uuid:
            return False

        if snap_name:
            return snap_name in self.get_snap_names()
        else:
            if not self.path_exists(self.get_snap_base_path()):
                return False
            return not self.is_dir_empty(self.get_snap_base_path())

    def is_snap_path(self, path):
        if self.empty:
            return

        subvol_name = basename(self.base_path)
        return (subvol_name == basename(dirname(dirname(path))))

    def is_snap_data_path(self, path):
        if self.empty:
            return

        subvol_name = basename(self.base_path)
        return (subvol_name == basename(dirname(dirname(dirname(path)))))


class SubvolumeV3(SubvolHelper, SubvolumeV2):
    '''
    Code for v3 subvolumes.
    '''

    VERSION = 3


    # ----- init and related helper methods -----


    def __init__(self, mgr, fs, spec, group, name, uuid=None,
                 disc_version=None):
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

        # TODO: check if we can do without self.creating without too much
        # hassle?
        self.creating = False

        if disc_version is None:
            self.creating = True
        elif disc_version == self.VERSION:
            verify_uuid(self.uuid)
        elif disc_version < self.VERSION:
            self._auto_upgrade(disc_version)
        else:
            msg = f'disc_version = {disc_version}'
            log.error(msg)
            assert False, msg

    def _reset_basic_attrs(self):
        self._reset_basic_paths()
        self._reset_md_attrs()
        self._reset_v2_helper()

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


    # ----- v3 helpers -----


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


    # ----- v2 helpers -----


    @property
    def v2(self):
        if not self._v2: # type: ignore
            self._v2 = V2Incar(self.fs, self.spec, # type: ignore
                               self.base_path, self.md, self.creating)
        return self._v2

    def _reset_v2_helper(self):
        self._v2 = None # type: ignore


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

    def _create_dir_layout(self, attrs):
        assert self.creating

        mode = attrs.pop('mode')
        if not self.path_exists(self.group.path):
            self.fs.mkdirs(self.group.path, self.spec.DEFAULT_MODE)
        self.fs.mkdirs(self.gen_mnt_path(), mode)

        self.mark_subvolume()
        self.set_attrs(self.gen_mnt_path(), attrs)

    def _create(self, attrs, subvol_type, write_auth_md):
        assert self.creating

        old_uuid = None
        if self.uuid and self.path_exists(self.gen_retained_path()):
            old_uuid = self.uuid
            self.uuid = gen_uuid()
        self._reset_basic_paths()
        self._reset_md_attrs()

        self._create_dir_layout(attrs)

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

    def is_v3_snap_path(self, path):
        for i in range(3):
            path = dirname(path)
        return (INCAR_BASE_DIR == basename(path))

    def is_v3_snap_data_path(self, path):
        for i in range(4):
            path = dirname(path)
        return (INCAR_BASE_DIR == basename(path))


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

    def _rm_base_dir_after_snap_removal(self):
        should_purge = False

        if len(self.get_incars()) == 0:
            self.trash_base_dir()
            should_purge = True

        return should_purge

    def _rm_incar_dir_after_snap_removal(self, snap_path):
        should_purge = False

        if self.v2.is_snap_path(snap_path):
            if self.v2.has_snap():
                return False

            self.md.rm_pre_v3_incar_sec()
            self.md.flush()

            self.v2.mark_empty()
            should_purge = True
        elif self.is_v3_snap_path(snap_path):
            uuid = basename(dirname(dirname(snap_path)))
            verify_uuid(uuid)
            if self.is_incar_retained(uuid):
                self.trash_incar_dir(uuid)
                should_purge = True
        else:
            msg = f'snap_path = {snap_path}'
            log.error(msg)
            assert False, msg

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
            should_purge = self._rm_base_dir_after_snap_removal()

        return should_purge

    def has_snap(self, snap_name=None):
        if self.v2.has_snap(snap_name):
            return True
        elif self.v3_has_snap(snap_name):
            return True
        else:
            return False

    def get_snap_names(self):
        all_snap_names = []
        all_snap_names += self.v2.get_snap_names()
        all_snap_names += self.get_v3_snap_names()
        return all_snap_names

    def get_snap_path(self, snap_name):
        '''
        Return path of an existing snapshot. Raise exception if the snapshot
        does not exist.
        '''
        if snap_path := self.v2.get_snap_path(snap_name):
            pass
        elif uuid := self.get_v3_incar_of_snap(snap_name):
            snap_path = self.gen_snap_path(snap_name, uuid)
        else:
            return None

        return snap_path

    def get_snap_data_path(self, snap_name):
        '''
        Return path where the actual data of the snapshot resides. Raise
        exception if the snapshot does not exist.
        '''
        if snap_data_path := self.v2.get_snap_data_path(snap_name):
            pass
        elif uuid := self.get_v3_incar_of_snap(snap_name):
            snap_data_path = self.gen_snap_data_path(snap_name, uuid)
        else:
            return None

        return snap_data_path

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


    # ----- methods for subvol snap clone op -----


    def add_clone_src_to_meta_file(self, volname, subvolume, snap_name):
        self.md.add_section("source")
        self.md.update_section("source", "volume", volname)
        if not subvolume.group.is_default_group():
            self.md.update_section("source", "group", subvolume.group_name)
        self.md.update_section("source", "subvolume", subvolume.subvol_name)
        self.md.update_section("source", "incar", self.uuid)
        self.md.update_section("source", "snapshot", snap_name)
        self.md.flush()

    def add_clone_failure_to_meta_file(self, errno, errmsg):
        try:
            self.md.add_section(self.md.CLONE_FAILURE_SECTION)
            self.md.update_section(self.md.CLONE_FAILURE_SECTION,
                                   self.md.CLONE_FAILURE_META_KEY_ERRNO, errno)
            self.md.update_section(self.md.CLONE_FAILURE_SECTION,
                                   self.md.CLONE_FAILURE_META_KEY_ERROR_MSG,
                                   errmsg)
            self.md.flush()
        except MetadataMgrException as e:
            raise VolumeException(exception=e)

    def create_as_clone(self, pool, src_volname, src_subvol, snap_name):
        try:
            # source snapshot attrs are used to create clone subvolume
            # attributes of subvolume's content though, are synced during the
            # cloning process.
            attrs = src_subvol.get_attrs(
                        src_subvol.get_snap_data_path(snap_name))

            # The source of the clone may have exceeded its quota limit as
            # CephFS quotas are imprecise. Cloning such a source may fail if
            # the quota on the destination is set before starting the clone
            # copy. So always set the quota on destination after cloning is
            # successful.
            attrs["quota"] = None

            # override snapshot pool setting, if one is provided for the clone
            if pool is not None:
                attrs["data_pool"] = pool
                attrs["pool_namespace"] = None

            self._create(attrs, SubvolumeTypes.TYPE_CLONE, write_auth_md=False)
            self.add_clone_src_to_meta_file(src_volname, src_subvol, snap_name)
        except (VolumeException, MetadataMgrException, Error) as e:
            self._cleanup_on_creation_failure(e)
        except Exception as e:
            raise VolumeException(exception=e)


    # ----- methods for subvol auto-upgrade -----


    def _auto_upgrade(self, disc_version):
        if disc_version == 2:
            self._upgrade_v2_subvol()
        else:
            assert False, \
                f'disc_version = {disc_version}'

    def set_prev_path(self, prev_subvol_mnt_path):
        xattr = 'ceph.dir.subvolume.prevpath'

        prev_path = to_str(prev_subvol_mnt_path)
        # caps will have '/' prefixing path, so ensure prev_path also has it.
        if prev_path[0] != '/':
            prev_path = '/' + prev_subvol_mnt_path

        # XXX: needs to be done as **early** as possible to keep the possibility
        # of client IO interruption minimum, keeping subvol upgrade seamless.
        #
        # XXX: applying prevpath vxattr on v1/v2 UUID path as it would be
        # renamed to mnt path. setting it after renaming v1/v2 UUID path to mnt
        # path create a minute period between them during which MDS will reject
        # client requests, causing client IO interruption.
        for i in range(100):
            try:
                self.fs.setxattr(prev_subvol_mnt_path, xattr,
                                 prev_subvol_mnt_path)
            except ObjectNotFound:
                sleep(60)
            else:
                return
        else:
            raise V2UpgradeError(f'attempted setting {xattr} every '
                                 'minute for last 2 hours but got '
                                 'ObjectNotFound exception everytime, MDS did '
                                 'not upgrade all this time')


    # ----- methods for subvol auto-upgrade from v2 -----


    def _update_v2_meta_file(self):
        '''
        Update subvolume meta file from v2 to v3.
        '''
        log.info(f'subvol {self.name}: updating meta file as part of v2 to v3'
                 'auto-upgrade...')

        try:
            self.md.refresh()

            self.md.update_global_section('version', self.VERSION)
            self.md.update_global_section('path', self.gen_mnt_path())
            if self.v2.has_snap():
                self.md.add_pre_v3_incar_sec('v2', self.v2.uuid,
                                             self.v2.uuid_path)

            self.md.flush()
        except MetadataMgrException as e:
            raise V2UpgradeError(e)

        log.info(f'subvol {self.name}: meta file update as part of v2 to v3 '
                 'upgrade was successful')

    def _upgrade_v2_layout(self):
        '''
        Upgrade subvolume layout from v2 to v3.
        '''
        log.info(f'subvol {self.name}: upgrading layout from v2 to v3...')

        try:
            uid, gid, mode = self.statx(self.v2.uuid_path, ('uid', 'gid',
                                                            'mode'))
            sv_xattrs = self.get_all_xattrs(self.v2.uuid_path)
            self.fs.mkdirs(self.gen_incar_path(), 0o755)
            self.fs.rename(self.v2.uuid_path, self.gen_mnt_path())
            self.fs.chmod(self.gen_mnt_path(), mode)
            self.fs.chown(self.gen_mnt_path(), uid, gid)
            if sv_xattrs:
                self.set_all_xattrs(self.gen_mnt_path(), sv_xattrs)

            self.fs.rename(self.v2.meta_path, self.meta_path)
            self.fs.symlink(self.meta_file_name, self.meta_symlink_path[1:])
            for path in (self.meta_path, self.meta_symlink_path):
                self.fs.chown(path, 0, 0)
                self.fs.chmod(path, 0o644)
        except Exception as e:
            raise V2UpgradeError(e)

        log.info(f'subvol {self.name}: layout upgrade from v2 to v3 was '
                 'successful')

    def _upgrade_v2_subvol(self):
        '''
        Upgrade v2 subvolume to v3 subvolume.
        '''
        log.info(f'subvol {self.name}: initiating v2 to v3 auto-upgrade')

        assert self.v2.uuid, \
            f'self.v2.uuid = {self.v2.uuid}'
        assert self.v2.uuid == self.uuid, \
            f'self.v2.uuid = {self.v2.uuid} self.uuid = {self.uuid}'

        self.set_prev_path(self.v2.uuid_path)
        self._reset_basic_paths()

        self.v2.upgrading = True
        self._upgrade_v2_layout()
        self._reset_md_attrs()
        self._update_v2_meta_file()

        self.v2.mark_upgraded()
        assert self.uuid == self.v2.uuid
        self.clean_stale_snapshot_metadata()

        log.info(f'subvol {self.name}: auto-upgrade from v2 to v3 was '
                 'successful')


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

    def create_clone(self, pool, src_volname, src_subvol, snap_name):
        self.create_as_clone(pool, src_volname, src_subvol, snap_name)
