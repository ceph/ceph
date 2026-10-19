from errno import EINVAL, EAGAIN, ENOTEMPTY, ENOENT, EEXIST
from os import strerror
from os.path import basename, dirname
from logging import getLogger
from typing import Final
from time import sleep

from cephfs import Error, InvalidValue, ObjectNotFound

from .subvolume_v2 import SubvolumeV2
from .subvolume_attrs import SubvolumeStates, SubvolumeTypes
from .metadata_manager import MetadataManager
from .auth_metadata import AuthMetadataManager
from .op_sm import SubvolumeOpSm
from ..template import SubvolumeOpType
from ..trash import create_trashcan, open_trashcan
from ...utils import gen_uuid, verify_uuid, safe_join, to_utf8b, to_str
from ...fs_util import (listdir, path_exists, listsnaps, get_all_xattrs, statx,
                        set_all_xattrs, is_dir_empty)
from ...exception import (VolumeException, MetadataMgrException, OpSmException,
                          V2UpgradeError, V1UpgradeError)


log = getLogger(__name__)


META_FILE_NAME: Final = to_utf8b('.meta')
# incar = incarnation
INCAR_BASE_DIR: Final = to_utf8b('roots')
RETAINED_DIR: Final = to_utf8b('.retained')
MNT_DIR: Final = to_utf8b('mnt')


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

    # simply returns snap path, regardless of whether it exists.
    #
    # unfortunately, v1/v2 has this strange naming which doesn't tell enough.
    # adhering with it for now to avoid unrelated group of changes to codebase
    # in this patch series.
    def snapshot_path(self, snap_name):
        return self.gen_snap_path(snap_name)

    # returns snap path if the snap exists.
    #
    # unfortunately, v1/v2 has this strange naming which doesn't tell enough.
    # adhering with it for now to avoid unrelated group of changes to codebase
    # in this patch series.
    def snapshot_data_path(self, snap_name):
        if snap_path := self.get_snap_path(snap_name):
            return snap_path

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
        raise VolumeException(ENOENT, f'snap "{snap_name}" does not exist')

    def list_snapshots(self):
        '''
        :return: list of snap names
        :rtype: list of str
        '''
        return self.get_snap_names()

    def create_snapshot(self, snap_name):
        return self.create_snap(snap_name)

    def remove_snapshot(self, snap_name, force):
        return self.rm_snap(snap_name, force)


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

    def statx(self, path, fields=None):
        return statx(self.fs, path, fields)

    def is_dir_empty(self, path):
        return is_dir_empty(self.fs, path)

    def get_all_xattrs(self, path):
        return get_all_xattrs(self.fs, path)

    def set_all_xattrs(self, path, xattrs):
        return set_all_xattrs(self.fs, path, xattrs)


class V1V2Base(SubvolHelper, PreV3Helper):
    '''
    Base class for methods of V1Incar and V2Incar
    '''

    def __init__(self, fs, spec, base_path):
        super().__init__(fs, spec)

        self.fs = fs
        self.spec = spec
        self._base_path = base_path

        self.empty = False
        self.upgrading = False
        self.v3_incar_base_path = safe_join(self.base_path, INCAR_BASE_DIR)

        self._uuid = None if self.is_upgraded() else self.fetch_uuid()
        self._meta_path = None
        self._uuid_path = None
        self._snap_base_path = None

    def mark_empty(self):
        assert not self.empty
        self.empty = True

        self.fs = None
        self.spec = None
        self._base_path = None
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

    @property
    def base_path(self):
        if self.empty:
             return

        return self._base_path

    def fetch_uuid(self):
        assert not self.is_upgraded(), \
            ('v1/v2 subvolume was upgraded to v3, it doesn\'t have it\'s own '
             'UUID anymore')

        subdirs = self.list_dirs(self.base_path)
        if INCAR_BASE_DIR in subdirs:
            subdirs.remove(INCAR_BASE_DIR)

        if subdirs:
            verify_uuid(subdirs[0])
            return subdirs[0]

    @property
    def uuid(self):
        raise NotImplementedError('to be overridden by derived classes')

    @property
    def meta_path(self):
        assert not self.is_upgraded(), \
            ('v1/v2 subvolume has been upgraded to v3, v1/v2 meta path is '
             'now invalid and non-existent')

        if not self._meta_path:
            self._meta_path = safe_join(self.base_path, META_FILE_NAME)
        return self._meta_path

    @property
    def uuid_path(self):
        if not self._uuid_path:
            self._uuid_path = safe_join(self.base_path, self.uuid)
        return self._uuid_path

    @property
    def snap_base_path(self):
        raise NotImplementedError('to be overridden by derived classes')

    def get_snap_names(self):
        if self.empty:
            return []

        return self.list_snaps(self.snap_base_path)

    def get_snap_path(self, snap_name):
        if self.empty:
            return

        if snap_name in self.get_snap_names():
            return safe_join(self.snap_base_path, snap_name)

    def has_snap(self, snap_name=None):
        if self.empty:
            return False

        if snap_name:
            return snap_name in self.get_snap_names()
        else:
            if not self.path_exists(self.snap_base_path):
                return False
            return not self.is_dir_empty(self.snap_base_path)


class V1Incar(V1V2Base):
    '''
    For helping SubvolV3 with its v1 incarnation and v1 subvol layout during
    subvol auto-upgrade.
    '''

    def __init__(self, fs, spec, base_path, md):
        self.md = md
        super().__init__(fs, spec, base_path)
        self.empty = (not self.uuid)

    def mark_upgraded(self):
        super().mark_upgraded()

        self._snap_base_path = None

    def fetch_uuid(self):
        if self.is_upgraded():
            self.md.refresh()
            return self.md.get_global_option('v1_incar', None)
        else:
            return super().fetch_uuid()

    @property
    def uuid(self):
        if self.empty:
            return

        if not self._uuid:
            self._uuid = self.fetch_uuid()
        return self._uuid

    def fetch_snap_base_path(self):
        if self.empty:
            return None

        if self.is_upgraded():
            return safe_join(self.base_path, INCAR_BASE_DIR, self.uuid, MNT_DIR,
                             self.spec.SNAP_BASE_DIR)
        else:
            return safe_join(self.uuid_path, self.spec.SNAP_BASE_DIR)

    @property
    def snap_base_path(self):
        if self.empty:
            return

        if not self._snap_base_path:
            self._snap_base_path = self.fetch_snap_base_path()
        return self._snap_base_path

    def is_snap_path(self, path):
        if self.empty:
            return

        return (INCAR_BASE_DIR == basename(dirname(dirname(dirname(dirname(path))))))


class V2Incar(V1V2Base):
    '''
    For helping SubvolV3 with its v2 incarnation and v2 subvol layout during
    subvol auto-upgrade.
     '''

    def __init__(self, fs, spec, base_path):
        super().__init__(fs, spec, base_path)

        self.empty = (self.is_upgraded() and not self.has_snap())

    def mark_upgraded(self):
        super().mark_upgraded()

        self._uuid = None

    @property
    def uuid(self):
        assert not self.is_upgraded(), \
            ('v1/v2 subvolume has been upgraded to v3, v1/v2 uuid is now '
             'invalid and non-existent')

        if self.empty:
            return

        if not self._uuid:
            self._uuid = self.fetch_uuid()
        return self._uuid

    @property
    def snap_base_path(self):
        if self.empty:
            return

        if not self._snap_base_path:
            self._snap_base_path = safe_join(self.base_path,
                                             self.spec.SNAP_BASE_DIR)
        return self._snap_base_path

    def is_snap_path(self, path):
        if self.empty:
            return

        subvol_name = basename(self.base_path)
        return (subvol_name == basename(dirname(dirname(path))))


class SubvolumeV3(SubvolHelper, PreV3Helper, SubvolumeV2):
    '''
    Code for v3 subvolumes.
    '''

    VERSION: Final = 3


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
        self._reset_v1_helper()

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

    def get_v3_incars(self):
        return self.list_dirs(self.incar_base_path)

    def is_v3_snap_path(self, path):
        return (INCAR_BASE_DIR == basename(dirname(dirname(dirname(path)))))


    # ----- v2 and v1 helpers -----


    @property
    def v2(self):
        if not self._v2:
            self._v2 = V2Incar(self.fs, self.spec, self.base_path)
        return self._v2

    def _reset_v2_helper(self):
        self._v2 = None

    @property
    def v1(self):
        if not self._v1:
            self._v1 = V1Incar(self.fs, self.spec, self.base_path, self.md)
        return self._v1

    def _reset_v1_helper(self):
        self._v1 = None


    # ----- methods for subvol creation, opening and removal -----


    @property
    def has_pending_purges(self):
        # since v3 does not have in-subvol ".trash" dir, this method must always
        # return False.
        return False

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
            self.fs.unlink(self.gen_meta_path())
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
                                  'exception in subvolume metadata: '
                                  f'{strerror(-abs(exc.args[0]))}')
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
                                      'subvol creation failed, OpSmException '
                                      f'was raised: {e}')
            if subvol_type == SubvolumeTypes.TYPE_CLONE:
                raise VolumeException(EINVAL,
                                      'clone failed, OpSmException was raised: '
                                      f'{e}')

        if self.creating or len(self.get_v3_incars()) == 1:
            self.init_config(self.VERSION, subvol_type,
                             self.gen_mnt_path().decode('utf-8'), initial_state)
        else:
            self._set_incarnation_metadata(subvol_type,
                                           self.gen_mnt_path().decode('utf-8'),
                                           initial_state)

        log.debug(f'subvol {self.name}: meta file written successfully')

    def _create(self, mode, attrs, subvol_type, auth=True):
        assert self.creating

        old_uuid = None
        if self.uuid and self.path_exists(self.gen_retained_path()):
            old_uuid = self.uuid
            self.uuid = gen_uuid()
        self._reset_basic_paths()
        self._reset_md_attrs()

        if not self.path_exists(self.group.path):
            self.fs.mkdirs(self.group.path, self.spec.DEFAULT_MODE)
        self.fs.mkdirs(self.gen_mnt_path(), mode)
        log.debug(f'subvol {self.name}: layout created successfully')

        self.mark_subvolume()
        self.set_attrs(self.gen_mnt_path(), attrs)
        log.debug(f'subvol {self.name}: xattrs applied successfully')

        self._write_meta_file(subvol_type)
        self.set_meta_symlink(old_uuid)
        if old_uuid:
            self._reset_md_attrs()

        if auth:
            # Create the subvolume metadata file which manages auth-ids if it
            # doesn't exist
            self.auth_md.create_subvolume_metadata_file(self.group.name,
                                                        self.name)
            log.debug(f'subvol {self.name}: auth md file created successfully')
        log.debug(f'subvol {self.name}: created successfully')

    def create(self, mode, attrs):
        self.creating = True

        try:
            self._create(mode, attrs, SubvolumeTypes.TYPE_NORMAL)
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

            if len(self.get_v3_incars()) == 1:
                self.trash_base_dir()

            # Delete the volume meta file, if it's not already deleted
            self.auth_md.delete_subvolume_metadata_file(self.group.name, self.name)

    def trash_base_dir(self):
        create_trashcan(self.fs, self.spec)

        with open_trashcan(self.fs, self.spec) as trashcan:
            # it's incorrect to call this method if there are more than one
            # incars.
            assert len(self.get_v3_incars()) in (0, 1)
            assert self.has_snap() is False
            trashcan.dump(self.base_path)

    def trash_incar_dir(self, uuid):
        create_trashcan(self.fs, self.spec)
        with open_trashcan(self.fs, self.spec) as trashcan:
            trashcan.dump(self.gen_incar_path(uuid))


    # ----- helper methods for snap code -----


    def get_v3_snap_names(self):
        snap_names = []
        for uuid in self.get_v3_incars():
            path = self.gen_snap_base_path(uuid)
            snap_names += self.list_snaps(path)
        return snap_names

    def get_v3_incar_of_snap(self, snap_name):
        for uuid in self.get_v3_incars():
            path = self.gen_snap_base_path(uuid)
            if snap_name in self.list_snaps(path):
                return uuid

    # Listing all snaps to find one can be unncessary and also expensive due to
    # possibility of multiple snaps in multiple incarnations. Therefore, use
    # this method where, instead, listing is stoppped as soon as given snap name
    # is found instead.
    def has_v3_snap(self, snap_name=None):
        if snap_name:
            uuid = self.get_v3_incar_of_snap(snap_name)
            path = self.gen_snap_path(snap_name, uuid)
            return self.path_exists(path)

        for uuid in self.get_v3_incars():
            path = self.gen_snap_base_path(uuid)
            if not self.is_dir_empty(path):
                return True
        else:
            return False


    # ----- methods for snaps -----


    def create_snap(self, snap_name):
        if self.has_snap(snap_name):
            raise VolumeException(EEXIST, f'snap "{snap_name}" already exists')

        SubvolumeV2.create_snapshot(self, snap_name)

    def stat_subvol_mnt_path(self):
        '''
        stat() path where data of subvol users is stored.

        This method overrides the v2 method, it must raise
        cephfs.ObjectNotFound if path(s) don't exists.
        '''
        try:
            self.fs.stat(self.gen_mnt_path())
        except ObjectNotFound:
            # when subvol is present but it has no incars
            self.fs.stat(self.base_path)

    def rm_snap(self, snap_name, force):
        snap_path = self.get_snap_path(snap_name)
        if not snap_path:
            raise VolumeException(ENOENT, f'snap "{snap_name}" does not exist')

        super(SubvolumeV2, self).remove_snapshot(snap_name, force)

        should_purge = False
        if self.v1.is_snap_path(snap_path):
            if self.v1.has_snap():
                return False

            self.md.remove_global_option('has_v1_snaps')
            self.md.flush()

            self.v1.mark_empty()
            should_purge = True
        elif self.v2.is_snap_path(snap_path):
            if self.v2.has_snap():
                return False

            self.md.remove_global_option('has_v2_snaps')
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

        if len(self.get_v3_incars()) == 0:
            self.trash_base_dir()
            should_purge = True

        return should_purge

    def has_snap(self, snap_name=None):
        if self.v1.has_snap(snap_name):
            return True
        elif self.v2.has_snap(snap_name):
            return True
        elif self.has_v3_snap(snap_name):
            return True
        else:
            return False

    def get_snap_names(self):
        all_snap_names = []
        all_snap_names += self.v1.get_snap_names()
        all_snap_names += self.v2.get_snap_names()
        all_snap_names += self.get_v3_snap_names()
        return all_snap_names

    def get_snap_path(self, snap_name):
        '''
        Find incarnation of the snapshot, including pre-v3 incarnations, and
        return path of the snapshot.
        '''
        if snap_path := self.v1.get_snap_path(snap_name):
            pass
        elif snap_path := self.v2.get_snap_path(snap_name):
            pass
        elif uuid := self.get_v3_incar_of_snap(snap_name):
            snap_path = self.gen_snap_path(snap_name, uuid)
        # none of the incarnations had a snap with this name.
        else:
            return None

        return snap_path if self.path_exists(snap_path) else None


    # ----- methods for subvol removal while retaining snaps -----

    def allowed_ops_by_state(self, vol_state):
        if vol_state == SubvolumeStates.STATE_COMPLETE:
            return {op_type for op_type in SubvolumeOpType}
        elif vol_state == SubvolumeStates.STATE_RETAINED:
            return {
                SubvolumeOpType.CREATE, SubvolumeOpType.REMOVE,
                SubvolumeOpType.REMOVE_FORCE, SubvolumeOpType.LIST,
                SubvolumeOpType.INFO,

                SubvolumeOpType.SNAP_CREATE, SubvolumeOpType.SNAP_REMOVE,
                SubvolumeOpType.SNAP_REMOVE_FORCE, SubvolumeOpType.SNAP_LIST,
                SubvolumeOpType.SNAP_GETPATH, SubvolumeOpType.SNAP_INFO,
                SubvolumeOpType.SNAP_PROTECT, SubvolumeOpType.SNAP_UNPROTECT,

                SubvolumeOpType.CLONE_SOURCE, SubvolumeOpType.CLONE_FAILED
            }
        else:
            return {SubvolumeOpType.REMOVE_FORCE, SubvolumeOpType.LIST,
                    SubvolumeOpType.CLONE_CREATE, SubvolumeOpType.CLONE_STATUS,
                    SubvolumeOpType.CLONE_CANCEL, SubvolumeOpType.CLONE_INTERNAL,
                    SubvolumeOpType.CLONE_SOURCE, SubvolumeOpType.CLONE_FAILED}

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


    # ----- methods for subvol auto-upgrade -----


    def _auto_upgrade(self, disc_version):
        if disc_version == 2:
            self._upgrade_from_v2()
        elif disc_version == 1:
            self._upgrade_from_v1()
        else:
            assert False, \
                f'disc_version = {disc_version}'

    def set_prev_path(self, prev_path):
        prev_path_xattr = 'ceph.dir.subvolume.prevpath'

        prev_path = to_str(prev_path)
        # caps will have '/' prefixing path, so ensure prev_path also has it.
        if prev_path[0] != '/':
            prev_path = '/' + prev_path

        # XXX: needs to be done as **early** as possible to keep the possibility
        # of client IO interruption minimum, keeping subvol upgrade seamless.
        #
        # XXX: applying prevpath vxattr on v1/v2 UUID path as it would be
        # renamed to mnt path. setting it after renaming v1/v2 UUID path to mnt
        # path create a minute period between them during which MDS will reject
        # client requests, causing client IO interruption.
        for i in range(100):
            try:
                self.fs.setxattr(prev_path, prev_path_xattr, prev_path)
            except ObjectNotFound:
                sleep(60)
            else:
                return
        else:
            raise V2UpgradeError(f'attempted setting {prev_path_xattr} every '
                                 'minute for last 2 hours but got '
                                 'ObjectNotFound exception everytime, MDS did '
                                 'not upgrade all this time')


    # ----- methods for subvol auto-upgrade from v2 -----


    def _upgrade_v2_to_v3_layout(self):
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

    def _update_v2_to_v3_meta(self):
        log.info(f'subvol {self.name}: updating meta file as part of v2 to v3'
                 'auto-upgrade...')

        try:
            self.md.refresh()

            self.md.update_global_section('version', self.VERSION)
            self.md.update_global_section('path', self.gen_mnt_path())
            self.md.update_global_section('prevpath', self.v2.uuid_path)
            if self.v2.has_snap():
                self.md.update_global_section('has_v2_snaps', True)

            self.md.flush()
        except MetadataMgrException as e:
            raise V2UpgradeError(e)

        log.info(f'subvol {self.name}: meta file update as part of v2 to v3 '
                 'upgrade was successful')

    def _upgrade_from_v2(self):
        log.info(f'subvol {self.name}: initiating v2 to v3 auto-upgrade')

        assert self.v2.uuid, \
            f'self.v2.uuid = {self.v2.uuid}'
        assert self.v2.uuid == self.uuid, \
            f'self.v2.uuid = {self.v2.uuid} self.uuid = {self.uuid}'

        self.set_prev_path(self.v2.uuid_path)
        self._reset_basic_paths()

        self.v2.upgrading = True
        self._upgrade_v2_to_v3_layout()
        self._reset_md_attrs()
        self._update_v2_to_v3_meta()

        self.v2.mark_upgraded()
        self.clean_stale_snapshot_metadata()

        log.info(f'subvol {self.name}: auto-upgrade from v2 to v3 was '
                 'successful')


    # ----- methods for subvol auto-upgrade from v1 -----


    def _upgrade_v1_to_v3_layout(self):
        log.info(f'subvol {self.name}: upgrading layout from v1 to v3...')

        try:
            uid, gid, mode = self.statx(self.v1.uuid_path, ('uid', 'gid',
                                                            'mode'))
            sv_xattrs = self.get_all_xattrs(self.v1.uuid_path)
            self.fs.mkdirs(self.gen_incar_path(), 0o755)
            self.fs.rename(self.v1.uuid_path, self.gen_mnt_path())
            self.fs.chmod(self.gen_mnt_path(), mode)
            self.fs.chown(self.gen_mnt_path(), uid, gid)
            if sv_xattrs:
                self.set_all_xattrs(self.gen_mnt_path(), sv_xattrs)

            self.fs.rename(self.v1.meta_path, self.meta_path)
            self.fs.symlink(self.meta_file_name, self.meta_symlink_path[1:])
            for path in (self.meta_path, self.meta_symlink_path):
                self.fs.chown(path, 0, 0)
                self.fs.chmod(path, 0o644)
        except Exception as e:
            raise V1UpgradeError(e)

        log.info(f'subvol {self.name}: layout upgrade from v1 to v3 was '
                 'successful')

    def _update_v1_to_v3_meta(self):
        log.info(f'subvol {self.name}: updating meta file as part of v1 to v3'
                 'auto-upgrade...')

        try:
            self.md.refresh()

            self.md.update_global_section('version', self.VERSION)
            self.md.update_global_section('path', self.gen_mnt_path())
            self.md.update_global_section('prevpath', self.v1.uuid_path)
            self.v1.mark_upgraded()
            if self.v1.has_snap():
                self.md.update_global_section('has_v1_snaps', True)
                self.md.update_global_section('v1_incar', self.uuid)

            self.md.flush()
        except MetadataMgrException as e:
            raise V1UpgradeError(e)

        log.info(f'subvol {self.name}: meta file update as part of v1 to v3 '
                 'upgrade was successful')

    def _upgrade_from_v1(self):
        log.info(f'subvol {self.name}: initiating v1 to v3 auto-upgrade')

        assert self.v1.uuid, \
            f'self.v1.uuid = {self.v1.uuid}'
        assert self.v1.uuid == self.uuid, \
            f'self.v1.uuid = {self.v1.uuid} self.uuid = {self.uuid}'

        self.v1.upgrading = True
        self.set_prev_path(self.v1.uuid_path)
        self._reset_basic_paths()

        self._upgrade_v1_to_v3_layout()
        self._reset_md_attrs()
        self._update_v1_to_v3_meta()

        assert self.uuid == self.v1.uuid
        self.clean_stale_snapshot_metadata()

        log.info(f'subvol {self.name}: auto-upgrade from v1 to v3 was '
                 'successful')
