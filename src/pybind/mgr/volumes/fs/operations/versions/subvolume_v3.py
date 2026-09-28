from errno import EINVAL, EAGAIN, ENOTEMPTY, ENOENT, EEXIST, ESTALE
from os.path import basename, dirname
from logging import getLogger
from typing import Final
from time import sleep

from cephfs import Error, InvalidValue, ObjectNotFound

from .subvolume_v2 import SubvolumeV2
from .subvolume_attrs import SubvolumeStates
from .metadata_manager import MetadataManager
from .auth_metadata import AuthMetadataManager
from ..trash import create_trashcan, open_trashcan
from ...utils import gen_uuid, verify_uuid, safe_join, to_utf8, to_str
from ...fs_util import (listdir, path_exists, listsnaps, statx, is_dir_empty,
                        get_all_xattrs, set_all_xattrs)
from ...exception import (VolumeException, MetadataMgrException,
                          V2UpgradeError)


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

    @property
    def retained(self):
        return self.is_retained()

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
        return self.get_snap_base_path()

    # simply returns snap path, regardless of whether it exists.
    #
    # unfortunately, v1/v2 has this strange naming which doesn't tell enough.
    # adhering with it for now to avoid unrelated group of changes to codebase
    # in this patch series.
    def snapshot_path(self, snap_name):
        return self.get_snap_path(snap_name)

    # returns snap path if the snap exists.
    #
    # unfortunately, v1/v2 has this strange naming which doesn't tell enough.
    # adhering with it for now to avoid unrelated group of changes to codebase
    # in this patch series.
    def snapshot_data_path(self, snap_name):
        if snap_path := self.find_snap(snap_name):
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
        return listdir(self.fs, path)

    def path_exists(self, path, follow_symlink=True):
        return path_exists(self.fs, path)

    def list_snaps(self, path):
        return listsnaps(self.fs, self.spec, path)

    def statx(self, path, fields=None):
        return statx(self.fs, path, fields)

    def is_dir_empty(self, path):
        return is_dir_empty(self.fs, path)

    def get_all_xattrs(self, path):
        return get_all_xattrs(self.fs, path)

    def set_all_xattrs(self, path, xattrs):
        return set_all_xattrs(self.fs, path, xattrs)


class V2Incar(SubvolHelper, PreV3Helper):
    '''
    For helping SubvolV3 with its v2 incarnation.
    '''

    def __init__(self, fs, spec, base_path):
        self.fs = fs
        self.spec = spec
        self.base_path: Final = base_path
        self.reset_attrs()

    def reset_attrs(self):
        self._uuid = None
        self._meta_path = None
        self._uuid_path = None
        self._snap_base_path = None

    def fetch_uuid(self):
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
    def meta_path(self):
        if not self.uuid:
            return

        if not self._meta_path:
            self._meta_path = safe_join(self.base_path, '.meta')
        return self._meta_path

    @property
    def uuid_path(self):
        if not self.uuid:
            return

        if not self._uuid_path:
            self._uuid_path = safe_join(self.base_path, self.uuid)
        return self._uuid_path

    @property
    def snap_base_path(self):
        if not self.uuid:
            return

        if not self._snap_base_path:
            self._snap_base_path = safe_join(self.base_path, self.spec.snap_base_dir)
        return self._snap_base_path

    def get_snap_names(self):
        if not self.uuid:
            return []

        return self.list_snaps(self.snap_base_path)

    def find_snap(self, snap_name):
        if not self.uuid:
            return None

        if snap_name in self.get_snap_names():
            return safe_join(self.snap_base_path, snap_name)

    def has_snap(self, snap_name=None, uuid=None):
        if not self.uuid:
            return False

        if uuid and uuid != self.uuid:
            return False

        if snap_name:
            return snap_name in self.get_snap_names()
        else:
            if not self.path_exists(self.snap_base_path):
                return False
            return not self.is_dir_empty(self.snap_base_path)

    def is_snap_path(self, path):
        dir_name = basename(dirname(dirname(path)))
        subvol_name = basename(self.base_path)
        return dir_name == subvol_name


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

        self.name = to_utf8(name)
        self.uuid = to_utf8(uuid) if uuid else gen_uuid()
        verify_uuid(self.uuid)

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


    # ----- v3 helpers -----


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

    def is_snap_path(self, path):
        dir_name = basename(dirname(dirname(dirname(path))))
        return dir_name == INCAR_BASE_DIR


    # ----- v2 helpers -----


    @property
    def v2(self):
        if not self._v2:
            self._v2 = V2Incar(self.fs, self.spec, self.base_path)
        return self._v2

    def _reset_v2_helper(self):
        self._v2 = None


    # ----- methods for subvol creation, opening and removal -----


    def is_retained(self):
        if self.creating:
            return False
        else:
            return SubvolumeV2.retained.fget(self)

    def _remove_on_failure(self, retained):
        if not retained:
            log.info(f'removing subvol: {self.name}')
            self.remove(internal_cleanup=True)

        try:
            # current incarnation path
            curr_incar_path = self.get_incar_path()
            log.info(f'removing subvol incar path: {to_str(curr_incar_path)}')
            self.fs.rmdir(curr_incar_path)
        except Error as e:
            raise VolumeException(e)

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
        if not self.path_exists(self.get_incar_path()):
            return

        subvol_xattr = 'ceph.dir.subvolume'
        try:
            # MDS treats this as a no-op for already marked subvolume
            self.fs.setxattr(self.get_incar_path(), subvol_xattr, '1', 0)
        except InvalidValue:
            raise VolumeException(EINVAL, f'invalid value for "{subvol_xattr}"')
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
        SubvolumeV2.create_or_update_meta_file(self, self.get_mnt_path(),
                                               subvol_type)

        self.set_meta_symlink()

    def _create(self, mode, attrs, subvol_type, auth=True):
        assert self.creating
        self._reset_basic_paths()

        if not self.path_exists(self.group.path):
            self.fs.mkdirs(self.group.path, self.spec.DEFAULT_MODE)
        self.fs.mkdirs(self.get_mnt_path(), mode)

        self.mark_subvolume()
        self.set_attrs(self.get_mnt_path(), attrs)

        self.create_or_update_meta_file(subvol_type)
        self._reset_md_attrs()
        if auth:
            # Create the subvolume metadata file which manages auth-ids if it
            # doesn't exist
            self.auth_md.create_subvolume_metadata_file(self.group.name,
                                                        self.name)

    def remove(self, should_retain_snaps=False, internal_cleanup=False):
        if self.has_snap():
            if should_retain_snaps:
                self.remove_but_retain_snaps()
            else:
                raise VolumeException(ENOTEMPTY,
                                     f'subvolume {self.name} has snapshots')
        else:
            if internal_cleanup or \
               self.safe_to_remove_subvolume_clone(self.state):
                if len(self.get_v3_incars()) == 1:
                    self.remove_base_dir()
            else:
                raise VolumeException(EAGAIN,
                                      f'{self.name} clone in-progress, please '
                                      'cancel the clone and retry')

    def trash_base_dir(self):
        create_trashcan(self.fs, self.spec)

        with open_trashcan(self.fs, self.spec) as trashcan:
            # it's incorrect to call this method if there are more than one
            # incars.
            assert len(self.get_v3_incars()) == 1
            assert self.has_snap() is False
            trashcan.dump(self.base_path)

    def trash_uuid_dir(self, uuid):
        create_trashcan(self.fs, self.spec)

        with open_trashcan(self.fs, self.spec) as trashcan:
            trashcan.dump(self.get_incar_path(uuid))


    # ----- helper methods for snap code -----


    def get_v3_snap_names(self):
        snap_names = []
        for uuid in self.get_v3_incars():
            path = self.get_snap_base_path(uuid)
            snap_names += self.list_snaps(path)
        return snap_names

    def get_v3_incar_of_snap(self, snap_name):
        for uuid in self.get_v3_incars():
            path = self.get_snap_base_path(uuid)
            if snap_name in self.list_snaps(path):
                return uuid

    # Listing all snaps can be expensive due to possibility of multiple snaps in
    # multiple incarnations. This methods returns as soon as it finds the given
    # snap instead of listing all snaps and then finding the given snap in the list.
    # So, use this method, instead, for finding a snap.
    def has_v3_snap(self, snap_name=None):
        if snap_name:
            uuid = self.get_v3_incar_of_snap(snap_name)
            path = self.get_snap_path(snap_name, uuid)
            return self.path_exists(path)

        for uuid in self.get_v3_incars():
            path = self.get_snap_base_path(uuid)
            if not self.is_dir_empty(path):
                return True
        else:
            return False


    # ----- methods for snaps -----


    def create_snap(self, snap_name):
        snap_name = to_utf8(snap_name)

        if self.has_snap(snap_name):
            raise VolumeException(EEXIST, f'snap "{snap_name}" already exists')

        SubvolumeV2.create_snapshot(self, snap_name)

    # TOOD,v3: remove this method? subvol snap rm cmd needs to tested before
    # that.
    @property
    def purgeable(self):
        return False if not self.retained or self.list_snapshots() else True

    def stat_subvol_data_path(self):
        '''
        stat() path where data of subvol users is stored.

        This method overrides the v2 method, it must raise
        cephfs.ObjectNotFound if path(s) don't exists.
        '''
        try:
            self.fs.stat(self.get_mnt_path())
        except ObjectNotFound:
            # when subvol is present but it has no incars
            self.fs.stat(self.base_path)

    def rm_snap(self, snap_name, force):
        snap_name = to_utf8(snap_name)

        snap_path = self.find_snap(snap_name)
        # UUID can be none if snap is absent but don't raise any exception in
        # this case since command's behaviour is expected to be idempotent.
        if not snap_path:
            raise VolumeException(ENOENT, f'snap "{snap_name}" does not exist')

        SubvolumeV2.remove_snapshot(self, snap_name, force)
        if self.is_retained():
            if not self.has_snap():
                self.trash_base_dir()
                return True
        else:
            if self.v2.is_snap_path(snap_path):
                if self.v2.has_snap():
                    return False

                self.md.remove_global_option('has_v2_snaps')
                self.md.flush()

                self.v2.mark_empty()
                return True
            elif self.is_snap_path(snap_path):
                uuid = basename(dirname(dirname(snap_path)))
                verify_uuid(uuid)
                if self.is_incar_retained(uuid):
                    self.trash_uuid_dir(uuid)
                    return True
            else:
                log.info(f'snap_path = {snap_path}')
                assert False

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

    def find_snap(self, snap_name):
        '''
        Find incarnation of the snapshot, including pre-v3 incarnations, and
        return path of the snapshot.
        '''
        snap_name = to_utf8(snap_name)

        if snap_path := self.v2.find_snap(snap_name):
            pass
        elif uuid := self.get_v3_incar_of_snap(snap_name):
            snap_path = self.get_snap_path(snap_name, uuid)
        # none of the incarnations had a snap with this name.
        else:
            return None

        return snap_path if self.path_exists(snap_path) else None


    # ----- methods for subvol removal while retaining snaps -----


    def is_incar_retained(self, uuid):
        subdirs = self.list_dirs(self.get_incar_path(uuid))
        assert len(subdirs) == 1,  \
            f'subdirs = {subdirs}'

        return (subdirs[0] in (RETAINED_DIR, MNT_DIR))

    def update_meta_file_after_retain(self):
        self.md.remove_section(self.md.USER_METADATA_SECTION)

        self.md.update_global_section('key', self.get_reatined_path())
        self.md.update_global_section('state',
                                      SubvolumeStates.STATE_RETAINED.value)

        self.md.flush()

    def remove_but_retain_snaps(self):
        assert self.state != SubvolumeStates.STATE_RETAINED

        try:
            self.update_meta_file_after_retain()
            self.fs.rename(self.get_mnt_path(), self.get_retained_path())
            self.auth_md.delete_subvolume_metadata_file(self.group.name,
                                                        self.name)
        except MetadataMgrException as e:
            log.error(f"failed to write config: {e}")
            raise VolumeException(e)


    # ----- methods for subvol auto-upgrade -----


    def _auto_upgrade(self, disc_version):
        if disc_version == 2:
            self._upgrade_from_v2()
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
        # XXX: applying prevpath vxattr on v2 UUID path as it would be renamed
        # renamed to mnt path. setting it after renaming v2 UUID path to mnt
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

            self.fs.mkdirs(self.get_incar_path(), 0o755)
            self.fs.rename(self.v2.uuid_path, self.get_mnt_path())
            self.fs.chmod(self.get_mnt_path(), mode)
            self.fs.chown(self.get_mnt_path(), uid, gid)
            if sv_xattrs:
                self.set_all_xattrs(self.get_mnt_path(), sv_xattrs)

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
            self.md.update_global_section('path', self.get_mnt_path())
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
