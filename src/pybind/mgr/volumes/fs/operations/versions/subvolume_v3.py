from errno import EINVAL
from os.path import basename, dirname
from logging import getLogger

from cephfs import Error, InvalidValue, ObjectNotFound

from .subvolume_v2 import SubvolumeV2
from .subvolume_attrs import SubvolumeStates
from .metadata_manager import MetadataManager
from .auth_metadata import AuthMetadataManager
from ..trash import create_trashcan, open_trashcan
from ...utils import gen_uuid, verify_uuid, safe_join, to_utf8, to_str
from ...fs_util import listdir, path_exists, listsnaps
from ...exception import VolumeException, MetadataMgrException


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

    def list_dirs(self, path):
        return listdir(self.fs, path)

    def path_exists(self, path):
        return path_exists(self.fs, path)

    def list_snaps(self, path):
        return listsnaps(self.fs, self.spec, path)


class SubvolumeV3(SubvolumeV2):
    '''
    Code for v3 subvolume.

    /volumes/_nogroup/subvol123/incars/<uuid>/mnt
                        |         |       |     ^ self.get_mnt_path()
                        |         |       ^ self.get_incar_path()
                        |         ^ self.incar_base_path
                        ^ self.base_path

    /volumes/_nogroup/subvol123/.meta
                                 ^ self.meta_symlink_path, points to current
                                   incar's meta of subvol

    /volumes/_nogroup/subvol123/.meta.<uuid>
                                  ^ self.meta_path for incar <uuid>

    /volumes/_nogroup/subvol123/incars/<uuid>/.snap/snap123
                                               |      ^ self.get_snap_path()
                                               ^ self.get_snap_base_path()

    /volumes/_nogroup/subvol123/<uuid>
                                    ^ v1 and v2 take snap of this dir
    /volumes/_nogroup/subvol123/.snap/snap123
                                    |     ^ self.v2.find_snap()
                                    ^ self.v2.snap_base_path
    /volumes/_nogroup/subvol123/<uuid>/.snap/snap123
                                        |      ^ self.v1.find_snap()
                                        ^ self.v1.get_snap_base_dir

    /volumes/_nogroup/subvol123/
                        ^ v0 takes snap of this dir
    /volumes/_nogroup/subvol123/.snap/snap123
                                  |     ^ self.v0.find_snap()
                                  ^ self.v0.get_snap_base_dir
    '''

    VERSION: Final = 3

    META_FILE_NAME: Final = '.meta'
    # incar = incarnation
    INCAR_BASE_DIR: Final = 'incars'
    RETAINED_DIR: Final = 'retained'
    MNT_DIR: Final = 'mnt'


    # ----- init and related helper methods -----


    def __init__(self, mgr, fs, spec, group, name, uuid=None,
                 disc_version=None):
        self.mgr = mgr
        self.fs = fs
        self.spec = spec
        self.group = group

        self.name = name
        self.uuid = uuid
        if self.uuid:
            verify_uuid(self.uuid)

        self._reset_basic_attrs()

        # TODO: check if we can do without self.creating without too much
        # hassle?
        self.creating = (disc_version is None)

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
            self._base_path = safe_join(self.spec.vol_base_path,
                                        self.group.name, self.name)
        return self._base_path

    @property
    def incar_base_path(self):
        if not self._incar_base_path:
            self._incar_base_path = safe_join(self.base_path,
                                              self.INCAR_BASE_DIR)
        return self._incar_base_path

    @property
    def meta_file_name(self):
        if not self._meta_file_name:
            uuid = to_str(self.uuid)
            self._meta_file_name = to_utf8(self.META_FILE_NAME + '.' + uuid)
        return self._meta_file_name

    @property
    def meta_path(self):
        if not self._meta_path:
            self._meta_path = safe_join(self.base_path, self.meta_file_name)
        return self._meta_path

    @property
    def meta_symlink_path(self):
        if not self._meta_symlink_path:
            self._meta_symlink_path = safe_join(self.base_path,
                                                self.META_FILE_NAME)
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
        return to_utf8(f'{self.META_FILE_NAME}.{to_str(self.uuid)}')

    def get_meta_file_path(self, uuid=None):
        return safe_join(self.base_path, self.get_incar_meta_file_name(uuid))

    def get_mnt_path(self, uuid=None):
        return safe_join(self.get_incar_path(uuid), self.MNT_DIR)

    def get_retained_path(self, uuid=None):
        return safe_join(self.get_incar_path(uuid), self.RETAINED_DIR)

    def get_snap_base_path(self, uuid=None):
        return safe_join(self.get_incar_path(uuid), self.spec.snap_base_dir)

    def get_snap_path(self, snap_name, uuid=None):
        return safe_join(self.get_snap_base_path(uuid), snap_name)

    def get_v3_incars(self):
        return self.list_dirs(self.incar_base_path)


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
        assert self.uuid is None
        self.uuid = gen_uuid()
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


    def get_v3_snap_names(self, uuid=None):
        if uuid:
            path = self.get_snap_base_path(uuid)
            return self.list_snaps(path)
        else:
            snap_names = []
            for uuid in self.get_v3_incars():
                path = self.get_snap_base_path(uuid)
                snap_names += self.list_snaps(path)
            return snap_names

    def get_snap_names(self, uuid=None):
        if uuid:
            if uuid in self.get_v3_incars():
                return self.get_v3_snap_names(uuid)
        else:
            return self.get_v3_snap_names()

    # Listing all snaps can be expensive due to possibility of multiple snaps in
    # multiple incarnations. This methods returns as soon as it finds the given
    # snap instead of listing all snaps and then finding the given snap in the list.
    # So, use this method, instead, for finding a snap.
    def has_v3_snap(self, snap_name=None, uuid=None):
        if snap_name:
            uuid = uuid if uuid else self.get_v3_incar_of_snap(snap_name)
            path = self.get_snap_path(snap_name, uuid)
            return self.path_exists(path)
        else:
            uuids = (uuid,) if uuid else self.get_v3_incars()
            for uuid in uuids:
                path = self.get_snap_base_path(uuid)
                if not self.is_dir_empty(path):
                    return True
            return False

    def has_snap(self, snap_name=None, uuid=None):
        if self.v2.has_snap(snap_name, uuid):
            return True
        elif self.has_v3_snap(snap_name, uuid):
            return True
        else:
            return False

    def get_v3_incar_of_snap(self, snap_name):
        for uuid in self.get_v3_incars():
            path = self.get_snap_base_path(uuid)
            if snap_name in self.list_snaps(path):
                return uuid


    # ----- methods for snaps -----


    def find_snap(self, snap_name, get_uuid=False):
        '''
        Find incarnation of the snapshot, including pre-v3 incarnations, and
        return path of the snapshot.
        '''
        snap_name = to_utf8(snap_name)

        if uuid := self.get_v3_incar_of_snap(snap_name):
            snap_path = self.get_snap_path(snap_name, uuid)
        # none of the incarnations had a snap with this name.
        else:
            return (None, None) if get_uuid else None

        if self.path_exists(snap_path):
            return (snap_path, uuid) if get_uuid else snap_path
        else:
            return (None, None) if get_uuid else None

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

        # UUID can be none if snap is absent but don't raise any exception in
        # this case since command's behaviour is expected to be idempotent.
        if not (snap_path := self.find_snap(snap_name)):
            raise VolumeException(ENOENT, f'snap "{snap_name}" does not exist')

        SubvolumeV2.remove_snapshot(self, snap_name, force)
        if self.is_retained():
            if not self.has_snap():
                self.trash_base_dir()
                return True
        else:
            uuid = basename(dirname(dirname(snap_path)))
            if not self.has_v3_snap(uuid):
                self.trash_uuid_dir(uuid)
                return True
        return False


    # ----- methods for subvol removal while retaining snaps -----


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
