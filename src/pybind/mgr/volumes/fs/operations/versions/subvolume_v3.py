import errno
from os.path import basename, join
from logging import getLogger

from cephfs import Error

from .subvolume_v2 import SubvolumeV2
from .subvolume_attrs import SubvolumeStates
from .metadata_manager import MetadataManager
from .auth_metadata import AuthMetadataManager
from ..trash import create_trashcan, open_trashcan
from ...fs_util import listdir, list_snaps
from ...exception import VolumeException, MetadataMgrException


log = getLogger(__name__)


class PreV3Helper:
    '''
    Methods that help make v3 code compatible with and v2, v1 and v0.
    '''

    @property
    def base_path(self):
        return self.subvol_path

    @property
    def config_path(self):
        return self.meta_path

    def snapshot_base_path(self):
        return self.get_incar_snap_base_path()

    def snapshot_data_path(self, snap_name):
        '''
        Path to a specific snapshot named 'snap_name'.
        '''
        return self.get_snap_path(snap_name)


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
        return safe_join(self.get_incar_path(uuid), self.spec.snap_base_path)

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


    # ----- methods for subvol creation and opening/discovery -----


    def set_subvol_xattr(self):
        subvol_xattr = 'ceph.dir.subvolume'

        try:
            # MDS treats this as a noop for already marked subvolume
            self.fs.setxattr(self.get_incar_path(), subvol_xattr, b'1', 0)
        except InvalidValue:
            raise VolumeException(EINVAL, f'invalid value for {subvol_xattr}')
        except Error as e:
            raise VolumeException(-e.args[0], e.args[1])

    def create_or_update_meta_file(self, subvol_type):
        super(SubvolumeV3, self).create_or_update_meta_file(subvol_type)

        if self.path_exists(self.meta_path)
            self.fs.unlink(self.meta_path)

        self.fs.symlink(self.meta_file_name, self.meta_symlink_path)

    def _create(self, mode, attrs, subvol_type, auth=True):
        if not self.path_exists(self.group.path):
            self.fs.mkdirs(self.group.path, self.spec.DEFAULT_MODE)
        self.fs.mkdirs(self.get_incar_mnt_path(), mode)

        self.set_subvol_xattr()
        self.set_attrs(self.get_incar_mnt_path(), attrs)

        self.create_or_update_meta_file(subvol_type)
        if auth:
            # Create the subvolume metadata file which manages auth-ids if it
            # doesn't exist
            self.auth_mdata_mgr.create_subvolume_metadata_file(self.group.name,
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
        # code under _trash_subvol_dir can be move here technically but this
        # extra layer of call has been added to indicate that in subvol v3
        # terms
        self.trash_subvol_dir()

    # since there is not in-subvol ".trash" dir in subvol v3, this method
    # should always return False
    @property
    def has_pending_purges(self):
        return False


    # ----- methods for snapshot creation -----


    def get_incar_uuid_for_snap(self, snap_name):
        '''
        Return incarnation's UUID in which the snapshot name is present.
        When multiple incarnations for a subvolume exists, check if a snap
        exists in one of the incarnations.
        '''
        for uuid in self.get_v3_incars():
            # construct path to ".snap" directory for given UUID.
            path = self.get_incar_snap_base_path(uuid)
            if snap_name in self.list_snaps(path):
                return uuid

    def create_snapshot(self, snap_name):
        if self.get_incar_uuid_for_snap(snap_name) != None:
            raise VolumeException(errno.EEXIST,
                                  f'subvolume \'{snap_name}\' already exists')

        super(SubvolumeV3, self).create_snapshot(snap_name)

    def remove_snapshot(self, snap_name, force):
        # XXX: UUID can be none if snap is absent but don't raise any exception
        # in this case since command's behaviour is expected to be idempotent.
        uuid = self.get_incar_uuid_for_snap(snap_name)
        snap_path = self.snapshot_path(snap_name, uuid=uuid)

        super(SubvolumeV3, self).remove_snapshot(snap_name, force=force,
                                                 snap_path=snap_path)

    def remove_but_retain_snaps(self):
        assert self.state != SubvolumeStates.STATE_RETAINED

        try:
            self.update_meta_file_after_retain()
            self.trash_incarnation_dir()

            self.auth_mdata_mgr.delete_subvolume_metadata_file(self.group.name,
                                                               self.name)
        except MetadataMgrException as e:
            log.error(f"failed to write config: {e}")
            raise VolumeException(e.args[0], e.args[1])

    # in subvol v3, self.mnt_dir (AKA data dir) is renamed to ".unlinked" if
    # subvol is deleted but snapshots are retained.
    def trash_incarnation_dir(self):
        self.fs.rename(self.mnt_dir, self.unlinked_dir)

    def update_meta_file_after_retain(self):
        self.metadata_mgr.remove_section(MetadataManager.USER_METADATA_SECTION)

        self.metadata_mgr.update_global_section('key', self.unlinked_path)
        self.metadata_mgr.update_global_section('key',
                                                SubvolumeStates.STATE_RETAINED.value)

        self.metadata_mgr.flush()


    # ----- methods for clone operations -----


    def get_snap_path(self, snap_name):
        uuid = self.get_incar_uuid_for_snap(snap_name)
        if uuid == None:
            raise VolumeException(-errno.ENOENT,
                                  f'snapshot \'{snap_name}\' does not exist')
        elif uuid == self.uuid:
            snap_path = join(self.snapshot_path(snap_name), b'mnt')
        else:
            snap_path = join(self.roots_dir, uuid,
                             self.vol_spec.snapshot_dir_prefix.encode('utf-8'),
                             snap_name.encode('utf-8'), b'mnt')

        # v2 raises exception if the snapshot path do not exist so do the same
        # to prevent any bugs due to difference in behaviour.
        #
        # not raising exception indeed leads to a bug: the volumes plugin fails
        # when exception is not raised by this method when it is called by
        # do_clone() method of async_cloner.py. this is made to happen by a
        # test by deleting snapshot after running the snapshot clone command
        # but before the clone operation actually begins. this is done by
        # adding a delay using mgr/volumes/snapshot_clone_delay config option.
        try:
            self.fs.stat(snap_path)
        except cephfs.ObjectNotFound as e:
            if e.errno == errno.ENOENT:
                raise VolumeException(-errno.ENOENT,
                                      f'snapshot \'{snap_name}\' does not exist')
            raise VolumeException(-e.args[0], e.args[1])

        return snap_path

    @property
    def purgeable(self):
        return False if not self.retained or self.list_snapshots() else True

    def list_snapshots(self):
        '''
        Return list of name of all snapshots from all the incarnations.
        '''
        # list of all incarnations/UUID dirs of this subvolume.
        incars = listdir(self.fs, self.roots_dir)

        all_snap_names = []

        for incar_uuid in incars:
            # construct path to ".snap" directory for given UUID.
            snap_dir = join(self.roots_dir, incar_uuid,
                            self.vol_spec.snapshot_dir_prefix.encode('utf-8'))
            all_snap_names.extend(list_snaps(self.fs, self.vol_spec, snap_dir))
        return all_snap_names
