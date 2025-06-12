import errno
from os.path import basename, join
from logging import getLogger
from uuid import uuid4

import cephfs

from .metadata_manager import MetadataManager
from .subvolume_attrs import SubvolumeStates
from .subvolume_v2 import SubvolumeV2
from ..trash import create_trashcan, open_trashcan
from ...exception import VolumeException
from ...fs_util import listdir, create_base_dir


log = getLogger(__name__)


PATH_MAX = 4096


class SubvolumeV3(SubvolumeV2):
    '''
    1. Following is layout of subvol v3 directories.

        /volumes/<group>/<subvol>/roots/<uuid1>/mnt
                                                ^ mount dir
                                        ^ data dir/uuid dir
                                    ^ roots dir
                         ^ subvol dir

    2. Following is the path of meta file -

    /volumes/<group>/<subvol>/.meta.<uuid3>

    3. UUID dir of current incarnation can be found through the symlink -

    /volumes/<group>/<subvol>/roots/{.meta -> .meta.<uuid3>}

    NOTE: Absence of ".meta" implies that subvolume has been deleted, taking
    snapshots will not be possible then.

    4. Subvolume metadata (.fscrypt and .snap for example) lives in UUID dir -

    /volumes/<group>/<subvol>/roots/<uuid1>/.fscrypt
    /volumes/<group>/<subvol>/roots/<uuid1>/.snap

    5. This is how a subvol will look with previous incarnations -

    /volumes/<group>/<subvol>/roots/<uuid1>
    /volumes/<group>/<subvol>/roots/<uuid2>
    /volumes/<group>/<subvol>/roots/<uuid3>
    /volumes/<group>/<subvol>/roots/.meta.<uuid1>
    /volumes/<group>/<subvol>/roots/.meta.<uuid2>
    /volumes/<group>/<subvol>/roots/.meta.<uuid3>
    '''

    VERSION = 3

    def __init__(self, mgr, fs, vol_spec, group, subvolname, legacy=False,
                 uuid=None):
        # XXX: this needs to be defined beforehand since __init__() below calls
        # __init__() from previous versions and previous versions needs
        # self.base_path to be defined. self.subvol_dir in v3 is same
        # self.base_path in older versions.
        self.subvol_dir = f'/volumes/{group.groupname}/{subvolname}'

        # XXX: both of these needs to be defined beforehand because __init__()
        # below will initialize metadata manager too which results in
        # self.config_path() being called. and self.config_path() needs this
        # variable (see the method's comment for the reason behind it).
        if uuid:
            self.uuid = uuid
        else:
            self.uuid = uuid4()

        self.meta = f'{self.subvol_dir}/.meta.{self.uuid}'

        # encode these variables since they'll be used in __init__() below and
        # all its underlying calls.
        self.meta = self.meta.encode('utf-8')
        self.subvol_dir = self.subvol_dir.encode('utf-8')

        super(SubvolumeV3, self).__init__(mgr, fs, vol_spec, group, subvolname)

        # decoding it so that rest of the paths can be built using this path.
        # It must be encoded again before this method ends since rest of the
        # class needs it in encoded form.
        self.subvol_dir = self.subvol_dir.decode('utf-8')

        # contains data dir for all incarnations
        self.roots_dir = f'{self.subvol_dir}/roots'
        # meta file for the current subvolume's incarnation
        self.current_meta = f'{self.subvol_dir}/.meta'

        self.uuid_dir = f'{self.roots_dir}/{self.uuid}'
        self.mnt_dir = f'{self.uuid_dir}/mnt'
        self.unlinked_dir = f'{self.uuid_dir}/.unlinked'

        self.snap_dir = f'{self.uuid_dir}/{self.vol_spec.snapshot_dir_prefix}'
        self.fscrypt_dir = f'{self.uuid_dir}/.fscrypt'

        self.subvol_dir = self.subvol_dir.encode('utf-8')
        self.roots_dir = self.roots_dir.encode('utf-8')
        self.current_meta = self.current_meta.encode('utf-8')
        # encoded already before calling __init__(), keeping this comment to
        # prevent accidental re-encoding in future.
        #self.meta = self.meta.encode('utf-8')

        self.uuid_dir = self.uuid_dir.encode('utf-8')
        self.mnt_dir = self.mnt_dir.encode('utf-8')
        self.unlinked_dir = self.unlinked_dir.encode('utf-8')

        self.snap_dir = self.snap_dir.encode('utf-8')
        self.fscrypt_dir = self.fscrypt_dir.encode('utf-8')

    @staticmethod
    def version():
        return SubvolumeV3.VERSION

    @property
    def base_path(self):
        return self.subvol_dir

    @property
    def config_path(self):
        '''
        Path to meta file for current incarnation of the subvolume.

        NOTE: overriding method from class SubvolumeBase, since meta file's name
        now contains UUID in it and in class SubvolumeBase UUID can't be
        accessed.
        '''
        return self.meta


    # following methods either help or do subvolume creation and opening/discovery


    def set_subvol_xattr(self):
        # set subvolume attr, on subvolume root, marking it as a CephFS subvolume
        # subvolume root is where snapshots would be taken, and hence is the base_path for v2 subvolumes
        try:
            # MDS treats this as a noop for already marked subvolume
            self.fs.setxattr(self.uuid_dir, 'ceph.dir.subvolume', b'1', 0)
        except cephfs.InvalidValue:
            raise VolumeException(-errno.EINVAL, "invalid value specified for ceph.dir.subvolume")
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

    def _create_v3_layout(self, mode):
        create_base_dir(self.fs, self.group.path, self.vol_spec.DEFAULT_MODE)
        self.fs.mkdirs(self.mnt_dir, mode)

    def create_or_update_meta_file(self, subvol_type):
        super(SubvolumeV3, self).create_or_update_meta_file(subvol_type)

        try:
            self.fs.stat(self.current_meta)
            self.fs.unlink(self.current_meta)
        except cephfs.ObjectNotFound:
            pass

        self.fs.symlink(basename(self.meta), self.current_meta)

    def _create(self, mode, attrs, subvol_type, auth=True):
        self._create_v3_layout(mode)

        self.set_subvol_xattr()
        self.set_attrs(self.mnt_dir, attrs)

        self.create_or_update_meta_file(subvol_type)
        if auth:
            # Create the subvolume metadata file which manages auth-ids if it
            # doesn't exist
            self.auth_mdata_mgr.create_subvolume_metadata_file(
                self.group.groupname, self.subvolname)


    # following are methods that help or do subvol deletion


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

    def is_mnt_dir_empty(self):
        if self.state == SubvolumeStates.STATE_RETAINED:
            # "retained" state implies "subvol rm --retian-snapshots" was run
            # and therefore it doesn't matter if it contains files or not, it
            # will be considered empty. in fact, self.mnt_dir must be absent
            # at this point.
            return True
        else:
            return not listdir(self.fs, self.mnt_dir)

    def trash_subvol_dir(self):
        create_trashcan(self.fs, self.vol_spec)

        with open_trashcan(self.fs, self.vol_spec) as trashcan:
            trashcan.dump(self.subvol_dir)

    def remove(self, retainsnaps=False, internal_cleanup=False):
        super(SubvolumeV3, self).remove(retainsnaps, internal_cleanup)

        # if entire subvol dir was deleted, and not just incarnation dir, then
        # .meta file was also deleted along and therefore skip unlinking/
        # re-linking it and return.
        try:
            self.fs.stat(self.subvol_dir)
        except cephfs.ObjectNotFound:
            return

        #self.fs.unlink(self.current_meta)
        #self.fs.symlink('dummy', self.current_meta)
        #self.uuid = None
        #self.meta = self.current_meta

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


    # following are methods that help or do snapshot creation


    def snapshot_path(self, snap_name, uuid=None):
        '''
        Path to a specific snapshot named 'snap_name'.
        '''
        if uuid:
            return join(self.roots_dir, uuid,
                        self.vol_spec.snapshot_prefix.encode('utf-8'),
                        snap_name.encode('utf-8'))
        else:
            return join(self.snapshot_base_path(), snap_name.encode('utf-8'))

    def snapshot_base_path(self):
        return self.snap_dir

    def get_incar_uuid_for_snap(self, snap_name):
        '''
        Return incarnation's UUID in which the snapshot name is present.
        When multiple incarnations for a subvolume exists, check if a snap
        exists in one of the incarnations.
        '''
        # list of all incarnations/UUID dirs of this subvolume.
        incars = listdir(self.fs, self.roots_dir)

        for incar_uuid in incars:
            # construct path to ".snap" directory for given UUID.
            snap_dir = join(self.roots_dir, incar_uuid,
                            self.vol_spec.snapshot_dir_prefix.encode('utf-8'))
            all_snap_names = listdir(self.fs, snap_dir)
            # encode since listdir() call above returns list of bytes and list
            # of str
            if snap_name.encode('utf-8') in all_snap_names:
                return incar_uuid

        return None

    def create_snapshot(self, snap_name):
        if self.get_incar_uuid_for_snap(snap_name) != None:
            raise VolumeException(errno.EEXIST,
                                  f'subvolume \'{snap_name}\' already exists')

        super(SubvolumeV3, self).create_snapshot(snap_name)

    def remove_snapshot(self, snap_name, force):
        uuid = self.get_incar_uuid_for_snap(snap_name)
        if uuid == None:
            raise VolumeException(errno.ENOENT,
                                  f'snapshot \'{snap_name}\' does not exists')

        super(SubvolumeV3, self).remove_snapshot(snap_name, force=force,
                                                 uuid=uuid)

    # in subvol v3, self.mnt_dir (AKA data dir) is renamed to ".unlinked" if
    # subvol is deleted but snapshots are retained.
    def trash_incarnation_dir(self):
        self.fs.rename(self.mnt_dir, self.unlinked_dir)

    def update_meta_file_after_retain(self):
        self.metadata_mgr.remove_section(MetadataManager.USER_METADATA_SECTION)
        self.metadata_mgr.update_section(MetadataManager.GLOBAL_SECTION,
                                         MetadataManager.GLOBAL_META_KEY_PATH,
                                         self.unlinked_dir.decode('utf-8'))
        self.metadata_mgr.update_global_section(
            MetadataManager.GLOBAL_META_KEY_STATE,
            SubvolumeStates.STATE_RETAINED.value)
        self.metadata_mgr.flush()


    # Following methods help clone operation -


    def snapshot_data_path(self, snap_name):
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
        all_snap_names = []

        # list of all incarnations/UUID dirs of this subvolume.
        incars = listdir(self.fs, self.roots_dir)

        for incar_uuid in incars:
            # construct path to ".snap" directory for given UUID.
            snap_dir = join(self.roots_dir, incar_uuid,
                            self.vol_spec.snapshot_dir_prefix.encode('utf-8'))
            all_snap_names.extend(listdir(self.fs, snap_dir))
        return all_snap_names
