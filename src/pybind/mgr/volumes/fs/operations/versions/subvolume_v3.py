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

    def are_there_other_incarnations(self):
        if len(listdir(self.fs, self.roots_dir)) > 1:
            return True
        elif len(listdir(self.fs, self.roots_dir)) == 1:
            return False
        else:
            raise RuntimeError('self.roots_dir can\'t have zero or less than '
                               'zero directories in it')

    def trash_subvol_dir(self):
        create_trashcan(self.fs, self.vol_spec)

        if self.is_mnt_dir_empty() and not self.are_there_other_incarnations():
            with open_trashcan(self.fs, self.vol_spec) as trashcan:
                trashcan.dump(self.subvol_dir)

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


    def snapshot_base_path(self):
        return self.snap_dir

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
