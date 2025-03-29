from logging import getLogger
from uuid import uuid4

from .subvolume_v2 import SubvolumeV2


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

        self.subvol_dir = self.subvol_dir.encode('utf-8')
        self.roots_dir = self.roots_dir.encode('utf-8')
        self.current_meta = self.current_meta.encode('utf-8')
        # encoded already before calling __init__(), keeping this comment to
        # prevent accidental re-encoding in future.
        #self.meta = self.meta.encode('utf-8')

        self.uuid_dir = self.uuid_dir.encode('utf-8')
        self.mnt_dir = self.mnt_dir.encode('utf-8')

    @staticmethod
    def version():
        return SubvolumeV3.VERSION
