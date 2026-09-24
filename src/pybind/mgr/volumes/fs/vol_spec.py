from .operations.index import Index
from .operations.group import Group
from .operations.trash import Trash
from .operations.versions.subvolume_base import SubvolumeBase


class VolSpec(object):
    """
    specification of a "volume" -- base directory and various prefixes.
    """

    # default base path for subvolume groups
    DEFAULT_VOL_BASE_PATH = "/volumes"
    # and the default namespace
    DEFAULT_POOL_NS_PFX = "fsvolumens_"
    # default mode for subvol prefix and group
    DEFAULT_MODE = 0o755
    # internal directories
    INTERNAL_DIRS = [Group.NO_GROUP_NAME, Index.GROUP_NAME, Trash.GROUP_NAME, SubvolumeBase.LEGACY_CONF_DIR]

    def __init__(self, snap_base_dir, vol_base_path=None, pool_ns_pfx=None):
        self.SNAP_BASE_DIR = snap_base_dir

        if vol_base_path:
            self.VOL_BASE_PATH = vol_base_path
        else:
            self.VOL_BASE_PATH = self.DEFAULT_VOL_BASE_PATH

        if pool_ns_pfx:
            self.POOL_NS_PFX = pool_ns_pfx
        else:
            self.POOL_NS_PFX = self.DEFAULT_POOL_NS_PFX

        assert self.VOL_BASE_PATH[0] == '/'

    @property
    def snapshot_dir_prefix(self):
        """
        Return the snapshot directory prefix
        """
        return self.SNAP_BASE_DIR

    @property
    def base_dir(self):
        """
        Return the top level directory under which subvolumes/groups are created
        """
        return self.VOL_BASE_PATH

    @property
    def fs_namespace(self):
        """
        return a filesystem namespace by stashing pool namespace prefix and subvolume-id
        """
        return self.POOL_NS_PFX
