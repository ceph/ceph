import os
from .operations.index import Index
from .operations.group import Group
from .operations.trash import Trash
from .operations.versions.subvolume_base import SubvolumeBase


class VolSpec(object):
    """
    specification of a "volume" -- base directory and various prefixes.
    """

    # where shall we (by default) create subvolumes
    DEFAULT_SUBVOL_PREFIX = "/volumes"
    # and the default namespace
    DEFAULT_NS_PREFIX = "fsvolumens_"
    # default mode for subvol prefix and group
    DEFAULT_MODE = 0o755
    # internal directories
    INTERNAL_DIRS = [Group.NO_GROUP_NAME, Index.GROUP_NAME, Trash.GROUP_NAME, SubvolumeBase.LEGACY_CONF_DIR]

    def __init__(self, snapshot_prefix, subvolume_prefix=None, pool_ns_prefix=None):
        self.snapshot_prefix = snapshot_prefix
        self.subvolume_prefix = subvolume_prefix if subvolume_prefix else VolSpec.DEFAULT_SUBVOL_PREFIX
        self.pool_ns_prefix = pool_ns_prefix if pool_ns_prefix else VolSpec.DEFAULT_NS_PREFIX

    @property
    def snapshot_dir_prefix(self):
        """
        Return the snapshot directory prefix
        """
        return self.snapshot_prefix

    @property
    def base_dir(self):
        """
        Return the top level directory under which subvolumes/groups are created
        """
        return self.subvolume_prefix

    @property
    def fs_namespace(self):
        """
        return a filesystem namespace by stashing pool namespace prefix and subvolume-id
        """
        return self.pool_ns_prefix
