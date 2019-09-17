import os
import uuid

class SubvolumeSpec(object):
    """
    Specification of a subvolume, identified by (subvolume-id, group-id) tuple. Add fields as
    required...
    """

    # where shall we (by default) create subvolumes
    DEFAULT_SUBVOL_PREFIX = "/volumes"
    # and the default namespace
    DEFAULT_NS_PREFIX = "fsvolumens_"

    # Reserved subvolume group name which we use in paths for subvolumes
    # that are not assigned to a group (i.e. created with group=None)
    NO_GROUP_NAME = "_nogroup"

    def __init__(self, subvolumeid, groupid, subvolume_prefix=None, pool_ns_prefix=None):
        assert groupid != SubvolumeSpec.NO_GROUP_NAME

        self.subvolumeid = subvolumeid
        self.groupid = groupid if groupid is not None else SubvolumeSpec.NO_GROUP_NAME
        self.subvolume_prefix = subvolume_prefix if subvolume_prefix else SubvolumeSpec.DEFAULT_SUBVOL_PREFIX
        self.pool_ns_prefix = pool_ns_prefix if pool_ns_prefix else SubvolumeSpec.DEFAULT_NS_PREFIX

    def is_default_group(self):
        """
        Is the group the default group?
        """
        return self.groupid == SubvolumeSpec.NO_GROUP_NAME

    @property
    def subvolume_id(self):
        """
        Return the subvolume-id from the subvolume specification
        """
        return self.subvolumeid

    @property
    def group_id(self):
        """
        Return the group-id from the subvolume secification
        """
        return self.groupid

    @property
    def subvolume_path(self):
        """
        return the subvolume path from subvolume specification
        """
        return os.path.join(self.group_path, self.subvolumeid.encode('utf-8'))

    @property
    def group_path(self):
        """
        return the group path from subvolume specification
        """
        return os.path.join(self.subvolume_prefix.encode('utf-8'), self.groupid.encode('utf-8'))

    @property
    def trash_path(self):
        """
        return the trash path from subvolume specification
        """
        return os.path.join(self.subvolume_prefix.encode('utf-8'), b"_deleting", self.subvolumeid.encode('utf-8'))

    @property
    def unique_trash_path(self):
        """
        return a unique trash directory entry path
        """
        return os.path.join(self.subvolume_prefix.encode('utf-8'), b"_deleting", str(uuid.uuid4()).encode('utf-8'))

    @property
    def fs_namespace(self):
        """
        return a filesystem namespace by stashing pool namespace prefix and subvolume-id
        """
        return "{0}{1}".format(self.pool_ns_prefix, self.subvolumeid)

    @property
    def trash_dir(self):
        """
        return the trash directory path
        """
        return os.path.join(self.subvolume_prefix.encode('utf-8'), b"_deleting")

    def make_subvol_snap_path(self, snapdir, snapname):
        """
        return the subvolume snapshot path for a given snapshot name
        """
        return os.path.join(self.subvolume_path, snapdir.encode('utf-8'), snapname.encode('utf-8'))

    def make_subvol_snapdir_path(self, snapdir):
        """
        return the subvolume snapdir path
        """
        return os.path.join(self.subvolume_path, snapdir.encode('utf-8'))

    def make_group_snap_path(self, snapdir, snapname):
        """
        return the group snapshot path for a given snapshot name
        """
        return os.path.join(self.group_path, snapdir.encode('utf-8'), snapname.encode('utf-8'))

    def make_group_snapdir_path(self, snapdir):
        """
        return the group's snapdir path
        """
        return os.path.join(self.group_path, snapdir.encode('utf-8'))

    def __str__(self):
        return "{0}/{1}".format(self.groupid, self.subvolumeid)
