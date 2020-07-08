import errno

from enum import Enum, unique

from ..exception import VolumeException

class GroupTemplate(object):
    def list_subvolumes(self):
        raise VolumeException(-errno.ENOTSUP, "operation not supported.")

    def create_snapshot(self, snapname):
        """
        create a subvolume group snapshot.

        :param: group snapshot name
        :return: None
        """
        raise VolumeException(-errno.ENOTSUP, "operation not supported.")

    def remove_snapshot(self, snapname):
        """
        remove a subvolume group snapshot.

        :param: group snapshot name
        :return: None
        """
        raise VolumeException(-errno.ENOTSUP, "operation not supported.")

    def list_snapshots(self):
        """
        list all subvolume group snapshots.

        :param: None
        :return: None
        """
        raise VolumeException(-errno.ENOTSUP, "operation not supported.")

@unique
class SubvolumeOpType(Enum):
    CREATE          = 'create'
    REMOVE          = 'rm'
    REMOVE_FORCE    = 'rm-force'
    PIN             = 'pin'
    LIST            = 'ls'
    GETPATH         = 'getpath'
    INFO            = 'info'
    RESIZE          = 'resize'
    SNAP_CREATE     = 'snap-create'
    SNAP_REMOVE     = 'snap-rm'
    SNAP_LIST       = 'snap-ls'
    SNAP_INFO       = 'snap-info'
    SNAP_PROTECT    = 'snap-protect'
    SNAP_UNPROTECT  = 'snap-unprotect'
    CLONE_SOURCE    = 'clone-source'
    CLONE_CREATE    = 'clone-create'
    CLONE_STATUS    = 'clone-status'
    CLONE_CANCEL    = 'clone-cancel'
    CLONE_INTERNAL  = 'clone_internal'

class SubvolumeTemplate(object):
    VERSION = None # type: int

    @staticmethod
    def version():
        return SubvolumeTemplate.VERSION

    def open(self, op_type):
        raise VolumeException(-errno.ENOTSUP, "operation not supported.")

    def status(self):
        raise VolumeException(-errno.ENOTSUP, "operation not supported.")

    def create(self, size, isolate_nspace, pool, mode, uid, gid):
        """
        set up metadata, pools and auth for a subvolume.

        This function is idempotent.  It is safe to call this again
        for an already-created subvolume, even if it is in use.

        :param size: In bytes, or None for no size limit
        :param isolate_nspace: If true, use separate RADOS namespace for this subvolume
        :param pool: the RADOS pool where the data objects of the subvolumes will be stored
        :param mode: the user permissions
        :param uid: the user identifier
        :param gid: the group identifier
        :return: None
        """
        raise VolumeException(-errno.ENOTSUP, "operation not supported.")

    def create_clone(self, pool, source_volname, source_subvolume, snapname):
        """
        prepare a subvolume to be cloned.

        :param pool: the RADOS pool where the data objects of the cloned subvolume will be stored
        :param source_volname: source volume of snapshot
        :param source_subvolume: source subvolume of snapshot
        :param snapname: snapshot name to be cloned from
        :return: None
        """
        raise VolumeException(-errno.ENOTSUP, "operation not supported.")

    def remove(self):
        """
        make a subvolume inaccessible to guests.

        This function is idempotent.  It is safe to call this again

        :param: None
        :return: None
        """
        raise VolumeException(-errno.ENOTSUP, "operation not supported.")

    def resize(self, newsize, nshrink):
        """
        resize a subvolume

        :param newsize: new size In bytes (or inf/infinite)
        :return: new quota size and used bytes as a tuple
        """
        raise VolumeException(-errno.ENOTSUP, "operation not supported.")

    def create_snapshot(self, snapname):
        """
        snapshot a subvolume.

        :param: subvolume snapshot name
        :return: None
        """
        raise VolumeException(-errno.ENOTSUP, "operation not supported.")

    def remove_snapshot(self, snapname):
        """
        remove a subvolume snapshot.

        :param: subvolume snapshot name
        :return: None
        """
        raise VolumeException(-errno.ENOTSUP, "operation not supported.")

    def list_snapshots(self):
        """
        list all subvolume snapshots.

        :param: None
        :return: None
        """
        raise VolumeException(-errno.ENOTSUP, "operation not supported.")

    def attach_snapshot(self, snapname, tgt_subvolume):
        """
        attach a snapshot to a target cloned subvolume. the target subvolume
        should be an empty subvolume (type "clone") in "pending" state.

        :param: snapname: snapshot to attach to a clone
        :param: tgt_subvolume: target clone subvolume
        :return: None
        """
        raise VolumeException(-errno.ENOTSUP, "operation not supported.")

    def detach_snapshot(self, snapname, tgt_subvolume):
        """
        detach a snapshot from a target cloned subvolume. the target subvolume
        should either be in "failed" or "completed" state.

        :param: snapname: snapshot to detach from a clone
        :param: tgt_subvolume: target clone subvolume
        :return: None
        """
        raise VolumeException(-errno.ENOTSUP, "operation not supported.")
