import errno

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

class SubvolumeTemplate(object):
    VERSION = None

    @staticmethod
    def version():
        return SubvolumeTemplate.VERSION

    def open(self, need_complete=True, expected_types=[]):
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

    def pin(self, pin_type, pin_setting):
        """
        pin a subvolume

        :param pin_type: type of pin
        :param pin_setting: setting for pin
        :return: None
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

    def snapshot_path(self, snapname):
        """
        return the snapshot path for a given snapshot name

        :param: subvolume snapshot name
        :return: snapshot path
        """
        raise VolumeException(-errno.ENOTSUP, "operation not supported.")

    def list_snapshots(self):
        """
        list all subvolume snapshots.

        :param: None
        :return: None
        """
        raise VolumeException(-errno.ENOTSUP, "operation not supported.")

    def is_snapshot_protected(self, snapname):
        """
        check if a snapshot is protected.

        :param: snapname: snapshot to protect
        :return: True if the snapshot is protected, False otherwise.
        """
        raise VolumeException(-errno.ENOTSUP, "operation not supported.")

    def protect_snapshot(self, snapname):
        """
        protect a subvolume snapshot. only a protected snapshot can be cloned.

        :param: snapname: snapshot to protect
        :return: None
        """
        raise VolumeException(-errno.ENOTSUP, "operation not supported.")

    def unprotect_snapshot(self, snapname):
        """
        unprotect a subvolume snapshot. fail to unprotect if there are pending
        clone operations on the snapshot.

        :param: snapname: snapshot to unprotect
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
