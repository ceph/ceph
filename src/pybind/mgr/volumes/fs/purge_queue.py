import errno
import logging
import os
import stat

import cephfs

from .async_job import AsyncJobs
from .exception import VolumeException
from .operations.resolver import resolve_trash
from .operations.template import SubvolumeOpType
from .operations.group import open_group
from .operations.subvolume import open_subvol
from .operations.volume import open_volume, open_volume_lockless
from .operations.trash import open_trashcan

log = logging.getLogger(__name__)


# helper for fetching a trash entry for a given volume
def get_trash_entry_for_volume(fs_client, volspec, volname, running_jobs):
    log.debug("fetching trash entry for volume '{0}'".format(volname))

    try:
        with open_volume_lockless(fs_client, volname) as fs_handle:
            try:
                with open_trashcan(fs_handle, volspec) as trashcan:
                    path = trashcan.get_trash_entry(running_jobs)
                    return 0, path
            except VolumeException as ve:
                if ve.errno == -errno.ENOENT:
                    return 0, None
                raise ve
    except VolumeException as ve:
        log.error("error fetching trash entry for volume '{0}' ({1})".format(volname, ve))
        return ve.errno, None


def subvolume_purge(fs_client, volspec, volname, trashcan, subvolume_trash_entry, should_cancel):
    groupname, subvolname = resolve_trash(volspec, subvolume_trash_entry.decode('utf-8'))
    log.debug("subvolume resolved to {0}/{1}".format(groupname, subvolname))

    try:
        with open_volume(fs_client, volname) as fs_handle:
            with open_group(fs_handle, volspec, groupname) as group:
                with open_subvol(fs_client.mgr, fs_handle, volspec, group, subvolname, SubvolumeOpType.REMOVE) as subvolume:
                    log.debug("subvolume.path={0}, purgeable={1}".format(subvolume.path, subvolume.purgeable))
                    if not subvolume.purgeable:
                        return
                    # this is fine under the global lock -- there are just a handful
                    # of entries in the subvolume to purge. moreover, the purge needs
                    # to be guarded since a create request might sneak in.
                    trashcan.purge(subvolume.base_path, should_cancel)
    except VolumeException as ve:
        if not ve.errno == -errno.ENOENT:
            raise


# helper for starting a purge operation on a trash entry
def purge_trash_entry_for_volume(fs_client, volspec, volname, purge_entry, should_cancel):
    log.debug("purging trash entry '{0}' for volume '{1}'".format(purge_entry, volname))

    ret = 0
    try:
        with open_volume_lockless(fs_client, volname) as fs_handle:
            with open_trashcan(fs_handle, volspec) as trashcan:
                try:
                    pth = os.path.join(trashcan.path, purge_entry)
                    stx = fs_handle.statx(pth, cephfs.CEPH_STATX_MODE | cephfs.CEPH_STATX_SIZE,
                                          cephfs.AT_SYMLINK_NOFOLLOW)
                    if stat.S_ISLNK(stx['mode']):
                        tgt = fs_handle.readlink(pth, 4096)
                        tgt = tgt[:stx['size']]
                        log.debug("purging entry pointing to subvolume trash: {0}".format(tgt))
                        delink = True
                        try:
                            trashcan.purge(tgt, should_cancel)
                        except VolumeException as ve:
                            if not ve.errno == -errno.ENOENT:
                                delink = False
                                return ve.errno
                        finally:
                            if delink:
                                subvolume_purge(fs_client, volspec, volname, trashcan, tgt, should_cancel)
                                log.debug("purging trash link: {0}".format(purge_entry))
                                trashcan.delink(purge_entry)
                    else:
                        log.debug("purging entry pointing to trash: {0}".format(pth))
                        trashcan.purge(pth, should_cancel)
                except cephfs.Error as e:
                    log.warn("failed to remove trash entry: {0}".format(e))
    except VolumeException as ve:
        ret = ve.errno
    return ret


class ThreadPoolPurgeQueueMixin(AsyncJobs):
    """
    Purge queue mixin class maintaining a pool of threads for purging trash entries.
    Subvolumes are chosen from volumes in a round robin fashion. If some of the purge
    entries (belonging to a set of volumes) have huge directory tree's (such as, lots
    of small files in a directory w/ deep directory trees), this model may lead to
    _all_ threads purging entries for one volume (starving other volumes).
    """
    def __init__(self, volume_client, tp_size):
        super(ThreadPoolPurgeQueueMixin, self).__init__(volume_client, "purgejob", tp_size)

        self.vc = volume_client

    def get_next_job(self, volname, running_jobs):
        return get_trash_entry_for_volume(self.fs_client, self.vc.volspec, volname, running_jobs)

    def execute_job(self, volname, job, should_cancel):
        purge_trash_entry_for_volume(self.fs_client, self.vc.volspec, volname, job, should_cancel)
