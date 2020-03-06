import errno
import logging

from .async_job import AsyncJobs
from .exception import VolumeException
from .operations.volume import open_volume, open_volume_lockless
from .operations.trash import open_trashcan

log = logging.getLogger(__name__)

# helper for fetching a trash entry for a given volume
def get_trash_entry_for_volume(volume_client, volname, running_jobs):
    log.debug("fetching trash entry for volume '{0}'".format(volname))

    try:
        with open_volume_lockless(volume_client, volname) as fs_handle:
            try:
                with open_trashcan(fs_handle, volume_client.volspec) as trashcan:
                    path = trashcan.get_trash_entry(running_jobs)
                    return 0, path
            except VolumeException as ve:
                if ve.errno == -errno.ENOENT:
                    return 0, None
                raise ve
    except VolumeException as ve:
        log.error("error fetching trash entry for volume '{0}' ({1})".format(volname, ve))
        return ve.errno, None

# helper for starting a purge operation on a trash entry
def purge_trash_entry_for_volume(volume_client, volname, purge_dir, should_cancel):
    log.debug("purging trash entry '{0}' for volume '{1}'".format(purge_dir, volname))

    ret = 0
    try:
        with open_volume_lockless(volume_client, volname) as fs_handle:
            with open_trashcan(fs_handle, volume_client.volspec) as trashcan:
                trashcan.purge(purge_dir, should_cancel)
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
        self.vc = volume_client
        super(ThreadPoolPurgeQueueMixin, self).__init__(volume_client, "puregejob", tp_size)

    def get_next_job(self, volname, running_jobs):
        return get_trash_entry_for_volume(self.vc, volname, running_jobs)

    def execute_job(self, volname, job, should_cancel):
        purge_trash_entry_for_volume(self.vc, volname, job, should_cancel)
