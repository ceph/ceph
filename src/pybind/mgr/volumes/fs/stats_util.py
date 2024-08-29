'''
This module contains 3 sections -

1. Helper classes and helper functions for fetching, processing and reporting
   progress statistics for async jobs. This code doesn't depend on the type of
   asynchronous job.

   These statistics can be reported in two ways. First, as a Python dictionary
   which is eventually printed in output of a command (see output of "ceph fs
   clone status" command when clone jobs are in-progress, for example). And
   second, as a progress bar which is printed in output of "ceph status" command.

2. Code to fetch, process and report statistics of the progress made by the
   asynchronous clone threads.

3. Code to fetch, process and report statistics of the progress made by the
   asynchronous purge threads.
'''
import errno
from os.path import join as os_path_join
from typing import Optional
from logging import getLogger

from .operations.volume import open_volume_lockless, list_volumes
from .operations.subvolume import open_clone_subvol_pair_in_vol, open_subvol_in_vol
from .operations.template import SubvolumeOpType
from .operations.clone_index import open_clone_index, PATH_MAX
from .operations.trash import get_trashcan_stats
from .operations.resolver import resolve_group_and_subvolume_name
from .exception import VolumeException
from .async_cloner import get_clone_state
from .operations.versions.subvolume_attrs import SubvolumeStates

from mgr_util import RTimer, format_bytes, format_dimless
from cephfs import ObjectNotFound


log = getLogger(__name__)


def get_size_ratio_str(size1, size2):
    size1, size2 = format_bytes(size1, 4), format_bytes(size2, 4)

    size_string =  f'{size1}/{size2}'
    size_string = size_string.replace(' ', '')
    return size_string


def get_num_ratio_str(num1, num2):
    num1, num2 = format_dimless(num1, 4), format_dimless(num2, 4)

    num_string = f'{num1}/{num2}'
    num_string = num_string.replace(' ', '')
    return num_string


def get_amount_copied(src_path, dst_path, fs_handle):
    rbytes = 'ceph.dir.rbytes'

    try:
        size_t = int(fs_handle.getxattr(src_path, rbytes))
    except ObjectNotFound:
        log.info(f'get_amount_copied(): source path "{src_path}" went missing, '
                  'couldn\'t run getxattr on it')
        return

    try:
        size_c = int(fs_handle.getxattr(dst_path, rbytes))
    except ObjectNotFound:
        log.info(f'get_amount_copied(): destination path "{dst_path}" went '
                  'missing, couldn\'t run getxattr on it')
        return

    percent: Optional[float]
    if size_t == 0 or size_c == 0:
        percent = 0
    else:
        percent = ((size_c/size_t) * 100)
        percent = round(percent, 3)

    return size_t, size_c, percent


def get_percent_copied(src_path, dst_path, fs_handle):
    retval = get_amount_copied(src_path, dst_path, fs_handle)
    if not retval:
        return retval
    else:
        _, _, percent = retval
        return percent


class VolumesProgressBar:
    '''
    This class contains code necessary for initiating and finishing reporting
    of async jobs. This includes spawning and terminating reporter thread and
    addition and removal of progress bars to "ceph status" output.

    =======HOW TO USE THIS CLASS=======
    1. Inherit this class, call its __init___() and pass OP_NAME to __init__().
       It should indicate the operation for which the progress is to be
       reported. For example, for cloning and purging it should be 'clone' and
       'purge' respectively.
    2. Write all code for updating progress bars under the method
       "_update_progress_bars()" directly or under a method called by this
       method.
    3. When the async job is finished, call self.finish().
    '''

    def __init__(self, volclient, volspec, OP_NAME, RTIMER_SLEEP=1):
        self.volspec = volspec

        # instance of VolumeClient is needed here so that call to
        # LibCephFS.getxattr() can be made.
        self.volclient = volclient

        self.volspec = volspec

        # Creating an RTimer instance in advance so that we can check if async
        # job's progress reporting has already been initiated by calling
        # RTimer.is_alive().
        self.update_task = RTimer(1, self._update_progress_bars) # type: ignore

        self.RTIMER_SLEEP = RTIMER_SLEEP

        # this is to be set by derived classes. This is neeeded for logging
        # and other misc reasons
        self.OP_NAME = OP_NAME

        # Creating an RTimer instance in advance so that we can check if async
        # job's progress reporting has already been initiated by calling
        # RTimer.is_alive().
        self.update_task = RTimer(self.RTIMER_SLEEP,
                                  self._update_progress_bars) # type: ignore

    def initiate(self):
        assert self.OP_NAME
        assert isinstance(self.OP_NAME, str)

        if self.update_task.is_alive():
            log.info('progress reporting thread is already alive, not '
                     'initiating it again')
            return

        log.info(f'initiating progress reporting for {self.OP_NAME} '
                  'threads...')
        self.update_task = RTimer(self.RTIMER_SLEEP,
                                  self._update_progress_bars) # type: ignore
        self.update_task.start()
        log.info(f'progress reporting for {self.OP_NAME} threads has been '
                  'initiated')

    def _update_progress_bar_event(self, ev_id, ev_msg, ev_progress_fraction):
        log.debug(f'ev_id = {ev_id} ev_progress_fraction = {ev_progress_fraction}')
        log.debug(f'ev_msg = {ev_msg}')
        log.debug('calling update() from mgr/update module')

        self.volclient.mgr.remote('progress', 'update', ev_id=ev_id,
                                  ev_msg=ev_msg,
                                  ev_progress=ev_progress_fraction,
                                  refs=['mds', self.OP_NAME], add_to_ceph_s=True)

        log.debug('call to update() from mgr/update module was successful')

    def _finish_progress_events(self):
        raise RuntimeError('Should be implemented by subclass')

    def finish(self):
        '''
        All cloning jobs have been completed. Terminate this RTimer thread.
        '''
        self._finish_progress_events()

        log.info(f'marking this RTimer thread as finished; thread object ID - {self}')
        self.update_task.finished.set()


# Following section contains code for fetching, processing and reporting
# statistics for the progress made by asynchronous clone threads

def get_clone_stats(src_path, dst_path, fs_handle):
    rentries = 'ceph.dir.rentries'

    # set it to true when either src_path or dst_path has gone missing.
    either_path_gone_missing = False

    try:
        # rentries_t = total rentries
        rentries_t = int(fs_handle.getxattr(src_path, rentries))
    except ObjectNotFound:
        either_path_gone_missing = True
        log.info(f'get_stats(): source path "{src_path}" went missing, '
                  'couldn\'t run getxattr on it')

    try:
        # rentries_c = rentries copied (so far)
        rentries_c = int(fs_handle.getxattr(dst_path, rentries))
    except ObjectNotFound:
        either_path_gone_missing = True
        log.info(f'get_stats(): destination path "{dst_path}" went missing, '
                  'couldn\'t run getxattr on it')

    if either_path_gone_missing:
        return {}

    retval = get_amount_copied(src_path, dst_path, fs_handle)
    if not retval:
        return {}

    size_t, size_c, percent = retval
    return {
        'percentage cloned': percent,
        'amount cloned': get_size_ratio_str(size_c, size_t),
        'files cloned': get_num_ratio_str(rentries_c, rentries_t),
    }


class CloneInfo:

    def __init__(self, volname):
        self.volname = volname

        self.src_group_name = None
        self.src_subvol_name = None
        self.src_path = None

        self.dst_group_name = None
        self.dst_subvol_name = None
        self.dst_path = None


class CloneProgressBar(VolumesProgressBar):
    '''
    Report progress made by asychronous clone threads.
    '''

    def __init__(self, volclient, volspec):
        super().__init__(volclient, volspec, OP_NAME='clone')

        self.ongoing_clones_count = 0
        # progress event ID for ongoing async jobs
        self.on_pev_id: Optional[str] = 'mgr-vol-ongoing-clone'
        # progress event ID for ongoing+pending async jobs
        self.onpen_pev_id: Optional[str] = 'mgr-vol-total-clone'

    def _get_clone_dst_info(self, fs_handle, ci, clone_entry,
                            clone_index_path):
        log.debug('collecting info for cloning destination')

        ce_path = os_path_join(clone_index_path, clone_entry)
        # XXX: This may raise ObjectNotFound exception. As soon as cloning is
        # finished, clone entry is deleted by cloner thread. This exception is
        # handled in _get_info_for_all_clones().
        dst_subvol_base_path = fs_handle.readlink(ce_path, PATH_MAX).\
            decode('utf-8')

        ci.dst_group_name, ci.dst_subvol_name = \
            resolve_group_and_subvolume_name(self.volspec, dst_subvol_base_path)
        with open_subvol_in_vol(self.volclient, self.volspec, ci.volname,
                                ci.dst_group_name, ci.dst_subvol_name,
                                SubvolumeOpType.CLONE_INTERNAL) \
                                as (_, _, dst_subvol):
            ci.dst_path = dst_subvol.path
            log.debug(f'destination subvolume path for clone - {ci.dst_path}')

        clone_state = get_clone_state(self.volclient, self.volspec, ci.volname,
                                      ci.dst_group_name, ci.dst_subvol_name)
        if clone_state == SubvolumeStates.STATE_INPROGRESS:
            self.ongoing_clones_count += 1

        log.debug('finished collecting info for cloning destination')

    def _get_clone_src_info(self, fs_handle, ci):
        log.debug('collecting info for cloning source')

        with open_clone_subvol_pair_in_vol(self.volclient, self.volspec,
                                           ci.volname, ci.dst_group_name,
                                           ci.dst_subvol_name) as \
                                           (dst_subvol, src_subvol, snap_name):
            ci.src_group_name = src_subvol.group_name
            ci.src_subvol_name = src_subvol.subvolname
            ci.src_path = src_subvol.snapshot_data_path(snap_name)
            log.debug(f'source subvolume path for clone - {ci.src_path}')

        log.debug('finished collecting info for cloning source')

    def _get_info_for_all_clones(self):
        clones:list[CloneInfo] = []

        log.debug('collecting all entries in clone index...')
        volnames = list_volumes(self.volclient.mgr)
        for volname in volnames:
            with open_volume_lockless(self.volclient, volname) as fs_handle:
                with open_clone_index(fs_handle, self.volspec) as clone_index:
                    clone_index_path = clone_index.path
                    # get clone in order in which they were launched, this
                    # should be same as the ctime on clone entry.
                    clone_index_entries = clone_index.list_entries_by_ctime_order()
                    log.debug('finished collecting all clone index entries, '
                              f'found {len(clones)} clone index entries')

                # reset ongoing clone counter before iterating over all clone
                # entries
                self.ongoing_clones_count = 0

                log.debug('collecting info for clones found through clone index '
                         'entries...')
                for ce in clone_index_entries:
                    ci = CloneInfo(volname)

                    try:
                        self._get_clone_dst_info(fs_handle, ci, ce,
                                                 clone_index_path)
                        self._get_clone_src_info(fs_handle, ci)
                    except ObjectNotFound as e:
                        log.info('Exception ObjectNotFound was raised. Apparently '
                                 'entry in clone index was removed because one of '
                                 'the clone job(s) has completed/cancelled, '
                                 'therefore ignoring and proceeding. '
                                 f'Printing the exception: {e}')
                        continue
                    except VolumeException as e:
                        if e.errno != -errno.EINVAL:
                            raise
                        log.info('Exception VolumeException was raised. Apparently '
                                 'an entry from the metadata file of clone source '
                                 'was removed because one of the clone job(s) has '
                                 'completed/cancelled. Therefore ignoring and '
                                 f'proceeding Printing the exception: {e}')
                        continue

                    if not ci.src_path or not ci.dst_path:
                        continue

                    clones.append(ci)

        log.debug('finished collecting info on all clones, found '
                  f'{len(clones)} clones out of which '
                  f'{self.ongoing_clones_count} are ongoing clones')
        return clones

    def _update_progress_bars(self):
        '''
        Look for amount of progress made by all cloning operations and prints
        progress bars, in "ceph -s" output, for average progress made
        accordingly.

        This method is supposed to be run only by instance of class RTimer
        present in this class.
        '''
        clones = self._get_info_for_all_clones()
        if not clones:
            self.finish()
            return

        # there has to be 1 ongoing clone for this method to run, perhaps it
        # wasn't found by it because the index entry for it hasn't been created
        # yet.
        if self.ongoing_clones_count == 0:
            self.ongoing_clones_count = 1

        # onpen bar (that is progress bar for clone jobs in ongoing and pending
        # state) is printed when clones are in pending state. it is kept in
        # printing until all clone jobs finish.
        show_onpen_bar = True if len(clones) > self.ongoing_clones_count \
            else False

        percent = 0.0

        assert self.on_pev_id is not None
        sum_percent_ongoing = 0.0
        avg_percent_ongoing = 0.0
        total_ongoing_clones = min(len(clones), self.ongoing_clones_count)

        if show_onpen_bar:
            assert self.onpen_pev_id is not None
            sum_percent_onpen = 0.0
            avg_percent_onpen = 0.0
            total_onpen_clones = len(clones)

        for clone in clones:
            with open_volume_lockless(self.volclient, clone.volname) as \
                    fs_handle:
                percent = get_percent_copied(clone.src_path, clone.dst_path,
                                             fs_handle)
                if not percent:
                    continue
                if clone in clones[:total_ongoing_clones]:
                    sum_percent_ongoing += percent
                if show_onpen_bar:
                    sum_percent_onpen += percent

        avg_percent_ongoing = round(sum_percent_ongoing / total_ongoing_clones, 3)
        # progress module takes progress as a fraction between 0.0 to 1.0.
        avg_progress_fraction = avg_percent_ongoing / 100
        msg = (f'{total_ongoing_clones} ongoing clones - average progress is '
               f'{avg_percent_ongoing}%')
        self._update_progress_bar_event(ev_id=self.on_pev_id, ev_msg=msg,
            ev_progress_fraction=avg_progress_fraction)
        log.debug('finished updating progress bar for ongoing clones with '
                  f'following message - {msg}')

        if show_onpen_bar:
            avg_percent_onpen = round(sum_percent_onpen / total_onpen_clones, 3)
            # progress module takes progress as a fraction between 0.0 to 1.0.
            avg_progress_fraction = avg_percent_onpen / 100
            msg = (f'Total {total_onpen_clones} clones - average progress is '
                   f'{avg_percent_onpen}%')
            self._update_progress_bar_event(ev_id=self.onpen_pev_id, ev_msg=msg,
                                        ev_progress_fraction=avg_progress_fraction)
            log.debug('finished updating progress bar for ongoing+pending '
                      f'clones with following message - {msg}')

    def _finish_progress_events(self):
        '''
        Remove progress bars from "ceph status" output.
        '''
        log.info('removing progress bars from "ceph status" output')

        assert self.on_pev_id is not None and 'None' not in self.on_pev_id
        assert self.onpen_pev_id is not None and 'None' not in self.onpen_pev_id

        self.volclient.mgr.remote('progress', 'complete', self.on_pev_id)
        self.volclient.mgr.remote('progress', 'complete', self.onpen_pev_id)

        log.info('finished removing progress bars from "ceph status" output')


# Following section contains code for fetching, processing and reporting
# statistics for the progress made by asynchronous purge threads


def get_trash_stats_for_all_vols(volclient):
    subvol_count = 0
    file_count = 0
    size = 0

    volnames = list_volumes(volclient.mgr)
    for volname in volnames:
        s_count, f_count, size_ = get_trashcan_stats(volclient, volname)
        log.debug(f'In trash directory of volume "{volname}", {s_count} '
                  f'subvolumes containing {f_count} files were found')
        subvol_count += s_count
        file_count += f_count
        size += size_

    log.debug(f'In trash directory of {len(volnames)} volumes, '
              f'{subvol_count} subvolumes containing total {file_count} '
               'files were found')
    return subvol_count, file_count, size


class TrashStats:

    def __init__(self, volclient):
        self.volclient = volclient

        # total number of subvols in trashcan.
        self.total_subvols = 0
        # total number of files in trashcan.
        self.total_files = 0
        # total amount of data in trashcan.
        self.total_size = 0

        # number of subvols left in trashcan.
        self.subvols_left = 0
        # number of files left in trashcan.
        self.files_left = 0
        # amount of data left in trashcan.
        self.size_left = 0

    def fetch_initial_stats(self):
        trash_stats = get_trash_stats_for_all_vols(self.volclient)

        self.total_subvols = trash_stats[0]
        self.total_files = trash_stats[1]
        self.total_size = trash_stats[2]

    def fetch_leftover_data_stats(self):
        trash_stats = get_trash_stats_for_all_vols(self.volclient)

        self.subvols_left = trash_stats[0]
        self.files_left = trash_stats[1]
        self.size_left = trash_stats[2]

    def update_total_data_stats(self):
        self.total_subvols = self.subvols_left
        self.total_files = self.files_left
        self.total_size = self.size_left

    def is_trash_empty(self):
        return self.subvols_left == self.files_left == self.size_left == 0

    def are_laggy(self):
        return self.total_files < self.files_left

    def get_fraction_and_percent(self):
        files_purged = self.total_files - self.files_left
        # progress fraction that progress module accepts to print the progress bar.
        fraction = round(files_purged / self.total_files, 3)
        percent = round(fraction * 100, 3)
        return fraction, percent


class PurgeProgressBar(VolumesProgressBar):
    '''
    Report progress made by asynchronous purge threads.
    '''

    def __init__(self, volclient, volspec):
        super().__init__(volclient, volspec, OP_NAME='purge', RTIMER_SLEEP=1)

        self.pev_id = 'mgr-vol-ongoing-purge'
        self.trash_stats = TrashStats(self.volclient)

        # count how many times rstats were found to be laggy
        self.laggy_count = 0
        # after how many laggy counts should a note be shown to the user.
        self.LAGGY_NOTE_LIMIT = 20
        # after how many laggy counts progress bar be disabled.
        self.LAGGY_DISABLE_LIMIT = self.LAGGY_NOTE_LIMIT * 3

        # in case laggy rstats, previous msg and previous progress on progress
        # bar are printed. therefore they need to be saved.
        self.prev_msg = None
        self.prev_fraction = None

    def initiate(self):
        super().initiate()

        self.trash_stats.fetch_initial_stats()
        log.debug('fetching rstats for trash dir for the first time, setting '
                  f'initial stats: {self.trash_stats.total_subvols} subvols '
                  f'containing {self.trash_stats.total_files} files')

    def _update_progress_bars(self):
        self.trash_stats.fetch_leftover_data_stats()

        if self.trash_stats.is_trash_empty():
            self.finish()
            return

        # XXX: latest stats are higher than initial stats, it means either the
        # rstats are laggy or more subvols were moved to trash dir. in any case,
        # update init_file_count and init_subvol_count.
        if self.trash_stats.are_laggy():
            self.laggy_count += 1

            self.trash_stats.update_total_data_stats()
            log.debug('rstats values for trash dir increased instead of '
                      'decreasing, because they are laggy. updating initial '
                      'stats to latest stats found. latest stats: '
                      f'{self.trash_stats.subvols_left} subvols, '
                      f'{self.trash_stats.files_left} files')

            if self.laggy_count < self.LAGGY_NOTE_LIMIT:
                return
            elif (self.laggy_count < self.LAGGY_DISABLE_LIMIT and
                  self.laggy_count >= self.LAGGY_NOTE_LIMIT and
                  self.prev_msg  and self.prev_fraction):
                # to avoid printing negative progress, print previous progress
                # and inform user that rstats are laggy
                self.prev_msg += ' (laggy file system stats)'
                self._update_progress_bar_event(self.pev_id, self.prev_msg,
                                                    self.prev_fraction)
                return
            elif self.laggy_count >= self.LAGGY_DISABLE_LIMIT:
                # don't print the progress bar but don't stop this thread.
                # when rstats stop being laggy, progress bar will be printed
                # again.
                self._finish_progress_events()
                return

        # reset laggy count since rstats aren't laggy anymore.
        self.laggy_count = 0
        fraction, percent = self.trash_stats.get_fraction_and_percent()
        msg = (f'Purging {self.trash_stats.total_subvols} subvolumes/'
               f'{self.trash_stats.total_files} files, average progress = '
               f'{percent}%')

        # save msg and fraction so that they can be reused in case rstats are
        # found to be laggy in next round.
        self.prev_msg = msg
        self.prev_fraction = fraction

        self._update_progress_bar_event(self.pev_id, msg, fraction)
        log.debug(f'finished updating purge progress bar with message: {msg}')

    def _finish_progress_events(self):
        '''
        Remove progress bars from "ceph status" output.
        '''
        assert self.pev_id is not None and 'None' not in self.pev_id
        self.volclient.mgr.remote('progress', 'complete', self.pev_id)
        log.debug('finished removing progress bars from "ceph status" output')
