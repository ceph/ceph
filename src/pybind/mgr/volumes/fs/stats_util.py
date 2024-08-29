'''
This module contains 3 sections -

1. Helper classes and helper functions for fetching, processing nd reporting
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
from .operations.trash import Trash
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


# TODO: update the guidelines here that shows how to derive and use this class properly
class AsyncJobProgressReporter:
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

    def __init__(self, volclient, volspec, OP_NAME):
        self.volspec = volspec

        # instance of VolumeClient is needed here so that call to
        # LibCephFS.getxattr() can be made.
        self.volclient = volclient

        # Creating an RTimer instance in advance so that we can check if async
        # job's progress reporting has already been initiated by calling
        # RTimer.is_alive().
        self.update_task = RTimer(1, self._update_progress_bars) # type: ignore

        # self.OP_NAME is to be set by derived classes. This is is need to
        # label progress bars, logging, etc.
        self.OP_NAME = OP_NAME

        # progress event ID for ongoing async jobs, define them after defining
        # them self.OP_NAME in deriverd classes
        self.on_pev_id: Optional[str] = f'mgr-vol-ongoing-{self.OP_NAME}'
        # progress event ID for ongoing+pending async jobs
        self.onpen_pev_id: Optional[str] = f'mgr-vol-total-{self.OP_NAME}'

    def initiate_reporting(self):
        assert self.OP_NAME
        assert isinstance(self.OP_NAME, str)
        assert 'None' not in self.OP_NAME

        if self.update_task.is_alive():
            log.info('progress reporting thread is already alive, not '
                     'initiating it again')
            return

        log.info(f'initiating progress reporting for {self.OP_NAME} '
                  'threads...')
        self.update_task = RTimer(1, self._update_progress_bars) # type: ignore
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
        '''
        Remove progress bars from "ceph status" output.
        '''
        log.info('removing progress bars from "ceph status" output')

        assert self.on_pev_id is not None and 'None' not in self.on_pev_id
        assert self.onpen_pev_id is not None and 'None' not in self.onpen_pev_id

        self.volclient.mgr.remote('progress', 'complete', self.on_pev_id)
        self.volclient.mgr.remote('progress', 'complete', self.onpen_pev_id)

        log.info('finished removing progress bars from "ceph status" output')

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


class CloneProgressReporter(AsyncJobProgressReporter):
    '''
    Report progress made by asychronous clone threads.
    '''

    def __init__(self, volclient, volspec):
        super().__init__(volclient, volspec, OP_NAME='clone')

        self.ongoing_clones_count = 0

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


# Following section contains code for fetching, processing and reporting
# statistics for the progress made by asynchronous purge threads


class PurgeProgressReporter(AsyncJobProgressReporter):
    '''
    Report progress made by asynchronous purge threads.
    '''

    def __init__(self, volclient, volspec):
        super().__init__(volclient, volspec, OP_NAME='purge')

        # TODO: num of concurrent purges should be constant that should be
        # imported and used here instead of setting it to 4 here.
        self.max_concurrent_purges = 4

    def initiate_reporting(self):
        super().initiate_reporting()

        subvol_count, file_count = self._get_trash_info()
        log.debug('collected stats of purge first time')

        # following 2 variables will be our reference to how many files/trash
        # entries have been deleted by purge threads, indicating progress
        # they've made.

        # init_subvol_count = initial num of trash entries
        self.init_subvol_count = subvol_count
        # init_file_count = initial num of files
        self.init_file_count = file_count
        log.debug(f'init: {self.init_file_count} files found in '
                   '{self.init_subvol_count} subvolumes')

    def _get_trash_info(self):
        file_count = 0
        subvol_count = 0
        trash_path = os_path_join(self.volspec.DEFAULT_SUBVOL_PREFIX,
                                  Trash.GROUP_NAME)

        volnames = list_volumes(self.volclient.mgr)

        for volname in volnames:
            with open_volume_lockless(self.volclient, volname) as fs_handle:
                file_count += int(fs_handle.getxattr(trash_path,
                                                     'ceph.dir.rfiles'))
                subvol_count += int(fs_handle.getxattr(trash_path,
                                                       'ceph.dir.subdirs'))

            log.debug(f'{file_count} files found in {subvol_count} subvolumes')
        return subvol_count, file_count

    def _update_progress_bars(self):
        subvol_count, file_count = self._get_trash_info()
        log.debug('collected stats of purge second time')
        if self.init_file_count == 0 and file_count != 0:
            # since initial count is zero but latest count is not zero,
            # let's check trash dir once more.
            self.subvol_count, self.init_file_count = subvol_count, file_count
            subvol_count, file_count = self._get_trash_info()
            log.debug('collected stats of purge one more additional time')

        if file_count == 0:
            self.finish()
            return

        self.show_onpen_bar = True if subvol_count > self.max_concurrent_purges else False

        diff = self.init_file_count - file_count
        # progress fraction that progress module accepts to print the progress bar.
        fraction = round(diff / self.init_file_count, 3)
        percent = round(fraction * 100, 3)
        msg = (f'Purging {self.init_subvol_count} '
               f'subvolumes/{self.init_file_count} files, average progress = '
               f'{percent}%')
        self._update_progress_bar_event(self.onpen_pev_id, msg, fraction)
        log.debug('finished updating progress bar for purges with message "{msg}"')
