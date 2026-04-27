'''
This module contains classes, methods & helpers that are used to get statistics
(specifically number of files and total size of data present under the source
and destination directory for the copy operation that is performed for snapshot
cloning) and pass, print, log and convert them to human readable format
conveniently.
'''
import errno
from os.path import join as os_path_join
from typing import Optional
from logging import getLogger

from .operations.volume import open_volume_lockless, list_volumes
from .operations.subvolume import open_clone_subvol_pair_in_vol, open_subvol_in_vol
from .operations.template import SubvolumeOpType
from .operations.clone_index import open_clone_index, PATH_MAX
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


def get_stats(src_path, dst_path, fs_handle):
    rentries = 'ceph.dir.rentries'
    # set it to true when either src_path or dst_path has gone missing.
    either_path_gone_missing = False

    try:
        rentries_t = int(fs_handle.getxattr(src_path, rentries))
    except ObjectNotFound:
        either_path_gone_missing = True
        log.info(f'get_stats(): source path "{src_path}" went missing, '
                  'couldn\'t run getxattr on it')

    try:
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


class CloneProgressReporter:

    def __init__(self, volclient, vol_spec):
        self.vol_spec = vol_spec

        # instance of VolumeClient is needed here so that call to
        # LibCephFS.getxattr() can be made.
        self.volclient = volclient

        # Creating an RTimer instance in advance so that we can check if clone
        # reporting has already been initiated by calling RTimer.is_alive().
        self.update_task = RTimer(1, self._update_progress_bars)

        # progress event ID for ongoing clone jobs
        self.on_pev_id: Optional[str] = 'mgr-vol-ongoing-clones'
        # progress event ID for ongoing+pending clone jobs
        self.onpen_pev_id: Optional[str] = 'mgr-vol-total-clones'

        self.ongoing_clones_count = 0

    def initiate_reporting(self):
        if self.update_task.is_alive():
            log.info('progress reporting thread is already alive, not '
                     'initiating it again')
            return

        log.info('initiating progress reporting for clones...')
        self.update_task = RTimer(1, self._update_progress_bars)
        self.update_task.start()
        log.info('progress reporting for clones has been initiated')

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
            resolve_group_and_subvolume_name(self.vol_spec, dst_subvol_base_path)
        with open_subvol_in_vol(self.volclient, self.vol_spec, ci.volname,
                                ci.dst_group_name, ci.dst_subvol_name,
                                SubvolumeOpType.CLONE_INTERNAL) \
                                as (_, _, dst_subvol):
            ci.dst_path = dst_subvol.path
            log.debug(f'destination subvolume path for clone - {ci.dst_path}')

        clone_state = get_clone_state(self.volclient, self.vol_spec, ci.volname,
                                      ci.dst_group_name, ci.dst_subvol_name)
        if clone_state == SubvolumeStates.STATE_INPROGRESS:
            self.ongoing_clones_count += 1

        log.debug('finished collecting info for cloning destination')

    def _get_clone_src_info(self, fs_handle, ci):
        log.debug('collecting info for cloning source')

        with open_clone_subvol_pair_in_vol(self.volclient, self.vol_spec,
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
                with open_clone_index(fs_handle, self.vol_spec) as clone_index:
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

    def _update_progress_bar_event(self, ev_id, ev_msg, ev_progress_fraction):
        log.debug(f'ev_id = {ev_id} ev_progress_fraction = {ev_progress_fraction}')
        log.debug(f'ev_msg = {ev_msg}')
        log.debug('calling update() from mgr/update module')

        self.volclient.mgr.remote('progress', 'update', ev_id=ev_id,
                                  ev_msg=ev_msg,
                                  ev_progress=ev_progress_fraction,
                                  refs=['mds', 'clone'], add_to_ceph_s=True)

        log.debug('call to update() from mgr/update module was successful')

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

        assert self.on_pev_id is not None
        assert self.onpen_pev_id is not None

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
