'''
This module contains code for reporting progress statistics for async clone jobs.
'''
from os.path import join as os_path_join
from logging import getLogger

from .stats_util import get_amount_copied, AsyncJobProgressReporter, \
    get_amount_copied, get_size_ratio_str, get_num_ratio_str, get_percent_copied
from .operations.volume import open_volume_lockless, list_volumes
from .operations.subvolume import open_clone_subvol_pair_in_vol, open_subvol_in_vol
from .operations.template import SubvolumeOpType
from .operations.clone_index import open_clone_index, PATH_MAX
from .operations.resolver import resolve_group_and_subvolume_name
from .exception import VolumeException

from cephfs import ObjectNotFound


log = getLogger(__name__)


def get_clone_stats(src_path, dst_path, fs_handle):
    rentries = 'ceph.dir.rentries'
    # rentries_t = total rentries
    rentries_t = int(fs_handle.getxattr(src_path, rentries))
    # rentries_c = rentries copied (so far)
    rentries_c = int(fs_handle.getxattr(dst_path, rentries))

    size_t, size_c, percent = get_amount_copied(src_path, dst_path, fs_handle)

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
    Report progress made by cloner threads.
    '''

    def __init__(self, volclient, volspec):
        super().__init__(volclient, volspec)

        # need to figure out how many progress bars should be printed. print 1
        # progress bar if number of ongoing clones is less than this value,
        # else print 2.
        self.max_concurrent_clones = self.volclient.mgr.max_concurrent_clones

        self.op_name = 'clone'

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
                        if e.error_str != 'error fetching subvolume metadata':
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
                  f'{len(clones)} clones')
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

        # onpen bar (that is progress bar for clone jobs in ongoing and pending
        # state) is printed when clones are in pending state. it is kept in
        # printing until all clone jobs finish.
        show_onpen_bar = True if len(clones) > self.max_concurrent_clones \
            else False

        percent = 0.0

        assert self.on_pev_id is not None
        sum_percent_ongoing = 0.0
        avg_percent_ongoing = 0.0
        total_ongoing_clones = min(len(clones), self.max_concurrent_clones)

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
