'''
Contains helpers that are used to get statistics (specifically number of
regular files and directories and total size of data present under a given
directory) and pass, print, log and convert them to human readable format
conveniently.
'''
from os.path import join as os_path_join
from logging import getLogger
from time import time, sleep
from uuid import uuid4
from typing import Optional

from .operations.volume import open_volume_lockless, list_volumes
from .operations.subvolume import open_clone_sv_pair_in_vol
from .operations.clone_index import open_clone_index
from .operations.resolver import resolve_grp_and_sv_names

from mgr_util import RTimer, format_bytes, format_dimless
from cephfs import ObjectNotFound

log = getLogger(__name__)


def get_size_ratio_str(size1, size2, human=True):
    if human:
        size1, size2 = format_bytes(size1, 4), format_bytes(size2, 4)

    size_string =  f'{size1}/{size2}'
    size_string = size_string.replace(' ', '')
    return size_string


def get_num_ratio_str(num1, num2, human=True):
    if human:
        num1, num2 = format_dimless(num1, 4), format_dimless(num2, 4)

    num_string = f'{num1}/{num2}'
    num_string = num_string.replace(' ', '')
    return num_string


def get_amount_copied(src_path, dst_path, fsh, human=True):
    rbytes = 'ceph.dir.rbytes'

    size_t = int(fsh.getxattr(src_path, rbytes))
    size_c = int(fsh.getxattr(dst_path, rbytes))

    percent: Optional[float]
    if size_t == 0 or size_c == 0:
        percent = 0
    else:
        percent = ((size_c/size_t) * 100)
        percent = round(percent, 3)

    return size_t, size_c, percent


def get_percent_copied(src_path, dst_path, fsh, human=True):
    _, _, percent = get_amount_copied(src_path, dst_path, fsh)
    return percent


def get_stats(src_path, dst_path, fsh, human=True):
    rentries = 'ceph.dir.rentries'
    rentries_t = int(fsh.getxattr(src_path, rentries))
    rentries_c = int(fsh.getxattr(dst_path, rentries))

    size_t, size_c, percent = get_amount_copied(src_path, dst_path, fsh,
                                                human)

    return {
        'percentage cloned': percent,
        'amount cloned': get_size_ratio_str(size_c, size_t, human),
        'regfiles cloned': get_num_ratio_str(rentries_c, rentries_t, human),
    }


class CloneInfo:

    def __init__(self, volname=None, src_grp_name=None, src_sv_name=None,
                 src_path=None, dst_volname=None, dst_grp_name=None,
                 dst_sv_name=None, dst_path=None):
        self.volname = volname

        self.src_grp_name = src_grp_name
        self.src_sv_name = src_sv_name
        self.src_path = src_path

        self.dst_grp_name = dst_grp_name
        self.dst_sv_name = dst_sv_name
        self.dst_path = dst_path


class CloneProgressReporter:

    def __init__(self, vc, vol_spec):
        self.vol_spec = vol_spec

        # instance of VolumeClient is needed here so that call to
        # LibCephFS.getxattr() can be made.
        self.vc = vc

        # Creating an RTimer instance in advance so that we can check if clon
        # reporting has already been already initiated by calling
        # RTimer.is_alive().
        self.update_task = RTimer(1, self._update_progress_bars)

    def initiate_reporting(self):
        if self.update_task.is_alive():
            return

        # progress event ID for ongoing clone jobs
        self.on_pev_id: Optional[str] = str(uuid4())
        # progress event ID for ongoing+pending clone jobs
        self.onpen_pev_id: Optional[str] = str(uuid4())
        self.show_onpen_bar = False

        self.update_task = RTimer(1, self._update_progress_bars)
        self.update_task.start()

    def _get_clone_dst_info(self, fsh, ci, clone_entry, clone_index_path):
        ce_path = os_path_join(clone_index_path, clone_entry)
        # XXX: This may raise ObjectNotFound exception. As soon as cloning is
        # finished, clone entry is deleted by cloner thread. This exception is
        # handled in _get_info_for_all_clones().
        ci.dst_path = fsh.readlink(ce_path, 4096).decode('utf-8')

        ci.dst_grp_name, ci.dst_sv_name = \
            resolve_grp_and_sv_names(self.vol_spec, ci.dst_path)

    def _get_clone_src_info(self, fsh, ci):
        with open_clone_sv_pair_in_vol(
                self.vc, self.vol_spec, ci.volname, ci.dst_grp_name,
                ci.dst_sv_name) as (dst_sv, src_sv, snap_name):
            ci.src_grp_name = src_sv.group_name
            ci.src_sv_name = src_sv.subvolname
            ci.src_path = src_sv.snapshot_data_path(snap_name)

    def _get_info_for_all_clones(self):
        clones = []

        volnames = list_volumes(self.vc.mgr)
        for volname in volnames:
            with open_volume_lockless(self.vc, volname) as fsh:
                with open_clone_index(fsh, self.vol_spec) as clone_index:
                    clone_index_path = clone_index.path
                    clone_entries = clone_index.list_entries_by_ctime_order()

            for ce in clone_entries:
                ci = CloneInfo()
                ci.volname = volname

                try:
                    self._get_clone_dst_info(fsh, ci, ce, clone_index_path)
                    self._get_clone_src_info(fsh, ci)
                    if ci.src_path is None or ci.dst_path is None:
                        continue
                # clone entry went missing, it was removed because cloning has
                # finished.
                except ObjectNotFound:
                    continue

                clones.append(ci)

        return clones if clones else []

    def _update_progress_bar_event(self, ev_id, ev_msg, ev_progress_fraction):
        # in case this remote call on RTimer fails, its details won't printed,
        # logged or would cause a crash. therefore, leaving some info for
        # debugging in logs
        log.info(f'ev_id = {ev_id} ev_progress_fraction = {ev_progress_fraction}')
        log.info(f'ev_msg = {ev_msg}')

        self.vc.mgr.remote('progress', 'update', ev_id=ev_id, ev_msg=ev_msg,
                           ev_progress=ev_progress_fraction,
                           refs=['mds', 'clone'], add_to_ceph_s=True)

        log.info('update() of mgr/progress executed successfully')

    def _update_onpen_progress_bar(self, clones):
        '''
        Update the progress bar for ongoing + pending cloning operations.

        Returns True when all cloning operations have completed.
        '''
        assert self.onpen_pev_id is not None

        # onpen bar (that is progress bar for clone jobs in ongoing and pending
        # state) is printed when clones are in pending state. it is kept in
        # printing until all clone jobs finish.
        if len(clones) > 4:
            self.show_onpen_bar = True
        if not self.show_onpen_bar:
            return

        total_clones = len(clones)
        percent = 0.0
        sum_percent = 0.0
        avg_percent = 0.0

        for clone in clones:
            with open_volume_lockless(self.vc, clone.volname) as fsh:
                percent = get_percent_copied(clone.src_path, clone.dst_path,
                                             fsh)
            if percent <= 100:
                sum_percent += percent
            elif percent > 100:
                log.info('ERROR: percent of progress made by a clone job is '
                         'more than 100%.')

        avg_percent = round(sum_percent / total_clones, 3)
        # progress module takes progress as a fraction between 0.0 to 1.0.
        avg_progress_fraction = avg_percent / 100
        msg = (f'{total_clones} ongoing+pending clones; Avg progress is '
               f'{avg_percent}%')
        self._update_progress_bar_event(ev_id=self.onpen_pev_id, ev_msg=msg,
                                    ev_progress_fraction=avg_progress_fraction)

    def _update_ongoing_progress_bar(self, clones):
        '''
        Update the progress bar for ongoing cloning operations.
        '''
        assert self.on_pev_id is not None

        percent = 0.0
        sum_percent = 0.0
        avg_percent = 0.0

        for clone in clones[:4]:
            with open_volume_lockless(self.vc, clone.volname) as fsh:
                percent = get_percent_copied(clone.src_path, clone.dst_path,
                                             fsh)
            if percent < 100:
                sum_percent += percent
            elif percent > 100:
                log.info('ERROR: percent of progress made by a clone job is '
                         'more than 100%.')

        total_clones = len(clones) if len(clones) < 4 else 4
        avg_percent = round(sum_percent / total_clones, 3)
        # progress module takes progress as a fraction between 0.0 to 1.0.
        avg_progress_fraction = avg_percent / 100
        msg = f'{total_clones} ongoing clones; Avg progress is {avg_percent}%'
        self._update_progress_bar_event(ev_id=self.on_pev_id, ev_msg=msg,
                                    ev_progress_fraction=avg_progress_fraction)

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
            self._wait_and_finish()
            return

        self._update_ongoing_progress_bar(clones)
        self._update_onpen_progress_bar(clones)

    def _set_waiting_period_msg(self, time_left):
        avg_progress_fraction = 1.0

        msg_on = (f'All cloning finished, waiting {time_left} seconds before '
                'exiting')
        self._update_progress_bar_event(
            ev_id=self.on_pev_id, ev_msg=msg_on,
            ev_progress_fraction=avg_progress_fraction)

        if self.show_onpen_bar:
            msg_onpen = (f'No pending clones left, waiting {time_left} '
                          'seconds before exiting')
            self._update_progress_bar_event(
                ev_id=self.onpen_pev_id, ev_msg=msg_onpen,
                ev_progress_fraction=avg_progress_fraction)

    def _wait_and_finish(self):
        '''
        All cloning has been finished. Wait for some time before terminating
        the/this thread updating and deleting the objects for progress bar
        events. In case new clones are launched, all these resources can be
        reused.
        '''
        log.info('waiting for new clones before terminating this thread.')

        interval = 1
        wait_limit = 30
        start_time = time()
        cur_time = start_time
        time_elapsed = cur_time - start_time
        time_left = wait_limit - time_elapsed

        self._set_waiting_period_msg(time_left)

        while time_elapsed < wait_limit:
            clones = self._get_info_for_all_clones()
            if len(clones):
                return

            sleep(interval)
            cur_time += interval
            time_elapsed = cur_time - start_time
            time_left = wait_limit - time_elapsed

            self._set_waiting_period_msg(time_left)

        log.info('no new clones were launched during waiting period, '
                 'terminating this thread and related resources now.')
        self._finish()

    def _finish(self):
        '''
        All cloning has been finished and enough waiting has been done for new
        clones to appear. Remove progress bars from "ceph -s" output.
        '''
        assert self.on_pev_id is not None
        assert self.onpen_pev_id is not None

        self.vc.mgr.remote('progress', 'complete', self.on_pev_id)
        self.on_pev_id = None

        self.vc.mgr.remote('progress', 'complete', self.onpen_pev_id)
        self.onpen_pev_id = None
        self.show_onpen_bar = False

        self.update_task.finished.set()
