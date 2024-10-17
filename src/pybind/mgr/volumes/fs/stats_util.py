'''
This module contains helper classes and helper functions for getting, processing
and reporting progress statistics for async jobs. The code in this module is
 helper code that doesn't depend on the type of async job.

These statistics can be reported in two ways. First, as a Python dictionary
which is eventually printed in output of a command. And second, as a progress
bar which is printed in output of "ceph status" command.
'''
from os.path import join as os_path_join
from typing import Optional
from logging import getLogger

from .operations.volume import open_volume_lockless, list_volumes
from .operations.subvolume import open_clone_subvol_pair_in_vol, open_subvol_in_vol
from .operations.template import SubvolumeOpType
from .operations.clone_index import open_clone_index, PATH_MAX
from .operations.resolver import resolve_group_and_subvolume_name
from .exception import VolumeException

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

    size_t = int(fs_handle.getxattr(src_path, rbytes))
    size_c = int(fs_handle.getxattr(dst_path, rbytes))

    percent: Optional[float]
    if size_t == 0 or size_c == 0:
        percent = 0
    else:
        percent = ((size_c/size_t) * 100)
        percent = round(percent, 3)

    return size_t, size_c, percent


def get_percent_copied(src_path, dst_path, fs_handle):
    _, _, percent = get_amount_copied(src_path, dst_path, fs_handle)
    return percent


# TODO: update the guidelines here that shows how to derive and use this class properly
class AsyncJobProgressReporter:
    '''
    This class contains code necessary for initiating and finishing reporting
    of async jobs. This includes spawning and terminating reporter thread and
    addition and removal of progress bars to "ceph status" output.

    =======HOW TO USE THIS CLASS=======
    1. Inherit this class and call its __init___().
    2. Set self.op_name to operation for which progress is to be reported. For
       example, for cloning and purging it should be 'clone' and 'purge'
       respectively.
    3. Write all code for updating progress bars under the method
       "_update_progress_bars()" directly or under a method called by this
       method.
    4. When the async job is finished, call self.finish().
    '''

    def __init__(self, volclient, volspec):
        self.volspec = volspec

        # instance of VolumeClient is needed here so that call to
        # LibCephFS.getxattr() can be made.
        self.volclient = volclient

        # Creating an RTimer instance in advance so that we can check if clone
        # reporting has already been initiated by calling RTimer.is_alive().
        self.update_task = RTimer(1, self._update_progress_bars) # type: ignore

        # self.op_name is to be set by derived classes. This is is need to
        # label progress bars, logging, etc.
        self.op_name = None

    def initiate_reporting(self):
        assert self.op_name
        assert isinstance(self.op_name, str)

        if self.update_task.is_alive():
            log.info('progress reporting thread is already alive, not '
                     'initiating it again')
            return

        log.info('initiating progress reporting for clones...')
        # progress event ID for ongoing clone jobs
        self.on_pev_id: Optional[str] = f'mgr-vol-ongoing-{self.op_name}'
        # progress event ID for ongoing+pending clone jobs
        self.onpen_pev_id: Optional[str] = f'mgr-vol-total-{self.op_name}'

        self.update_task = RTimer(1, self._update_progress_bars) # type: ignore
        self.update_task.start()
        log.info('progress reporting for clones has been initiated')

    def _update_progress_bar_event(self, ev_id, ev_msg, ev_progress_fraction):
        log.debug(f'ev_id = {ev_id} ev_progress_fraction = {ev_progress_fraction}')
        log.debug(f'ev_msg = {ev_msg}')
        log.debug('calling update() from mgr/update module')

        self.volclient.mgr.remote('progress', 'update', ev_id=ev_id,
                                  ev_msg=ev_msg,
                                  ev_progress=ev_progress_fraction,
                                  refs=['mds', 'clone'], add_to_ceph_s=True)

        log.debug('call to update() from mgr/update module was successful')

    def _finish_progress_events(self):
        '''
        Remove progress bars from "ceph status" output.
        '''
        log.info('removing progress bars from "ceph status" output')

        assert self.on_pev_id is not None
        assert self.onpen_pev_id is not None

        self.volclient.mgr.remote('progress', 'complete', self.on_pev_id)
        self.on_pev_id = None

        self.volclient.mgr.remote('progress', 'complete', self.onpen_pev_id)
        self.onpen_pev_id = None

        log.info('finished removing progress bars from "ceph status" output')

    def finish(self):
        '''
        All cloning jobs have been completed. Terminate this RTimer thread.
        '''
        self._finish_progress_events()

        log.info(f'marking this RTimer thread as finished; thread object ID - {self}')
        self.update_task.finished.set()
