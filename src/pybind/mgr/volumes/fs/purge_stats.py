'''
This module contains code for reporting progress statistics of async purge jobs.
'''
from logging import getLogger

from .stats_util import AsyncJobProgressReporter
from .operations.volume import list_volumes, open_volume_lockless


log = getLogger(__name__)


class PurgeProgressReporter(AsyncJobProgressReporter):
    '''
    Report progress made by purge threads.
    '''

    def __init__(self, volclient, volspec):
        super().__init__(volclient, volspec)

        # TODO: num of concurrent purges should be constant that should be
        # imported and used here instead of setting it to 4 here.
        self.max_concurrent_purges = 4

        self.op_name = 'purge'

    def initiate_reporting(self):
        super().initiate_reporting()

        subvol_count, file_count = self._get_trash_info()

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
        trash_path = '/volumes/_deleting/'

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
        log.debug('collected stats of purge first time')
        if not file_count:
            self.finish()
            return

        self.show_onpen_bar = True if subvol_count > self.max_concurrent_purges else False

        log.debug('collected stats of purge second time')
        diff = self.init_file_count - file_count
        # progress fraction that progress module accepts to print the progress bar.
        fraction = round(diff / self.init_file_count, 3)
        percent = round(fraction * 100, 3)
        msg = f'Purging {self.init_subvol_count} subvolumes/{self.init_file_count} files, average progress = {percent}%'
        self._update_progress_bar_event(self.onpen_pev_id, msg, fraction)
        log.debug('finished updating progress bar for purges with message "{msg}"')
