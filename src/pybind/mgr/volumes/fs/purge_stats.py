'''
This module contains code for reporting progress statistics of async purge jobs.
'''
from os.path import join as os_path_join
from logging import getLogger

from .stats_util import AsyncJobProgressReporter
from .fs_util import listdir
from .operations.volume import list_volumes, open_trashcan_in_vol

from cephfs import ObjectNotFound


log = getLogger(__name__)


class PurgeProgressReporter(AsyncJobProgressReporter):
    '''
    Report progress made by purge threads.
    '''

    def __init__(self, volclient, volspec):
        super().__init__(volclient, volspec)

        # need to figure out how many progress bars should be printed. print 1
        # progress bar if number of ongoing clones is less than this value,
        # else print 2.
        # TODO: num of concurrent purges should be constant that should be
        # imported and used here instea of setting it to 4 here.
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

    def _get_trash_info(self):
        file_count = 0
        subvol_count = 0

        volnames = list_volumes(self.volclient.mgr)
        for volname in volnames:
            with open_trashcan_in_vol(self.volclient, volname,
                                      self.volspec) as (fs_handle, trashcan):
                # t_entries = trash entries
                t_entries = trashcan.get_trash_entries_by_ctime_order()
                log.debug(f'trash entries found in trashcan of volume {volname} are -\n{t_entries}')
                subvol_count += len(t_entries)

                for t_entry in t_entries:
                    te_path = os_path_join(trashcan.path, t_entry)
                    try:
                        te_dirs = listdir(fs_handle, te_path)
                    except ObjectNotFound as e:
                        log.debug(f'path {te_path} went missing, perhaps it was '
                                  'purged. Exception ObjectNotFound was raised '
                                  f'for it. Excetion - {e}')
                        continue
                    except Exception as e:
                        log.debug(f'Caught exception - {e}')
                        continue

                    # each trash entry should contain only one dir, the UUID
                    # dir of subvolume.
                    try:
                        assert len(te_dirs) == 1
                    except AssertionError:
                        log.info(f'expected 1 dir in trash UUID dir but found '
                                 f'{len(te_dirs)} instead. dirs -\n{te_dirs}')
                        continue

                    sv_dir = te_dirs[0]
                    t_sv_path = os_path_join(te_path, sv_dir)

                    try:
                        file_count += len(listdir(fs_handle, t_sv_path,
                                                  filter_files=False))
                    except ObjectNotFound as e:
                        log.debug(f'path {t_sv_path} went missing, perhaps it was '
                                  'purged. Exception ObjectNotFound was raised '
                                  f'for it. Excetion - {e}')
                    except Exception as e:
                        log.debug(f'Caught exception - {e}')
                        continue

        return subvol_count, file_count

    def _update_progress_bars(self):
        subvol_count, file_count = self._get_trash_info()
        log.debug('collected stats of purge first time')
        if not file_count:
            self.finish()
            return

        self.show_onpen_bar = True if subvol_count > 4 else False

        log.debug('collected stats of purge second time')
        diff = self.init_file_count - file_count
        # progress fraction that progress module accepts to print the progress bar.
        fraction = round(diff / self.init_file_count, 3)
        percent = fraction * 100
        msg = f'Purging {self.init_subvol_count} subvolumes/{self.init_file_count} files, average progress = {percent}%'
        self._update_progress_bar_event(self.onpen_pev_id, msg, fraction)
        log.debug('finished upd ating progress bar for purges with message "{msg}"')
