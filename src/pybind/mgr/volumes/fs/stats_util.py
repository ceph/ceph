'''
Contains helpers that are used to get statistics (specifically number of
regular files and directories and total size of data present under a given
directory) and pass, print, log and convert them to human readable format
conveniently.
'''
import errno
from logging import getLogger
from .exception import VolumeException


log = getLogger(__name__)


def get_size_h(num):
    '''
    Convert size to human-readable format.
    '''
    size = ('B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB')

    if isinstance(num, bytes):
        num = int(num)

    if isinstance(num, str):
        if not num.isdigit():
            return num
        num = int(num)

    if num < 1024:
        return f'{num} {size[0]}'

    i = 0
    while True:
        num = num / 1024
        i += 1
        if i >= len(size):
            log.error(f"Biggest suffix we have is '{len(size)-1}' "
                       'but this is not sufficient in this case. '
                      f'Current size = {num}')

        x, y = str(num).split('.')
        if len(x) <= 3:
            if y:
                y = '' if y == '0' else y[:4]

            return f'{x}.{y} {size[i]}' if y else f'{x} {size[i]}'


def get_num_h(num):
    '''
    Convert number to human-readable format.
    '''
    size = ('', 'K', 'M', 'B', 'T')

    if isinstance(num, bytes):
        num = int(num)

    if isinstance(num, str):
        if not num.isdigit():
            return num
        num = int(num)

    if num < 1000:
        return f'{num}'

    i = 0
    while True:
        num = num / 1000
        i += 1
        if i >= len(size):
            log.error(f"Biggest suffix we have is '{len(size)-1}' "
                       'but this is not sufficient in this case. '
                      f'Current size = {num}')

        x, y = str(num).split('.')
        if len(x) <= 3:
            if y:
                y = '' if y == '0' else y[:4]

            return f'{x}.{y} {size[i]}' if y else f'{x} {size[i]}'


class Stats:
    '''
    Store statistics of the given path that'll be copied/deleted/purged (or
    handled, in short) in an organized manner.
    '''
    def __init__(self, given_path, op, fsh, should_cancel):
        # path for which this class instance will contain statistics
        self.given_path = given_path

        self.size_t = 0     # total size
        self.rfiles_t = 0   # total regfiles
        self.subdirs_t = 0  # total subdirectories
        self.slinks_t = 0   # total symlinks

        self.size_c = 0     # size copied
        self.rfiles_c = 0   # num of regfiles copied
        self.subdirs_c = 0  # num of dirs copied
        self.slinks_c = 0   # num of symlinks copied

        self.op = op
        # creating self.oped so that we can pring log messages accordingly.
        self.oped_str = self._get_operated_str() # oped = operated

        self.get_statistics_for_path(fsh, should_cancel)

    def get_statistics_for_path(self, fsh, should_cancel):
        '''
        Get statistics like of total number of files and subdirs of under
        "given_path" and total size of data underneath "given_path".
        '''
        self.rfiles_t = int(fsh.getxattr(self.given_path, 'ceph.dir.rfiles'))
        self.subdirs_t = int(fsh.getxattr(self.given_path,'ceph.dir.rsubdirs'))
        self.size_t = int(fsh.getxattr(self.given_path, 'ceph.dir.rbytes'))

        self.log_total_amount()

        if should_cancel():
            raise VolumeException(-errno.EINTR,
                                  "user interrupted clone operation")

    @property
    def percent(self):
        if self.size_t == 0:
            return 100 if self.size_c == 0 else 0
        return ((self.size_c/self.size_t) * 100)

    @property
    def progress_fraction(self):
        '''
        For progress MGR module (mgr/progress), value of attribute "progress"
        should be in range of 0.0 to 1.0. 0.0 represents 0% progress and 1.0
        represents 100% progress.
        '''
        return round(self.percent, 2)/100

    def _get_operated_str(self):
        '''
        Return past tense form of the operation for which this instance is
        being used.
        '''
        if self.op == 'copy':
            return 'copied'
        elif self.op == 'purge':
            return 'purged'
        else:
            log.critical('Class Stats is being used for an operation it '
                         'doesn\'t recognize')

    def get_size_stat(self, human=True):
        if human:
            x, y = get_size_h(self.size_c), get_size_h(self.size_t)
        else:
            x, y = self.size_c, self.size_t

        return f'{x}/{y}'

    def get_rfiles_stat(self, human=True):
        if human:
            x, y = get_num_h(self.rfiles_c), get_num_h(self.rfiles_t)
        else:
            x, y = self.rfiles_c, self.rfiles_t

        return f'{x}/{y}'

    def get_subdirs_stat(self, human=True):
        if human:
            x, y = get_num_h(self.subdirs_c), get_num_h(self.subdirs_t)
        else:
            x, y = self.subdirs_c, self.subdirs_t

        return f'{x}/{y}'

    def get_slinks_stat(self, human=True):
        if human:
            x, y = get_num_h(self.slinks_c), get_num_h(self.slinks_t)
        else:
            x, y = self.slinks_c, self.slinks_t

        return f'{x}/{y}'

    def get_progress_report(self, human=True):
        return {
            'percentage completed': self.percent,
            f'amount {self.oped_str}': self.get_size_stat(human=human),
            f'regfiles {self.oped_str}': self.get_rfiles_stat(human=human),
            f'subdirs {self.oped_str}': self.get_subdirs_stat(human=human),
            f'symlinks {self.oped_str}': self.get_slinks_stat(human=human)
        }

    def get_progress_report_str(self):
        progress_report_str = ''
        for k, v in self.get_progress_report().items():
            progress_report_str += f'{k} = {v}\n'

        # remove extra new line character at end, since print and log.xxx()
        # methods automatically add a newline at the end.
        return progress_report_str[:-1]

    def log_handled_amount(self):
        log.info('Following are statistics for amount handled vs. total '
                 'amount -')
        for k, v in self.get_progress_report().items():
            log.info(f'{k} = {v}')

    def log_total_amount(self):
        size_t = get_size_h(self.size_t)
        rfiles_t = get_num_h(self.rfiles_t)
        subdirs_t = get_num_h(self.subdirs_t)
        slinks_t = get_num_h(self.slinks_t)
        log.info('Following are statistics for source path -')
        log.info(f'    total size = {size_t}')
        log.info(f'    total number of regular files = {rfiles_t}')
        log.info(f'    total number of subdirectories = {subdirs_t}')
        log.info(f'    total number of symbolic links = {slinks_t}')
