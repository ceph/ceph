'''
Contains helpers that are used to get statistics (specifically number of
regular files and directories and total size of data present under a given
directory) and pass, print, log and convert them to human readable format
conveniently.
'''
from logging import getLogger
from uuid import uuid4

from .operations.volume import open_volume_lockless

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

    if size_t == 0:
        return -1 if size_c == 0 else -2

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

    def __init__(self, volname, dst_sv_name, src_path, dst_path):
        self.volname = volname
        self.dst_sv_name = dst_sv_name

        self.src_path = src_path
        self.dst_path = dst_path


class CloneProgressReporter:

    def __init__(self, vc):
        self.vc = vc
        self.clones = []
        self.pev_id = str(uuid4())

        self.update_task = RTimer(1, self.update)

    def add_clone(self, volname, dst_sv_name, src_path, dst_path):
        self.clones.append(CloneInfo(volname, dst_sv_name, src_path, dst_path))

        if not self.update_task.is_alive() and len(self.clones) > 0:
            self.update_task.start()

    def update(self):
        assert self.pev_id is not None
        total_clones = len(self.clones)
        assert total_clones > 0
        percent = 0.0

        for clone in self.clones:
            with open_volume_lockless(self.vc, clone.volname) as fsh:
                percent += get_percent_copied(clone.src_path, clone.dst_path,
                                              fsh)

        percent = round(percent / total_clones, 3)
        # progress module takes progress as a fraction between 0.0 to 1.0.
        progress_fraction = percent / 100
        msg = f'Avg progress made by {total_clones} clones is {percent}%'

        self.vc.mgr.remote('progress', 'update', ev_id=self.pev_id,
                           ev_msg=msg, ev_progress=progress_fraction,
                           refs=['mds', 'clone'], add_to_ceph_s=True)

        if progress_fraction >= 1.0:
            if progress_fraction > 1.0:
                log.error('avg percentage of cloning completed is more than '
                          '100%')
            self.finish()

    def finish(self):
        assert self.pev_id is not None

        self.vc.mgr.remote('progress', 'complete', self.pev_id)
        self.pev_id = None
        self.clones = []
        self.update_task.cancel()

    def abort(self):
        assert self.pev_id is not None

        msg = f'Cloning failed for {self.dst_sv_name}'
        self.vc.mgr.remote('progress', 'fail', self.pev_id, msg)
        self.pev_id = None
