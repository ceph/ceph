import os
import uuid
import stat
import logging
from contextlib import contextmanager

import cephfs

from .index import Index
from ..exception import IndexException, VolumeException
from ..fs_util import list_one_entry_at_a_time, listdir

log = logging.getLogger(__name__)


PATH_MAX = 4096


class CloneIndex(Index):
    SUB_GROUP_NAME = "clone"

    @property
    def path(self):
        return os.path.join(super(CloneIndex, self).path, CloneIndex.SUB_GROUP_NAME.encode('utf-8'))

    def _track(self, sink_path):
        tracking_id = str(uuid.uuid4())
        source_path = os.path.join(self.path, tracking_id.encode('utf-8'))
        log.info("tracking-id {0} for path {1}".format(tracking_id, sink_path))

        self.fs.symlink(sink_path, source_path)
        return tracking_id

    def track(self, sink_path):
        try:
            return self._track(sink_path)
        except (VolumeException, cephfs.Error) as e:
            if isinstance(e, cephfs.Error):
                e = IndexException(-e.args[0], e.args[1])
            elif isinstance(e, VolumeException):
                e = IndexException(e.errno, e.error_str)
            raise e

    def untrack(self, tracking_id):
        log.info("untracking {0}".format(tracking_id))
        source_path = os.path.join(self.path, tracking_id.encode('utf-8'))
        try:
            self.fs.unlink(source_path)
        except cephfs.Error as e:
            raise IndexException(-e.args[0], e.args[1])

    def list_entries_by_ctime_order(self):
        entry_names = listdir(self.fs, self.path, filter_files=False)
        if not entry_names:
            return []

        # clone entries with ctime obtained by statig them. basically,
        # following is a list of tuples where each tuple has 2 memebers.
        ens_with_ctime = []
        for en in entry_names:
            d_path = os.path.join(self.path, en)
            try:
                stb = self.fs.lstat(d_path)
            except cephfs.ObjectNotFound:
                log.debug(f'path {d_path} went missing, perhaps clone job was '
                          'finished')
                continue

            # add ctime next to clone entry
            ens_with_ctime.append((en, stb.st_ctime))

        ens_with_ctime.sort(key=lambda ctime: en[1])

        # remove ctime and return list of clone entries sorted by ctime.
        return [i[0] for i in ens_with_ctime]

    def get_oldest_clone_entry(self, exclude=[]):
        try:
            min_ctime_entry = None
            exclude_tracking_ids = [v[0] for v in exclude]
            log.debug("excluded tracking ids: {0}".format(exclude_tracking_ids))
            for entry in list_one_entry_at_a_time(self.fs, self.path):
                dname = entry.d_name
                dpath = os.path.join(self.path, dname)
                st = self.fs.lstat(dpath)
                if dname not in exclude_tracking_ids and stat.S_ISLNK(st.st_mode):
                    if min_ctime_entry is None or st.st_ctime < min_ctime_entry[1].st_ctime:
                        min_ctime_entry = (dname, st)
            if min_ctime_entry:
                linklen = min_ctime_entry[1].st_size
                sink_path = self.fs.readlink(os.path.join(self.path, min_ctime_entry[0]), PATH_MAX)
                return (min_ctime_entry[0], sink_path[:linklen])
            return None
        except cephfs.Error as e:
            log.debug('Exception cephfs.Error has been caught. Printing '
                      f'the exception - {e}')
            raise IndexException(-e.args[0], e.args[1])

    def find_clone_entry_index(self, sink_path):
        try:
            for entry in list_one_entry_at_a_time(self.fs, self.path):
                dname = entry.d_name
                dpath = os.path.join(self.path, dname)
                st = self.fs.lstat(dpath)
                if stat.S_ISLNK(st.st_mode):
                    target_path = self.fs.readlink(dpath, PATH_MAX)
                    if sink_path == target_path[:st.st_size]:
                        return dname
            return None
        except cephfs.Error as e:
            raise IndexException(-e.args[0], e.args[1])


def create_clone_index(fs, vol_spec):
    clone_index = CloneIndex(fs, vol_spec)
    try:
        fs.mkdirs(clone_index.path, 0o700)
    except cephfs.Error as e:
        raise IndexException(-e.args[0], e.args[1])


@contextmanager
def open_clone_index(fs, vol_spec):
    clone_index = CloneIndex(fs, vol_spec)
    try:
        fs.stat(clone_index.path)
    except cephfs.Error as e:
        raise IndexException(-e.args[0], e.args[1])
    yield clone_index
