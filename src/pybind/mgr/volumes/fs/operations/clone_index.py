import os
import uuid
import stat
import errno
import logging
from contextlib import contextmanager

import cephfs

from .index import Index
from ..exception import IndexException, VolumeException

log = logging.getLogger(__name__)

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
            elif isinstance(VolumeException, e):
                e = IndexException(e.errno, e.error_str)
            raise e

    def untrack(self, tracking_id):
        log.info("untracking {0}".format(tracking_id))
        source_path = os.path.join(self.path, tracking_id.encode('utf-8'))
        try:
            self.fs.unlink(source_path)
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
