import errno
import os

from ..exception import VolumeException
from .template import GroupTemplate

class Index(GroupTemplate):
    GROUP_NAME = "_index"

    def __init__(self, fs, vol_spec):
        self.fs = fs
        self.vol_spec = vol_spec
        self.groupname = Index.GROUP_NAME

    @property
    def path(self):
        return os.path.join(self.vol_spec.base_dir.encode('utf-8'), self.groupname.encode('utf-8'))

    def track(self, *args):
        raise VolumeException(-errno.EINVAL, "operation not supported.")

    def untrack(self, tracking_id):
        raise VolumeException(-errno.EINVAL, "operation not supported.")
