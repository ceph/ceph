import errno
import json

from tests import mock  # noqa: F401 -- sets up ceph module mocks

import rados


class FakeIoctx:
    """omap of the cephfs_mirror object; each write op is applied atomically"""
    def __init__(self, omap=None):
        self.omap = dict(omap or {})
        self.ops = []        # (keys set, keys removed) per write op
        self.reads = 0
        self.notifies = []
        self.fail_write = False
        self._sets, self._rms = [], []

    def get_omap_vals(self, read_op, start, prefix, max_return):
        items = [(k, v) for k, v in sorted(self.omap.items())
                 if k.startswith(prefix) and k > start]
        return iter(items[:max_return]), 0

    def operate_read_op(self, read_op, oid):
        self.reads += 1

    def set_omap(self, write_op, keys, vals):
        self._sets.append(dict(zip(keys, vals)))

    def remove_omap_keys(self, write_op, keys):
        self._rms.extend(keys)

    def _apply(self):
        sets, rms = self._sets, self._rms
        self._sets, self._rms = [], []
        if self.fail_write:
            raise rados.Error('write failed', errno=errno.EIO)
        keys = set()
        for kv in sets:
            self.omap.update(kv)
            keys |= set(kv)
        for k in rms:
            self.omap.pop(k, None)
        self.ops.append((sorted(keys), sorted(rms)))

    def operate_write_op(self, write_op, oid):
        self._apply()

    def operate_aio_write_op(self, write_op, oid, oncomplete):
        self._apply()
        oncomplete(mock.Mock(get_return_value=lambda: 0))

    def aio_notify(self, oid, cbk, msg=''):
        self.notifies.append((oid, json.loads(msg)))
