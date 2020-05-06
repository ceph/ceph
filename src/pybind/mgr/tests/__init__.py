# type: ignore
from __future__ import absolute_import


import os

if 'UNITTEST' in os.environ:

    # Mock ceph_module. Otherwise every module that is involved in a testcase and imports it will
    # raise an ImportError

    import sys

    try:
        from unittest import mock
    except ImportError:
        import mock

    M_classes = set()

    class M(object):
        def _ceph_get_store(self, k):
            return self._store.get(k, None)

        def _ceph_set_store(self, k, v):
            if v is None:
                if k in self._store:
                    del self._store[k]
            else:
                self._store[k] = v

        def _ceph_get_store_prefix(self, prefix):
            return {
                k: v for k, v in self._store.items()
                if k.startswith(prefix)
            }

        def _ceph_get_module_option(self, module, key, localized_prefix: None):
            return self._ceph_get_store(f'{module}/{key}')

        def _ceph_set_module_option(self, module, key, val):
            return self._ceph_set_store(f'{module}/{key}', val)

        def __init__(self, *args):
            self._store = {}

            if self.__class__.__name__ not in M_classes:
                # call those only once. 
                self._register_commands('')
                self._register_options('')
                M_classes.add(self.__class__.__name__)

            super(M, self).__init__()
            self._ceph_get_version = mock.Mock()
            self._ceph_get = mock.MagicMock()
            self._ceph_get_option = mock.MagicMock()
            self._configure_logging = lambda *_: None
            self._unconfigure_logging = mock.MagicMock()
            self._ceph_log = mock.MagicMock()
            self._ceph_dispatch_remote = lambda *_: None


    cm = mock.Mock()
    cm.BaseMgrModule = M
    cm.BaseMgrStandbyModule = M
    sys.modules['ceph_module'] = cm
