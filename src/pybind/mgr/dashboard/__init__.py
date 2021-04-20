# -*- coding: utf-8 -*-
# pylint: disable=wrong-import-position,global-statement,protected-access
"""
ceph dashboard module
"""
from __future__ import absolute_import

import os


if 'UNITTEST' not in os.environ:
    class _LoggerProxy(object):
        def __init__(self):
            self._logger = None

        def __getattr__(self, item):
            if self._logger is None:
                raise AttributeError("logger not initialized")
            return getattr(self._logger, item)

    class _ModuleProxy(object):
        def __init__(self):
            self._mgr = None

        def init(self, module_inst):
            global logger
            self._mgr = module_inst
            logger._logger = self._mgr._logger

        def __getattr__(self, item):
            if self._mgr is None:
                raise AttributeError("global manager module instance not initialized")
            return getattr(self._mgr, item)

    mgr = _ModuleProxy()
    logger = _LoggerProxy()

    from .module import Module, StandbyModule
else:
    import logging
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    logging.root.handlers[0].setLevel(logging.DEBUG)
    os.environ['PATH'] = '{}:{}'.format(os.path.abspath('../../../../build/bin'),
                                        os.environ['PATH'])

    # Mock ceph module otherwise every module that is involved in a testcase and imports it will
    # raise an ImportError
    import sys
    import mock

    mgr = mock.Mock()
    mgr.get_frontend_path.side_effect = lambda: os.path.abspath("./frontend/dist")

    def mock_ceph_modules():
        class MockRadosError(Exception):
            def __init__(self, message, errno=None):
                super(MockRadosError, self).__init__(message)
                self.errno = errno

            def __str__(self):
                msg = super(MockRadosError, self).__str__()
                if self.errno is None:
                    return msg
                return '[errno {0}] {1}'.format(self.errno, msg)

        rbd = mock.Mock()
        rbd.RBD_FEATURE_LAYERING = 1
        rbd.RBD_FEATURE_STRIPINGV2 = 2
        rbd.RBD_FEATURE_EXCLUSIVE_LOCK = 4
        rbd.RBD_FEATURE_OBJECT_MAP = 8
        rbd.RBD_FEATURE_FAST_DIFF = 16
        rbd.RBD_FEATURE_DEEP_FLATTEN = 32
        rbd.RBD_FEATURE_JOURNALING = 64
        rbd.RBD_FEATURE_DATA_POOL = 128
        rbd.RBD_FEATURE_OPERATIONS = 256

        sys.modules.update({
            'rados': mock.Mock(Error=MockRadosError, OSError=MockRadosError),
            'rbd': rbd,
            'cephfs': mock.Mock(),
            'ceph_module': mock.Mock(),
        })

    mock_ceph_modules()
