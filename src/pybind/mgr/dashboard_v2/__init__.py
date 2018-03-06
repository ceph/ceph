# -*- coding: utf-8 -*-
# pylint: disable=wrong-import-position,global-statement,protected-access
"""
openATTIC module
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
    sys.modules['ceph_module'] = mock.Mock()

    mgr = mock.Mock()
