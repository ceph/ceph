# -*- coding: utf-8 -*-
# pylint: disable=wrong-import-position,global-statement,protected-access
"""
ceph dashboard module
"""
from __future__ import absolute_import

import os


if 'UNITTEST' not in os.environ:
    class _ModuleProxy(object):
        def __init__(self):
            self._mgr = None

        def init(self, module_inst):
            self._mgr = module_inst

        def __getattr__(self, item):
            if self._mgr is None:
                raise AttributeError("global manager module instance not initialized")
            return getattr(self._mgr, item)

    mgr = _ModuleProxy()

else:
    import logging
    logging.basicConfig(level=logging.DEBUG)
    logging.root.handlers[0].setLevel(logging.DEBUG)
    os.environ['PATH'] = '{}:{}'.format(os.path.abspath('../../../../build/bin'),
                                        os.environ['PATH'])

    from tests import mock, mock_ceph_modules  # type: ignore

    mgr = mock.Mock()
    mgr.get_frontend_path.side_effect = lambda: os.path.abspath("./frontend/dist")

    mock_ceph_modules()

# DO NOT REMOVE: required for ceph-mgr to load a module
from .module import Module, StandbyModule  # noqa: F401
