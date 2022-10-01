# -*- coding: utf-8 -*-
# pylint: disable=wrong-import-position,global-statement,protected-access
"""
ceph dashboard module
"""

import os

import cherrypy

if 'COVERAGE_ENABLED' in os.environ:
    import coverage  # pylint: disable=import-error
    __cov = coverage.Coverage(config_file="{}/.coveragerc".format(os.path.dirname(__file__)),
                              data_suffix=True)
    __cov.start()
    cherrypy.engine.subscribe('after_request', __cov.save)
    cherrypy.engine.subscribe('stop', __cov.stop)

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
    import sys

    # Used to allow the running of a tox-based yml doc generator from the dashboard directory
    if os.path.abspath(sys.path[0]) == os.getcwd():
        sys.path.pop(0)

    from tests import mock  # type: ignore

    mgr = mock.Mock()
    mgr.get_frontend_path.return_value = os.path.abspath(os.path.join(
        os.path.dirname(__file__),
        'frontend/dist'))

    import rbd

    # Api tests do not mock rbd as opposed to dashboard unit tests. Both
    # use UNITTEST env variable.
    if isinstance(rbd, mock.Mock):
        rbd.RBD_MIRROR_IMAGE_MODE_JOURNAL = 0
        rbd.RBD_MIRROR_IMAGE_MODE_SNAPSHOT = 1

# DO NOT REMOVE: required for ceph-mgr to load a module
from .module import Module, StandbyModule  # noqa: F401
