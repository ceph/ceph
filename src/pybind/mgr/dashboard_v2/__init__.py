# -*- coding: utf-8 -*-
"""
openATTIC module
"""
from __future__ import absolute_import

import os

if 'UNITTEST' not in os.environ:

    class _LoggerProxy(object):
        def __init__(self):
            self.logger = None

        def __getattr__(self, item):
            if self.logger is None:
                raise AttributeError("Logging not initialized")
            return getattr(self.logger, item)


    logger = _LoggerProxy()

    # pylint: disable=W0403,W0401

    from .module import *  # NOQA
else:
    import logging
    import sys
    # pylint: disable=W0403
    from . import ceph_module_mock
    sys.modules['ceph_module'] = ceph_module_mock
    logging.basicConfig(level=logging.WARNING)
    logger = logging.getLogger(__name__)
    logging.root.handlers[0].setLevel(logging.WARNING)
