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
    # pylint: disable=wildcard-import, wrong-import-position
    from .module import *  # NOQA
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
