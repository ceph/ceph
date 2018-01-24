# -*- coding: utf-8 -*-
"""
openATTIC module
"""
from __future__ import absolute_import

import os

if 'UNITTEST' not in os.environ:
    # pylint: disable=W0403,W0401
    from .module import *  # NOQA
else:
    import sys
    # pylint: disable=W0403
    from . import ceph_module_mock
    sys.modules['ceph_module'] = ceph_module_mock
