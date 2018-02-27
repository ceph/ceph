# -*- coding: utf-8 -*-
"""
openATTIC module
"""

import os

if 'UNITTEST' not in os.environ:
    # pylint: disable=W0403,W0401
    from module import *  # NOQA
else:
    import sys
    # pylint: disable=W0403
    import ceph_module_mock
    sys.modules['ceph_module'] = ceph_module_mock
