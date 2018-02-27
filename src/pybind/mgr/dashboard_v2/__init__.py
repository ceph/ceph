# -*- coding: utf-8 -*-

import os

if 'UNITTEST' not in os.environ:
    from module import *  # NOQA
else:
    import sys
    import ceph_module_mock
    sys.modules['ceph_module'] = ceph_module_mock


