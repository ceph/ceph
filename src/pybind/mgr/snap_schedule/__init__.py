# -*- coding: utf-8 -*-

import os

if 'CEPH_SNAP_SCHEDULE_UNITTEST' not in os.environ:
    from .module import Module
