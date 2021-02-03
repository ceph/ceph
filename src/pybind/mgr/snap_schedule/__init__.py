# -*- coding: utf-8 -*-

from os import environ

if 'SNAP_SCHED_UNITTEST' in environ:
    import tests
elif 'UNITTEST' in environ:
    import tests
    from .module import Module
else:
    from .module import Module
