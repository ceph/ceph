from __future__ import absolute_import

import os
if 'UNITTEST' in os.environ:
    import tests

from .module import Module
