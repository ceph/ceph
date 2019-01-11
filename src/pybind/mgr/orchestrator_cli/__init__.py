from __future__ import absolute_import
import os

if 'UNITTEST' not in os.environ:
    from .module import OrchestratorCli
else:
    import sys
    import mock
    sys.path.append("..")
    sys.modules['ceph_module'] = mock.Mock()
