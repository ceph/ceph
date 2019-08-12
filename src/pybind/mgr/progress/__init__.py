import os
if 'UNITTEST' not in os.environ:
    from .module import *
else:
    import sys
    import mock
    sys.modules['ceph_module'] = mock.Mock()
    sys.modules['rados'] = mock.Mock()
    from .module import *
   

