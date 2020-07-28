import os

if 'UNITTEST' in os.environ:
    import tests
    tests.mock_ceph_modules()  # type: ignore

from .module import OSDSupport
