import sys

try:
    from mock import Mock, patch
except ImportError:
    from unittest.mock import Mock, patch


class MockRadosError(Exception):
    def __init__(self, message, errno=None):
        super(MockRadosError, self).__init__(message)
        self.errno = errno

    def __str__(self):
        msg = super(MockRadosError, self).__str__()
        if self.errno is None:
            return msg
        return '[errno {0}] {1}'.format(self.errno, msg)


def pytest_configure(config):
    sys.modules.update({
        'rados': Mock(Error=MockRadosError, OSError=MockRadosError),
        'rbd': Mock(),
        'cephfs': Mock(),
    })

    # we need the following patches to fix the issue of multiple inheritance when
    # one of the base classes is being mocked.
    # Error example:
    # TypeError: metaclass conflict: the metaclass of a derived class must be a (non-strict) \
    # subclass of the metaclasses of all its bases
    class _BaseMgrModule:
        pass

    patcher = patch("ceph_module.BaseMgrStandbyModule", new=_BaseMgrModule)
    patcher.start()
    patcher = patch("ceph_module.BaseMgrModule", new=_BaseMgrModule)
    patcher.start()
