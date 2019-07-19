import sys

try:
    from mock import Mock
except ImportError:
    from unittest.mock import Mock


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
