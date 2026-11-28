import errno

from unittest import mock

import pytest

import rbd_support.schedule as rbd_schedule
import rbd_support.task as rbd_task
import rbd_support.trash_purge_schedule as rbd_trash_purge_schedule


# The unit test harness replaces the rados and rbd bindings with mocks. Their
# attributes are Mock objects rather than exception classes, so any "except
# rbd.X" clause raises TypeError as soon as an exception reaches it. Give the
# names the code under test catches or raises real exception classes.
class ConnectionShutdown(Exception):
    pass


class RBDError(Exception):
    def __init__(self, message='', errno=None):
        super().__init__(message)
        self.errno = errno


class ImageNotFound(RBDError):
    pass


class ImageBusy(RBDError):
    pass


class InvalidArgument(RBDError):
    pass


class OperationCanceled(RBDError):
    pass


class OperationNotSupported(RBDError):
    pass


@pytest.fixture(autouse=True)
def rbd_exceptions():
    rbd_attrs = dict(Error=RBDError,
                     ImageNotFound=ImageNotFound,
                     ImageBusy=ImageBusy,
                     InvalidArgument=InvalidArgument,
                     OperationCanceled=OperationCanceled,
                     OperationNotSupported=OperationNotSupported,
                     ConnectionShutdown=ConnectionShutdown,
                     ECANCELED=errno.ECANCELED,
                     RBD=mock.MagicMock(),
                     Image=mock.MagicMock())
    # all rbd_support submodules share the same mocked rados/rbd modules
    assert rbd_task.rbd is rbd_schedule.rbd is rbd_trash_purge_schedule.rbd
    with mock.patch.multiple(rbd_task.rbd, **rbd_attrs), \
            mock.patch.object(rbd_task.rados, 'ConnectionShutdown',
                              ConnectionShutdown):
        yield


@pytest.fixture
def module():
    m = mock.MagicMock()
    m.get_ceph_option.return_value = 'rbd'
    m.is_authorized.return_value = True
    m.get.return_value = {
        'pools': [
            {'pool': 1, 'pool_name': 'rbd',
             'application_metadata': {'rbd': {}}},
            {'pool': 2, 'pool_name': 'other',
             'application_metadata': {'rbd': {}}},
            {'pool': 3, 'pool_name': 'cephfs_data',
             'application_metadata': {'cephfs': {}}},
        ]
    }
    m.get_module_option.return_value = ''
    m.get_localized_module_option.return_value = ''
    return m
