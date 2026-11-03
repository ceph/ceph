import errno
import stat
from contextlib import contextmanager

import pytest

from tests import mock  # noqa: F401 -- sets up ceph module mocks

from volumes.fs import async_cloner, purge_queue
from volumes.fs.exception import (JobDeferred, MetadataMgrException,
                                  VolumeException, is_quarantined_error)
from volumes.fs.operations.versions.subvolume_attrs import SubvolumeStates
from volumes.fs.volume import VolumeClient


class CephfsError(Exception):
    pass


@pytest.fixture(autouse=True)
def cephfs_error():
    # the mocked cephfs module has no exception classes; `except cephfs.Error`
    # blows up with a TypeError when an exception passes through it.
    with mock.patch.multiple(purge_queue.cephfs, Error=CephfsError,
                             CEPH_STATX_MODE=1, CEPH_STATX_SIZE=2,
                             AT_SYMLINK_NOFOLLOW=4):
        yield


def eacces():
    return VolumeException(-errno.EACCES, "Permission denied")


def test_is_quarantined_error():
    assert is_quarantined_error(eacces())
    assert is_quarantined_error(MetadataMgrException(-errno.EACCES, "denied"))
    assert not is_quarantined_error(VolumeException(-errno.ENOENT, "missing"))
    assert not is_quarantined_error(ValueError())


# -- purge -------------------------------------------------------------------

TRASH_TGT = b'/volumes/_nogroup/sv/.trash/uuid'


@pytest.fixture
def purge_env():
    fs_handle = mock.MagicMock()
    fs_handle.statx.return_value = {'mode': stat.S_IFLNK | 0o777, 'size': len(TRASH_TGT)}
    fs_handle.readlink.return_value = TRASH_TGT
    trashcan = mock.MagicMock()
    trashcan.path = b'/volumes/_deleting'

    @contextmanager
    def open_volume_lockless(fs_client, volname):
        yield fs_handle

    @contextmanager
    def open_trashcan(fs, volspec):
        yield trashcan

    with mock.patch.object(purge_queue, 'open_volume_lockless', open_volume_lockless), \
         mock.patch.object(purge_queue, 'open_trashcan', open_trashcan), \
         mock.patch.object(purge_queue, 'subvolume_purge') as subvolume_purge:
        yield trashcan, subvolume_purge


def purge_entry():
    return purge_queue.purge_trash_entry_for_volume(
        mock.Mock(), mock.Mock(), 'vol', b'entry', lambda: False)


def test_purge_quarantined_trash_is_deferred(purge_env):
    trashcan, subvolume_purge = purge_env
    trashcan.purge.side_effect = eacces()
    with pytest.raises(JobDeferred):
        purge_entry()
    subvolume_purge.assert_not_called()
    trashcan.delink.assert_not_called()


def test_purge_suppressed_errors_then_quarantined_subvolume_is_deferred(purge_env):
    # purge() "succeeds" (rmtree suppressed the errors), opening the
    # subvolume in subvolume_purge() then fails due to quarantine.
    trashcan, subvolume_purge = purge_env
    subvolume_purge.side_effect = eacces()
    with pytest.raises(JobDeferred):
        purge_entry()
    trashcan.delink.assert_not_called()


def test_purge_other_errors_are_not_deferred(purge_env):
    trashcan, subvolume_purge = purge_env
    trashcan.purge.side_effect = VolumeException(-errno.EIO, "I/O error")
    assert purge_entry() == -errno.EIO
    subvolume_purge.assert_not_called()
    trashcan.delink.assert_not_called()


def test_purge_success_delinks(purge_env):
    trashcan, subvolume_purge = purge_env
    assert purge_entry() == 0
    subvolume_purge.assert_called_once()
    trashcan.delink.assert_called_once_with(b'entry')


# -- cloner ------------------------------------------------------------------

def start_clone_sm(state_table):
    return async_cloner.start_clone_sm(mock.Mock(), mock.Mock(), 'vol', 'idx', None,
                                       'clone', state_table, lambda: False, 0)


def test_clone_state_quarantined_is_deferred():
    with mock.patch.object(async_cloner, 'get_clone_state', side_effect=eacces()):
        with pytest.raises(JobDeferred):
            start_clone_sm({})


def test_clone_state_update_quarantined_is_deferred():
    handler = mock.Mock(return_value=(SubvolumeStates.STATE_COMPLETE, False))
    with mock.patch.object(async_cloner, 'get_clone_state',
                           return_value=SubvolumeStates.STATE_INPROGRESS), \
         mock.patch.object(async_cloner, 'set_clone_state', side_effect=eacces()):
        with pytest.raises(JobDeferred):
            start_clone_sm({SubvolumeStates.STATE_INPROGRESS: handler})


def test_clone_other_errors_are_not_deferred():
    err = VolumeException(-errno.EIO, "I/O error")
    with mock.patch.object(async_cloner, 'get_clone_state', side_effect=err):
        with pytest.raises(VolumeException):
            start_clone_sm({})


@contextmanager
def clone_pair_raising(exc):
    raise exc
    yield  # pragma: no cover


@pytest.mark.parametrize('handler', [async_cloner.handle_clone_failed,
                                     async_cloner.handle_clone_complete])
def test_detach_from_quarantined_source_is_deferred(handler):
    with mock.patch.object(async_cloner, 'open_clone_subvol_pair_in_vol',
                           lambda *a, **kw: clone_pair_raising(eacces())):
        with pytest.raises(JobDeferred):
            handler(mock.Mock(), mock.Mock(), 'vol', 'idx', None, 'clone', lambda: False)


@pytest.mark.parametrize('handler', [async_cloner.handle_clone_failed,
                                     async_cloner.handle_clone_complete])
def test_detach_other_errors_are_not_deferred(handler):
    err = VolumeException(-errno.EIO, "I/O error")
    with mock.patch.object(async_cloner, 'open_clone_subvol_pair_in_vol',
                           lambda *a, **kw: clone_pair_raising(err)):
        assert handler(mock.Mock(), mock.Mock(), 'vol', 'idx', None, 'clone',
                       lambda: False) == (None, True)


def test_clone_failure_reason_for_quarantined_source():
    clone = mock.Mock()

    @contextmanager
    def open_at_volume(*args):
        yield clone

    with mock.patch.object(async_cloner, 'open_at_volume', open_at_volume):
        async_cloner.update_clone_failure_status(mock.Mock(), mock.Mock(), 'vol',
                                                 None, 'clone', eacces())
    clone.add_clone_failure.assert_called_once_with(errno.EACCES,
                                                    "source subvolume is quarantined")


# -- quarantine disable ------------------------------------------------------

@pytest.fixture
def vc():
    v = VolumeClient.__new__(VolumeClient)
    v.volspec = mock.Mock(base_dir='/volumes')
    v.mgr = mock.Mock()
    v.cloner = mock.Mock()
    v.purge_queue = mock.Mock()
    with mock.patch('volumes.fs.volume.get_mds_map', return_value={}):
        yield v


@pytest.mark.parametrize('enable,rc,cleared', [(False, 0, True),
                                               (False, -errno.EIO, False),
                                               (True, 0, False)])
def test_quarantine_disable_clears_deferred(vc, enable, rc, cleared):
    with mock.patch.object(VolumeClient, '_send_quarantine_command',
                           return_value=(rc, '', '')):
        vc.quarantine_subvolume(vol_name='vol', sub_name='sv', group_name=None,
                                enable=enable)
    if cleared:
        vc.cloner.clear_deferred.assert_called_once_with('vol')
        vc.purge_queue.clear_deferred.assert_called_once_with('vol')
    else:
        vc.cloner.clear_deferred.assert_not_called()
        vc.purge_queue.clear_deferred.assert_not_called()


def test_quarantine_command_is_one_shot(vc):
    # a command dropped by the MDS on a connection reset must fail (EPIPE)
    # rather than block mgr/volumes forever
    mds_map = {'up': {'mds_0': 4242},
               'info': {'gid_4242': {'state': 'up:active'}}}
    vc.mgr.tell_command.return_value = (0, '', '')
    vc._send_quarantine_command(mds_map, "quarantine enable", "/volumes/_nogroup/sv")
    vc.mgr.tell_command.assert_called_once_with(
        "mds", "4242", {"prefix": "quarantine enable", "path": "/volumes/_nogroup/sv"},
        one_shot=True)
