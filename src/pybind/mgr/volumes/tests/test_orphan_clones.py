import errno

import pytest

from tests import mock  # noqa: F401 -- sets up ceph module mocks

from volumes.fs.exception import IndexException, VolumeException
from volumes.fs.operations.versions import subvolume_v1
from volumes.fs.operations.versions.metadata_manager import MetadataManager
from volumes.fs.operations.versions.subvolume_v1 import SubvolumeV1


class CephfsError(Exception):
    pass


INDEX_PATH = b'/volumes/_index/clone'
SNAP = 'snap1'
LIVE = 'b1e7c5a0-0000-4000-8000-000000000001'
ORPHAN = 'b1e7c5a0-0000-4000-8000-000000000002'
OTHER_SNAP_ORPHAN = 'b1e7c5a0-0000-4000-8000-000000000003'


@pytest.fixture(autouse=True)
def cephfs_error():
    # the mocked cephfs module has no exception classes; `except cephfs.Error`
    # blows up with a TypeError when an exception passes through it.
    with mock.patch.object(subvolume_v1.cephfs, 'Error', CephfsError, create=True):
        yield


@pytest.fixture
def subvol():
    # bypass __init__, which needs a live volume, and wire up only what
    # remove_snapshot() touches.
    sv = SubvolumeV1.__new__(SubvolumeV1)
    sv.fs = mock.MagicMock()
    sv.vol_spec = mock.MagicMock()
    sv.metadata_mgr = MetadataManager(sv.fs, b'/volumes/_nogroup/sv/.meta', 0o640)
    sv.metadata_mgr.config.read_string(
        '[clone snaps]\n'
        f'{LIVE} = {SNAP}\n'
        f'{ORPHAN} = {SNAP}\n'
        f'{OTHER_SNAP_ORPHAN} = snap2\n')
    sv.metadata_mgr.flush = mock.MagicMock()
    sv.get_snap_section_name = lambda snapname: f'SNAP_METADATA_{snapname}'
    sv.snapshot_path = lambda snapname: b'/volumes/_nogroup/sv/uuid/.snap/' + snapname.encode('utf-8')

    index = mock.MagicMock()
    index.path = INDEX_PATH
    index_ctx = mock.MagicMock()
    index_ctx.__enter__.return_value = index
    with mock.patch.object(subvolume_v1, 'open_clone_index', return_value=index_ctx), \
         mock.patch.object(subvolume_v1, 'rmsnap') as rmsnap:
        sv.rmsnap = rmsnap
        yield sv


def index_entries(*track_ids):
    paths = {INDEX_PATH + b'/' + t.encode('utf-8') for t in track_ids}

    def lstat(path):
        if path not in paths:
            raise CephfsError(errno.ENOENT, 'No such file or directory')
    return lstat


def clone_snaps(sv):
    return dict(sv.metadata_mgr.config.items('clone snaps'))


def test_orphan_clone_does_not_block_snapshot_removal(subvol):
    subvol.fs.lstat.side_effect = index_entries()

    subvol.remove_snapshot(SNAP)

    subvol.rmsnap.assert_called_once()
    # the orphan of another snapshot is left alone
    assert clone_snaps(subvol) == {OTHER_SNAP_ORPHAN: 'snap2'}


def test_pending_clone_still_blocks_snapshot_removal(subvol):
    subvol.fs.lstat.side_effect = index_entries(LIVE)

    with pytest.raises(VolumeException) as e:
        subvol.remove_snapshot(SNAP)

    assert e.value.errno == -errno.EAGAIN
    subvol.rmsnap.assert_not_called()
    # the orphan is dropped and persisted even though removal is refused
    assert clone_snaps(subvol) == {LIVE: SNAP, OTHER_SNAP_ORPHAN: 'snap2'}
    subvol.metadata_mgr.flush.assert_called_once()


def test_nothing_flushed_without_orphans(subvol):
    subvol.fs.lstat.side_effect = index_entries(LIVE, ORPHAN)

    subvol.remove_orphan_clones(SNAP)

    assert len(clone_snaps(subvol)) == 3
    subvol.metadata_mgr.flush.assert_not_called()


def test_index_lookup_error_keeps_clone_entries(subvol):
    subvol.fs.lstat.side_effect = CephfsError(errno.EIO, 'Input/output error')

    with pytest.raises(VolumeException) as e:
        subvol.remove_snapshot(SNAP)

    assert e.value.errno == -errno.EIO
    assert len(clone_snaps(subvol)) == 3
    subvol.metadata_mgr.flush.assert_not_called()


def test_missing_clone_index_keeps_clone_entries(subvol):
    with mock.patch.object(subvolume_v1, 'open_clone_index',
                           side_effect=IndexException(-errno.ENOENT, 'missing')):
        subvol.remove_orphan_clones(SNAP)

    assert len(clone_snaps(subvol)) == 3
    subvol.fs.lstat.assert_not_called()
