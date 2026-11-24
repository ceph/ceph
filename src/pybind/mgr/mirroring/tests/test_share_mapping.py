import errno
import json
import pickle
import threading
import time
from types import SimpleNamespace

import pytest

from tests import mock  # noqa: F401 -- sets up ceph module mocks

from mirroring.fs import snapshot_mirror
from mirroring.fs.dir_map.load import load_shares
from mirroring.fs.dir_map.policy import Policy
from mirroring.fs.dir_map.update import UpdateDirMapRequest
from mirroring.fs.notify import InstanceWatcher
from mirroring.fs.snapshot_mirror import FSPolicy, FSSnapshotMirror
from mirroring.tests.fakes import FakeIoctx

P1 = '0f6d5e0a-6a1c-4c55-9a43-3c1c9d0b3a11'
P2 = '5a7c2b44-1f6b-4d0c-8f6e-2b9f0c7d1e22'
LEGACY = '9b3e7f10-2d4a-4b6c-a8e1-7c5d3f9a0e33'


def peer_record():
    return json.dumps({'version': 1, 'auth_mode': 'per_share'}).encode('utf-8')


def share_value(ioctx, dir_path):
    return json.loads(ioctx.omap[f'share_{dir_path}'])


# resolve_dir_peer

def make_mirror(legacy=(), per_share=()):
    # bypass __init__, which starts the policy threads and a CephfsClient
    m = object.__new__(FSSnapshotMirror)
    m.lock = threading.Lock()
    m.fs_map = {'filesystems': [{'id': 1, 'mdsmap': {'fs_name': 'a'},
                                 'mirror_info': {'peers': {u: {} for u in legacy}}}]}
    ioctx = FakeIoctx({f'peer_{u}': peer_record() for u in per_share})
    return m, ioctx


@pytest.mark.parametrize('legacy, per_share, peer_uuid, expected', [
    ((LEGACY,), (P1, P2), None, None),        # legacy peer present: unmapped
    ((), (), None, None),                     # no peers: unmapped, waits
    ((), (P1,), None, P1),                    # the only per-share peer
    ((LEGACY,), (P1, P2), P2, P2),            # explicit per-share peer
])
def test_resolve_dir_peer(legacy, per_share, peer_uuid, expected):
    m, ioctx = make_mirror(legacy, per_share)
    assert m.resolve_dir_peer('a', ioctx, peer_uuid) == expected


@pytest.mark.parametrize('legacy, per_share, peer_uuid, expected', [
    ((), (P1, P2), None, -errno.EINVAL),      # several per-share peers: ambiguous
    ((LEGACY,), (P1,), LEGACY, -errno.EINVAL),  # a legacy peer's uuid
    ((LEGACY,), (P1,), P2, -errno.ENOENT),    # not a peer of this fs
])
def test_resolve_dir_peer_rejects(legacy, per_share, peer_uuid, expected):
    m, ioctx = make_mirror(legacy, per_share)
    with pytest.raises(snapshot_mirror.MirrorException) as e:
        m.resolve_dir_peer('a', ioctx, peer_uuid)
    assert e.value.args[0] == expected


def test_add_dir_passes_resolved_peer():
    m, ioctx = make_mirror((), (P1,))
    fspolicy = SimpleNamespace(ioctx=ioctx, add_dir=mock.Mock())
    m.pool_policy = {'a': fspolicy}
    r, _, err = m.add_dir('a', '/volumes/g/sv1/', None)
    assert r == 0, err
    fspolicy.add_dir.assert_called_once_with('/volumes/g/sv1', P1)


def test_add_dir_refusal_adds_nothing():
    m, ioctx = make_mirror((LEGACY,), (P1,))
    fspolicy = SimpleNamespace(ioctx=ioctx, add_dir=mock.Mock())
    m.pool_policy = {'a': fspolicy}
    r, _, _ = m.add_dir('a', '/volumes/g/sv1', P2)
    assert r == -errno.ENOENT
    fspolicy.add_dir.assert_not_called()


# UpdateDirMapRequest

def test_share_written_with_dir_map_in_one_op():
    ioctx = FakeIoctx()
    done = []
    UpdateDirMapRequest(ioctx, {'/a': {'version': 1}}, [], done.append,
                        share_updates={'/a': P1}).send()
    assert done == [0]
    assert ioctx.ops == [(['dir_map_/a', 'share_/a'], [])]
    assert share_value(ioctx, '/a') == {'version': 1, 'peer_uuid': P1}


def test_no_share_written_without_share_updates():
    ioctx = FakeIoctx()
    UpdateDirMapRequest(ioctx, {'/a': {'version': 1}}, [], lambda r: None).send()
    assert 'share_/a' not in ioctx.omap and 'dir_map_/a' in ioctx.omap


def test_removal_drops_dir_map_and_share_in_one_op():
    ioctx = FakeIoctx({'dir_map_/a': b'x', 'share_/a': b'y', 'dir_map_/b': b'x'})
    UpdateDirMapRequest(ioctx, {}, ['/a'], lambda r: None).send()
    assert ioctx.ops == [([], ['dir_map_/a', 'share_/a'])]
    assert set(ioctx.omap) == {'dir_map_/b'}


# load_shares

@pytest.mark.parametrize('count, reads', [(0, 1), (1, 1), (600, 3), (512, 3)])
def test_load_shares_pages(count, reads):
    omap = {f'share_/d{i:04d}': json.dumps({'version': 1, 'peer_uuid': f'u{i}'}).encode('utf-8')
            for i in range(count)}
    omap['dir_map_/unrelated'] = b'x'
    ioctx = FakeIoctx(omap)
    dir_to_peer = load_shares(ioctx)
    assert len(dir_to_peer) == count and ioctx.reads == reads
    if count:
        assert dir_to_peer['/d0000'] == 'u0'


# FSPolicy

@pytest.fixture
def fspolicy():
    with mock.patch.object(snapshot_mirror, 'InstanceWatcher'):
        p = FSPolicy(mock.MagicMock(), FakeIoctx())
        p.init({}, {}, {})
    yield p
    p.shutdown()
    p.finisher.thread.stop()


def test_fspolicy_add_dir_stores_mapping(fspolicy):
    fspolicy.add_dir('/volumes/g/sv1', P1)
    fspolicy.add_dir('/volumes/g/sv2')
    assert share_value(fspolicy.ioctx, '/volumes/g/sv1')['peer_uuid'] == P1
    assert 'share_/volumes/g/sv2' not in fspolicy.ioctx.omap
    assert 'dir_map_/volumes/g/sv2' in fspolicy.ioctx.omap
    assert fspolicy.dir_to_peer == {'/volumes/g/sv1': P1}


def test_fspolicy_status_shows_mapped_peer(fspolicy):
    fspolicy.add_dir('/volumes/g/sv1', P1)
    fspolicy.add_dir('/volumes/g/sv2')
    assert json.loads(fspolicy.status('/volumes/g/sv1')[1])['peer_uuid'] == P1
    assert 'peer_uuid' not in json.loads(fspolicy.status('/volumes/g/sv2')[1])


def test_fspolicy_add_tracked_dir_keeps_its_mapping(fspolicy):
    fspolicy.add_dir('/volumes/g/sv1', P1)
    with pytest.raises(snapshot_mirror.MirrorException) as e:
        fspolicy.add_dir('/volumes/g/sv1', P2)
    assert e.value.args[0] == -errno.EEXIST
    assert share_value(fspolicy.ioctx, '/volumes/g/sv1')['peer_uuid'] == P1
    assert fspolicy.dir_to_peer == {'/volumes/g/sv1': P1}


def test_fspolicy_nested_dir_stores_nothing(fspolicy):
    fspolicy.add_dir('/volumes/g/sv1', P1)
    with pytest.raises(snapshot_mirror.MirrorException) as e:
        fspolicy.add_dir('/volumes/g', P2)
    assert e.value.args[0] == -errno.EINVAL
    assert 'share_/volumes/g' not in fspolicy.ioctx.omap
    assert '/volumes/g' not in fspolicy.dir_to_peer


@pytest.mark.parametrize('r, kept', [(0, False), (-errno.EIO, True)])
def test_fspolicy_removal_forgets_mapping_on_success(fspolicy, r, kept):
    fspolicy.add_dir('/volumes/g/sv1', P1)
    fspolicy.continue_action([], ['/volumes/g/sv1'], r)
    assert ('/volumes/g/sv1' in fspolicy.dir_to_peer) == kept


# capability gate: mapped directories only go to instances advertising per_share

def test_heartbeat_reply_features():
    listener = mock.Mock()
    with mock.patch.object(InstanceWatcher, 'schedule_notify_task'):
        watcher = InstanceWatcher(FakeIoctx(), {}, listener)
        watcher.handle_notify(None, 0, [(4242, 1, b'{"addr": "a1", "features": ["per_share"]}'),
                                        (4343, 2, b'{"addr": "a2"}')], [])
    listener.handle_instances.assert_called_once_with(
        {'4242': {'addr': 'a1', 'features': ['per_share']},
         '4343': {'addr': 'a2', 'features': []}}, {})


def test_fspolicy_records_instance_features(fspolicy):
    fspolicy.update_instances({'4242': {'addr': 'a1', 'features': ['per_share']},
                               '4343': {'addr': 'a2', 'features': []}}, {})
    deadline = time.time() + 5
    while not fspolicy.policy.per_share_instances and time.time() < deadline:
        time.sleep(0.01)
    assert fspolicy.policy.per_share_instances == {'4242'}
    assert pickle.loads(fspolicy.ioctx.omap['instance_4242'])['features'] == ['per_share']
    assert pickle.loads(fspolicy.ioctx.omap['instance_4343'])['features'] == []


def test_fspolicy_init_reads_features_from_records():
    with mock.patch.object(snapshot_mirror, 'InstanceWatcher'):
        p = FSPolicy(mock.MagicMock(), FakeIoctx())
        p.init({}, {'4242': {'version': 1, 'addr': 'a1', 'features': ['per_share']},
                    '4343': {'version': 1, 'addr': 'a2'}}, {})
    try:
        assert p.policy.per_share_instances == {'4242'}
    finally:
        p.shutdown()
        p.finisher.thread.stop()


def assign(policy, dir_path):
    policy.add_dir(dir_path)
    policy.start_action(dir_path)
    return policy.lookup(dir_path)['instance_id']


def test_policy_maps_mapped_dir_only_to_per_share_instance():
    policy = Policy(requires_per_share=lambda dir_path: dir_path == '/m')
    policy.add_instances(['i1', 'i2'], initial_update=True, per_share=['i2'])
    assert assign(policy, '/x') == 'i1'
    assert assign(policy, '/y') == 'i2'
    # both instances hold one directory; the mapped one may only go to i2
    assert assign(policy, '/m') == 'i2'
    assert assign(policy, '/u') == 'i1'


def test_policy_stalls_and_retries_mapped_dir():
    policy = Policy(requires_per_share=lambda dir_path: dir_path == '/m')
    policy.add_instances(['i1'], initial_update=True)
    assert assign(policy, '/m') is None
    assert policy.dir_status('/m') == {'state': 'stalled',
                                       'reason': 'no mirror daemon running supports per-share peers'}
    # a per-share instance joining retries the stalled directory
    assert '/m' in policy.add_instances(['i2'], per_share=['i2'])
    policy.start_action('/m')
    assert policy.lookup('/m')['instance_id'] == 'i2'


def test_policy_forgets_removed_per_share_instance():
    policy = Policy()
    policy.add_instances(['i1'], initial_update=True, per_share=['i1'])
    policy.remove_instances(['i1'])
    assert policy.per_share_instances == set()
