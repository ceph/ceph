import base64
import errno
import json
import threading
import uuid
from types import SimpleNamespace

import pytest

from tests import mock  # noqa: F401 -- sets up ceph module mocks

import rados

from mirroring.fs import snapshot_mirror
from mirroring.fs.snapshot_mirror import FSSnapshotMirror
from mirroring.fs.utils import AuthMode

LOCAL_FSID = '11111111-1111-1111-1111-111111111111'
REMOTE_FSID = '22222222-2222-2222-2222-222222222222'
OTHER_FSID = '33333333-3333-3333-3333-333333333333'
MON_HOST = 'v2:10.0.0.1:3300'
KEY = 'AQBfakeKeyForUnitTestsOnly=='
SITE = 'site-remote'


class FakeMgr:
    """mon commands and config-key store of one cluster"""
    def __init__(self, fsid, entities=None):
        self.rados = mock.MagicMock()
        self.rados.get_fsid.return_value = fsid
        self.rados.conf_get.return_value = MON_HOST
        self.entities = dict(entities or {})  # entity name -> caps
        self.config_key = {}
        self.commands = []

    def mon_command(self, cmd):
        self.commands.append(cmd)
        prefix = cmd['prefix']
        if prefix == 'config-key set':
            self.config_key[cmd['key']] = cmd['val']
        elif prefix == 'config-key rm':
            self.config_key.pop(cmd['key'], None)
        elif prefix == 'fs authorize':
            self.entities[cmd['entity']] = {'mds': 'allow rwps fsname=c', 'mon': 'allow r fsname=c',
                                            'osd': 'allow rw tag cephfs data=c'}
        elif prefix == 'auth get':
            if cmd['entity'] not in self.entities:
                return -errno.ENOENT, '', 'entity does not exist'
            return 0, json.dumps([{'entity': cmd['entity'], 'key': KEY,
                                   'caps': self.entities[cmd['entity']]}]), ''
        return 0, '', ''

    def prefixes(self):
        return [cmd['prefix'] for cmd in self.commands]


class FakeIoctx:
    """omap of the cephfs_mirror object of one file system"""
    def __init__(self):
        self.omap = {}
        self.notifies = []
        self.fail_write = False

    def get_omap_vals(self, read_op, start, prefix, max_return):
        items = [(k, v) for k, v in sorted(self.omap.items()) if k.startswith(prefix)]
        return iter(items[:max_return]), 0

    def operate_read_op(self, read_op, oid):
        pass

    def set_omap(self, write_op, keys, vals):
        self._pending = dict(zip(keys, vals))

    def operate_write_op(self, write_op, oid):
        if self.fail_write:
            raise rados.Error('write failed', errno=errno.EIO)
        self.omap.update(self._pending)

    def aio_notify(self, oid, cbk, msg=''):
        self.notifies.append((oid, json.loads(msg)))


class FakeRemote:
    """rados handle connected to the remote cluster with the super-key"""
    def __init__(self, fsid=REMOTE_FSID, filesystems=(('c', 7),)):
        self.fsid = fsid
        self.filesystems = filesystems

    def get_fsid(self):
        return self.fsid

    def mon_command(self, cmd, inbuf):
        assert json.loads(cmd)['prefix'] == 'fs dump'
        fs_map = {'filesystems': [{'id': fscid, 'mdsmap': {'fs_name': name}}
                                  for name, fscid in self.filesystems]}
        return 0, json.dumps(fs_map).encode(), ''


def make_mirror(mgr, fsmap_peers=None, mirrored=('a',)):
    # bypass __init__, which starts the policy threads and a CephfsClient
    m = object.__new__(FSSnapshotMirror)
    m.mgr = mgr
    m.rados = mgr.rados
    m.lock = threading.Lock()
    m.fs_map = {'filesystems': [{'id': 1, 'mdsmap': {'fs_name': 'a'},
                                 'mirror_info': {'peers': fsmap_peers or {}}}]}
    m.pool_policy = {fs: SimpleNamespace(ioctx=FakeIoctx()) for fs in mirrored}
    return m


def new_super_key():
    peer_uuid = str(uuid.uuid4())
    return f'client.mirror_peer.{peer_uuid}', peer_uuid


SUPER_KEY_CAPS = {'mon': 'allow r, allow command "auth get" with entity prefix "client.mirror."'}


def create_token(name, caps=SUPER_KEY_CAPS, auth_mode=AuthMode.per_share):
    """run peer_bootstrap_create on a secondary that has the super-key"""
    secondary = FakeMgr(REMOTE_FSID, {name: caps} if caps is not None else {})
    m = make_mirror(secondary, mirrored=())
    r, out, err = m.peer_bootstrap_create('c', name, SITE, auth_mode)
    token = json.loads(out)['token'] if r == 0 else None
    return r, token, err, secondary


def encode(token_dct):
    return base64.b64encode(json.dumps(token_dct).encode('utf-8')).decode('utf-8')


def decode(token):
    return json.loads(base64.b64decode(token).decode('utf-8'))


def import_token(m, token, remote=None, filesystem='a'):
    with mock.patch.object(snapshot_mirror, 'connect_to_cluster',
                           return_value=remote or FakeRemote()), \
         mock.patch.object(snapshot_mirror, 'disconnect_from_cluster'):
        return m.peer_bootstrap_import(filesystem, token)


def ioctx(m):
    return m.pool_policy['a'].ioctx


# peer_bootstrap_create (secondary)

def test_create_per_share_token():
    name, peer_uuid = new_super_key()
    r, token, err, secondary = create_token(name)
    assert r == 0, err
    assert decode(token) == {'fsid': REMOTE_FSID, 'filesystem': 'c', 'user': name,
                             'site_name': SITE, 'key': KEY, 'mon_host': MON_HOST,
                             'peer_uuid': peer_uuid, 'auth_mode': 'per_share'}
    # the super-key is provisioned by the orchestrator or the administrator
    assert 'fs authorize' not in secondary.prefixes()


@pytest.mark.parametrize('name', [
    'client.mirror_remote',
    'client.mirror_peer.',
    'client.mirror_peer.not-a-uuid',
    'client.mirror_peer.' + str(uuid.uuid4()).upper(),
    'client.mirror_peer.{' + str(uuid.uuid4()) + '}',
    'client.mirror_peer.' + uuid.uuid4().hex,
])
def test_create_rejects_bad_super_key_name(name):
    r, _, _, secondary = create_token(name)
    assert r == -errno.EINVAL
    assert 'auth get' not in secondary.prefixes()


def test_create_super_key_missing():
    name, _ = new_super_key()
    r, _, err, _ = create_token(name, caps=None)
    assert r == -errno.ENOENT
    assert 'does not exist' in err


@pytest.mark.parametrize('extra', [{'mds': 'allow rw'}, {'osd': 'allow rw'}])
def test_create_rejects_super_key_with_mds_or_osd_caps(extra):
    name, _ = new_super_key()
    r, _, _, _ = create_token(name, caps=dict(SUPER_KEY_CAPS, **extra))
    assert r == -errno.EINVAL


def test_create_legacy_token_unchanged():
    r, token, err, secondary = create_token('client.mirror_remote', caps=None,
                                            auth_mode=AuthMode.legacy)
    assert r == 0, err
    authorize = [c for c in secondary.commands if c['prefix'] == 'fs authorize']
    assert authorize and authorize[0]['caps'] == ['/', 'rwps']
    assert 'peer_uuid' not in decode(token) and 'auth_mode' not in decode(token)


# peer_bootstrap_import (primary)

def test_import_writes_peer_record_and_secret():
    name, peer_uuid = new_super_key()
    _, token, _, _ = create_token(name)
    primary = FakeMgr(LOCAL_FSID)
    m = make_mirror(primary)
    r, _, err = import_token(m, token)
    assert r == 0, err
    omap = ioctx(m).omap
    assert list(omap) == [f'peer_{peer_uuid}']
    record = json.loads(omap[f'peer_{peer_uuid}'])
    assert record == {'version': 1, 'client_name': name, 'site_name': SITE,
                      'remote_fsid': REMOTE_FSID, 'remote_fs_name': 'c',
                      'auth_mode': 'per_share'}
    # the secret goes to config-key only, never to the mirror object
    assert KEY not in omap[f'peer_{peer_uuid}'].decode('utf-8')
    secret = json.loads(primary.config_key[f'cephfs/mirror/peer/a/{peer_uuid}'])
    assert secret == {'fsid': REMOTE_FSID, 'mon_host': MON_HOST, 'key': KEY}
    assert ioctx(m).notifies == [('cephfs_mirror', {'type': 'peers_changed'})]
    # per-share peers stay out of the fs map
    assert 'fs mirror peer_add' not in primary.prefixes()


def test_reimport_is_noop():
    name, _ = new_super_key()
    _, token, _, _ = create_token(name)
    m = make_mirror(FakeMgr(LOCAL_FSID))
    assert import_token(m, token)[0] == 0
    omap, config_key = dict(ioctx(m).omap), dict(m.mgr.config_key)
    r, _, err = import_token(m, token)
    assert r == 0, err
    assert ioctx(m).omap == omap and m.mgr.config_key == config_key
    assert len(ioctx(m).notifies) == 1


@pytest.mark.parametrize('fsid, fs_name, filesystems, expected', [
    (REMOTE_FSID, 'c', (('c', 7),), -errno.EEXIST),            # same remote fs
    (REMOTE_FSID, 'd', (('c', 7), ('d', 8)), 0),               # same cluster, other fs
    (OTHER_FSID, 'c', (('c', 7),), 0),                         # other cluster, same fs name
])
def test_import_one_per_share_peer_per_remote_fs(fsid, fs_name, filesystems, expected):
    m = make_mirror(FakeMgr(LOCAL_FSID))
    name, _ = new_super_key()
    assert import_token(m, create_token(name)[1])[0] == 0
    name2, peer_uuid2 = new_super_key()
    token2 = dict(decode(create_token(name2)[1]), fsid=fsid, filesystem=fs_name)
    r, _, err = import_token(m, encode(token2), remote=FakeRemote(fsid, filesystems))
    assert r == expected, err
    assert (f'peer_{peer_uuid2}' in ioctx(m).omap) == (expected == 0)


def test_import_next_to_legacy_peer():
    legacy = {'u1': {'remote': {'client_name': 'client.mirror_remote',
                                'cluster_name': SITE, 'fs_name': 'c'}}}
    m = make_mirror(FakeMgr(LOCAL_FSID), fsmap_peers=legacy)
    name, _ = new_super_key()
    r, _, err = import_token(m, create_token(name)[1])
    assert r == 0, err


def test_import_same_cluster_other_fs():
    m = make_mirror(FakeMgr(LOCAL_FSID))
    name, peer_uuid = new_super_key()
    token = dict(decode(create_token(name)[1]), fsid=LOCAL_FSID, filesystem='b')
    r, _, err = import_token(m, encode(token),
                             remote=FakeRemote(LOCAL_FSID, (('a', 1), ('b', 2))))
    assert r == 0, err
    assert f'peer_{peer_uuid}' in ioctx(m).omap


def _bad_name(dct):
    dct['user'] = 'client.mirror_remote'


def _uuid_mismatch(dct):
    dct['peer_uuid'] = str(uuid.uuid4())


@pytest.mark.parametrize('desc, change, remote, filesystem, expected', [
    ('not mirrored', None, None, 'b', -errno.EINVAL),
    ('invalid auth_mode', {'auth_mode': 'foo'}, None, 'a', -errno.EINVAL),
    ('missing key', {'key': None}, None, 'a', -errno.EINVAL),
    ('missing user', {'user': None}, None, 'a', -errno.EINVAL),
    ('missing fsid', {'fsid': None}, None, 'a', -errno.EINVAL),
    ('missing mon_host', {'mon_host': None}, None, 'a', -errno.EINVAL),
    ('missing site_name', {'site_name': None}, None, 'a', -errno.EINVAL),
    ('missing filesystem', {'filesystem': None}, None, 'a', -errno.EINVAL),
    ('not a super-key name', _bad_name, None, 'a', -errno.EINVAL),
    ('peer_uuid does not match the name', _uuid_mismatch, None, 'a', -errno.EINVAL),
    ('fsid mismatch', None, FakeRemote(fsid=OTHER_FSID), 'a', -errno.EINVAL),
    ('remote fs missing', None, FakeRemote(filesystems=(('z', 9),)), 'a', -errno.ENOENT),
    ('mirroring into itself', {'fsid': LOCAL_FSID, 'filesystem': 'a'},
     FakeRemote(LOCAL_FSID, (('a', 1),)), 'a', -errno.EINVAL),
])
def test_import_rejects(desc, change, remote, filesystem, expected):
    name, _ = new_super_key()
    dct = decode(create_token(name)[1])
    if callable(change):
        change(dct)
    elif change:
        dct.update(change)
        dct = {k: v for k, v in dct.items() if v is not None}
    m = make_mirror(FakeMgr(LOCAL_FSID))
    r, _, err = import_token(m, encode(dct), remote=remote, filesystem=filesystem)
    assert r == expected, (desc, err)
    assert not ioctx(m).omap and not m.mgr.config_key and not ioctx(m).notifies


def test_import_rejects_garbage_token():
    m = make_mirror(FakeMgr(LOCAL_FSID))
    r, _, err = m.peer_bootstrap_import('a', 'not-a-token')
    assert r == -errno.EINVAL
    assert 'failed to parse token' in err


def test_import_omap_write_failure_removes_secret():
    m = make_mirror(FakeMgr(LOCAL_FSID))
    ioctx(m).fail_write = True
    name, _ = new_super_key()
    r, _, _ = import_token(m, create_token(name)[1])
    assert r == -errno.EIO
    assert not ioctx(m).omap and not m.mgr.config_key and not ioctx(m).notifies


def test_import_legacy_token_goes_to_peer_add():
    _, token, _, _ = create_token('client.mirror_remote', caps=None, auth_mode=AuthMode.legacy)
    m = make_mirror(FakeMgr(LOCAL_FSID))
    with mock.patch.object(FSSnapshotMirror, 'peer_add', return_value=(0, '{}', '')) as peer_add:
        m.peer_bootstrap_import('a', token)
    peer_add.assert_called_once_with('a', f'client.mirror_remote@{SITE}', 'c',
                                     {'fsid': REMOTE_FSID, 'key': KEY, 'mon_host': MON_HOST})
