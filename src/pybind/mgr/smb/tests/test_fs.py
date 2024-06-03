import time
import unittest
from unittest import mock

import pytest

import smb.fs
from smb.fs import _TTLCache


def test_mocked_fs_authorizer():
    def mmcmd(cmd):
        assert cmd['filesystem'] == 'cephfs'
        if 'kaboom' in cmd['entity']:
            return -5, 'oops', 'fail'
        return 0, 'ok', 'nice'

    m = mock.MagicMock()
    m.mon_command.side_effect = mmcmd

    fsauth = smb.fs.FileSystemAuthorizer(m)
    fsauth.authorize_entity('cephfs', 'client.smb.foo')
    with pytest.raises(smb.fs.AuthorizationGrantError):
        fsauth.authorize_entity('cephfs', 'client.smb.kaboom')


def test_mocked_fs_path_resolver(monkeypatch):
    # we have to "re-patch" whatever cephfs module gets mocked with because
    # the ObjectNotFound attribute is not an exception in the test environment
    monkeypatch.setattr('cephfs.ObjectNotFound', KeyError)

    def mmcmd(cmd):
        if cmd['prefix'] == 'fs subvolume getpath':
            if cmd['vol_name'] == 'cephfs' and cmd['sub_name'] == 'beta':
                return 0, '/volumes/cool/path/f00d-600d', ''
        return -5, '', 'eek'

    m = mock.MagicMock()
    m.mon_command.side_effect = mmcmd

    fspr = smb.fs.CephFSPathResolver(m, client=m)

    # resolve
    path = fspr.resolve('cephfs', '', '', '/zowie')
    assert path == '/zowie'

    path = fspr.resolve('cephfs', 'alpha', 'beta', '/zowie')
    assert path == '/volumes/cool/path/f00d-600d/zowie'

    with pytest.raises(smb.fs.CephFSSubvolumeResolutionError):
        path = fspr.resolve('ouch', 'alpha', 'beta', '/zowie')

    # resolve_exists
    m.connection_pool.get_fs_handle.return_value.statx.return_value = {
        'mode': 0o41777
    }
    path = fspr.resolve_exists('cephfs', 'alpha', 'beta', '/zowie')
    assert path == '/volumes/cool/path/f00d-600d/zowie'

    m.connection_pool.get_fs_handle.return_value.statx.return_value = {
        'mode': 0o101777
    }
    with pytest.raises(NotADirectoryError):
        fspr.resolve_exists('cephfs', 'alpha', 'beta', '/zowie')

    m.connection_pool.get_fs_handle.return_value.statx.side_effect = (
        mock.MagicMock(side_effect=OSError('nope'))
    )
    with pytest.raises(FileNotFoundError):
        fspr.resolve_exists('cephfs', 'alpha', 'beta', '/zowie')


class TestTTLCache(unittest.TestCase):
    def setUp(self):
        self.cache = _TTLCache(
            ttl=1, maxsize=3
        )  # Short TTL and small size for testing

    def test_cache_set_and_get(self):
        self.cache.set(('key1', 'key2', 'key3'), ('value1', 'val', 'test'))
        self.assertEqual(
            self.cache.get(('key1', 'key2', 'key3')),
            ('value1', 'val', 'test'),
        )

    def test_cache_expiry(self):
        self.cache.set(('key1', 'key2', 'key3'), ('value1', 'val', 'test'))
        time.sleep(1.5)  # Wait for the TTL to expire
        self.assertIsNone(self.cache.get(('key1', 'key2', 'key3')))

    def test_cache_eviction(self):
        # Fill the cache to maxsize
        self.cache.set(('key1', 'key2', 'key3'), ('value1', 'val', 'test'))
        self.cache.set(('key4', 'key5', 'key6'), ('value2', 'val', 'test'))
        self.cache.set(('key7', 'key8', 'key9'), ('value3', 'val', 'test'))

        # Add another entry to trigger eviction of the oldest
        self.cache.set(('key10', 'key11', 'key12'), ('value4', 'val', 'test'))

        # Ensure oldest entry is evicted
        self.assertIsNone(self.cache.get(('key1', 'key2', 'key3')))

        # Ensure other entries are present
        self.assertEqual(
            self.cache.get(('key4', 'key5', 'key6')),
            ('value2', 'val', 'test'),
        )
        self.assertEqual(
            self.cache.get(('key7', 'key8', 'key9')),
            ('value3', 'val', 'test'),
        )
        self.assertEqual(
            self.cache.get(('key10', 'key11', 'key12')),
            ('value4', 'val', 'test'),
        )

    def test_cache_clear(self):
        self.cache.set(('key1', 'key2', 'key3'), ('value1', 'val', 'test'))
        self.cache.clear()
        self.assertIsNone(self.cache.get(('key1', 'key2', 'key3')))


def test_caching_fs_path_resolver(monkeypatch):
    monkeypatch.setattr('cephfs.ObjectNotFound', KeyError)

    def mmcmd(cmd):
        if cmd['prefix'] == 'fs subvolume getpath':
            if (
                cmd['vol_name'] == 'cached_cephfs'
                and cmd['sub_name'] == 'cached_beta'
            ):
                return 0, '/volumes/cool/path/f00d-600d', ''
        return -5, '', 'cached_eek'

    m = mock.MagicMock()
    m.mon_command.side_effect = mmcmd

    fspr = smb.fs.CachingCephFSPathResolver(m, client=m)

    # Resolve a path (cache miss)
    path = fspr.resolve(
        'cached_cephfs', 'cached_alpha', 'cached_beta', '/zowie'
    )
    assert path == '/volumes/cool/path/f00d-600d/zowie'
    assert len(fspr._cache) == 1
    assert m.mon_command.call_count == 1

    # Resolve the same path again (cache hit)
    path = fspr.resolve(
        'cached_cephfs', 'cached_alpha', 'cached_beta', '/zowie'
    )
    assert path == '/volumes/cool/path/f00d-600d/zowie'

    # Ensure cache size remains the same
    assert len(fspr._cache) == 1
    assert m.mon_command.call_count == 1

    path = fspr.resolve('cached_cephfs', '', '', '/zowie')
    assert path == '/zowie'

    # If subvolume is empty cache size should remain the same
    assert len(fspr._cache) == 1
    assert m.mon_command.call_count == 1

    # Clear cache and validate
    fspr._cache.clear()
    assert len(fspr._cache) == 0

    # Re-resolve to repopulate cache
    path = fspr.resolve(
        'cached_cephfs', 'cached_alpha', 'cached_beta', '/zowie'
    )
    assert path == '/volumes/cool/path/f00d-600d/zowie'
    assert len(fspr._cache) == 1
    assert m.mon_command.call_count == 2
