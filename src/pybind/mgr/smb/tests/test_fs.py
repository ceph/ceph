from unittest import mock

import pytest

import smb.fs


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
