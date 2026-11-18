import io
import json
import sys

import pytest
from unittest.mock import patch

from ceph_volume.api import lvm
from ceph_volume.devices.lvm import lockbox
from ceph_volume.util.constants import cephx_lockbox_secret_tag


SECRET = 'synthetic-lockbox-secret'
FSID = '00000000-0000-0000-0000-000000000012'


def volume(path, secret='old-secret', cluster='ceph', osd_id='12', fsid=FSID, with_lockbox=True):
    tags = ','.join([
        'ceph.osd_id=%s' % osd_id,
        'ceph.osd_fsid=%s' % fsid,
        'ceph.cluster_name=%s' % cluster,
        'ceph.type=block',
    ])
    if with_lockbox:
        tags += ',%s=%s' % (cephx_lockbox_secret_tag, secret)
    return lvm.Volume(lv_name=path.rsplit('/', 1)[-1], lv_path=path, lv_tags=tags)


class TestSetLockboxSecret(object):

    def test_rejects_empty_secret(self):
        with pytest.raises(RuntimeError):
            lockbox.set_lockbox_secret('12', FSID, '')

    def test_rejects_secret_with_comma(self):
        with pytest.raises(RuntimeError):
            lockbox.set_lockbox_secret('12', FSID, 'abc,def')

    @patch('ceph_volume.devices.lvm.lockbox.get_lvs', return_value=[])
    def test_missing_lv(self, _get_lvs):
        with pytest.raises(RuntimeError):
            lockbox.set_lockbox_secret('12', FSID, SECRET)

    @patch('ceph_volume.devices.lvm.lockbox.get_lvs')
    def test_ignores_lv_without_lockbox_tag(self, get_lvs):
        get_lvs.return_value = [volume('/dev/vg/block', with_lockbox=False)]
        with pytest.raises(RuntimeError):
            lockbox.set_lockbox_secret('12', FSID, SECRET)

    @patch('ceph_volume.devices.lvm.lockbox.get_lvs')
    def test_inconsistent_cluster_name(self, get_lvs):
        get_lvs.return_value = [
            volume('/dev/vg/block', cluster='ceph'),
            volume('/dev/vg/db', cluster='other'),
        ]
        with pytest.raises(RuntimeError):
            lockbox.set_lockbox_secret('12', FSID, SECRET)

    @patch('ceph_volume.devices.lvm.lockbox.process.call', return_value=([], [], 0))
    @patch('ceph_volume.devices.lvm.lockbox.get_lvs')
    def test_updates_each_tagged_lv_once(self, get_lvs, call):
        current = volume('/dev/vg/block')
        already = volume('/dev/vg/wal', secret=SECRET)
        plain = volume('/dev/vg/db', with_lockbox=False)
        get_lvs.return_value = [current, already, plain]
        with patch('ceph_volume.devices.lvm.lockbox.os.path.exists', return_value=False):
            report = lockbox.set_lockbox_secret('12', FSID, SECRET)

        call.assert_called_once_with(
            [
                'lvchange',
                '--deltag', '%s=old-secret' % cephx_lockbox_secret_tag,
                '--addtag', '%s=%s' % (cephx_lockbox_secret_tag, SECRET),
                '/dev/vg/block',
            ],
            run_on_host=True,
            obfuscate=['--deltag', '--addtag'],
        )
        assert report['lvs'] == ['/dev/vg/block', '/dev/vg/wal']
        assert report['lockbox_keyring'] is None
        assert current.tags[cephx_lockbox_secret_tag] == SECRET

    @patch('ceph_volume.devices.lvm.lockbox.process.call', return_value=([], [], 1))
    @patch('ceph_volume.devices.lvm.lockbox.get_lvs')
    def test_lvchange_failure_does_not_touch_keyring(self, get_lvs, _call, tmp_path):
        get_lvs.return_value = [volume('/dev/vg/block')]
        keyring = tmp_path / 'lockbox.keyring'
        keyring.write_text('old\n')
        with patch.object(lockbox, 'lockbox_keyring_path', return_value=str(keyring)):
            with pytest.raises(RuntimeError):
                lockbox.set_lockbox_secret('12', FSID, SECRET)
        assert keyring.read_text() == 'old\n'

    @patch('ceph_volume.devices.lvm.lockbox.system.chown')
    @patch('ceph_volume.devices.lvm.lockbox.process.call', return_value=([], [], 0))
    @patch('ceph_volume.devices.lvm.lockbox.get_lvs')
    def test_rewrites_existing_keyring(self, get_lvs, _call, chown, tmp_path):
        get_lvs.return_value = [volume('/dev/vg/block')]
        keyring = tmp_path / 'lockbox.keyring'
        keyring.write_text('old\n')
        with patch.object(lockbox, 'lockbox_keyring_path', return_value=str(keyring)):
            report = lockbox.set_lockbox_secret('12', FSID, SECRET)
        expected = '[client.osd-lockbox.%s]\n\tkey = %s\n' % (FSID, SECRET)
        assert keyring.read_text() == expected
        assert report['lockbox_keyring'] == str(keyring)
        assert oct(keyring.stat().st_mode & 0o777) == '0o600'
        chown.assert_called_once_with(str(keyring), recursive=False)

    @patch('ceph_volume.devices.lvm.lockbox.system.chown')
    @patch('ceph_volume.devices.lvm.lockbox.process.call', return_value=([], [], 0))
    @patch('ceph_volume.devices.lvm.lockbox.get_lvs')
    def test_keyring_already_current(self, get_lvs, call, chown, tmp_path):
        get_lvs.return_value = [volume('/dev/vg/block', secret=SECRET)]
        keyring = tmp_path / 'lockbox.keyring'
        contents = '[client.osd-lockbox.%s]\n\tkey = %s\n' % (FSID, SECRET)
        keyring.write_text(contents)
        with patch.object(lockbox, 'lockbox_keyring_path', return_value=str(keyring)):
            report = lockbox.set_lockbox_secret('12', FSID, SECRET)
        assert keyring.read_text() == contents
        assert report['lockbox_keyring'] == str(keyring)
        call.assert_not_called()
        chown.assert_not_called()


class TestSetLockboxSecretCli(object):

    @patch.dict('os.environ', {'CEPH_VOLUME_SKIP_NEEDS_ROOT': '1'})
    @patch('ceph_volume.devices.lvm.lockbox.set_lockbox_secret')
    def test_reads_stdin_and_prints_json(self, set_secret, capsys, monkeypatch):
        set_secret.return_value = {
            'osd_id': '12',
            'osd_fsid': FSID,
            'lvs': ['/dev/vg/block'],
            'lockbox_keyring': None,
        }
        monkeypatch.setattr(sys, 'stdin', io.StringIO(SECRET + '\n'))
        lockbox.SetLockboxSecret(['--osd-id', '12', '--osd-fsid', FSID]).main()
        set_secret.assert_called_once_with('12', FSID, SECRET)
        report = json.loads(capsys.readouterr().out)
        assert report['lvs'] == ['/dev/vg/block']
        assert report['lockbox_keyring'] is None
