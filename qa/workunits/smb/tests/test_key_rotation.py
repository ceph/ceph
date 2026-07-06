import time

import pytest

import cephutil
import smbutil


def _smb_daemon_names(smb_cfg):
    jres = cephutil.cephadm_shell_cmd(
        smb_cfg,
        ['ceph', 'orch', 'ps', '--daemon-type', 'smb'],
        load_json=True,
    )
    assert jres.returncode == 0
    assert jres.obj
    return [d['daemon_name'] for d in jres.obj]


def _auth_entity(daemon_name):
    _, daemon_id = daemon_name.split('.', 1)
    return f'client.smb.config.{daemon_id}'


def _get_key(smb_cfg, entity):
    res = cephutil.cephadm_shell_cmd(
        smb_cfg,
        ['ceph', 'auth', 'get-key', entity],
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0, f'failed to get key for {entity}: {res.stderr}'
    return res.stdout.strip()


def _get_caps(smb_cfg, entity):
    res = cephutil.cephadm_shell_cmd(
        smb_cfg,
        ['ceph', 'auth', 'get', entity],
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0, f'failed to get auth for {entity}: {res.stderr}'
    return sorted(ln.strip() for ln in res.stdout.splitlines() if 'caps' in ln)


def _rotate_key(smb_cfg, daemon_name, timeout=300, interval=5):
    entity = _auth_entity(daemon_name)
    before = _get_key(smb_cfg, entity)

    res = cephutil.cephadm_shell_cmd(
        smb_cfg,
        ['ceph', 'orch', 'daemon', 'rotate-key', daemon_name],
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0, (
        f'rotate-key failed for {daemon_name}: {res.stderr}'
    )

    after = before
    waited = 0
    while after == before and waited < timeout:
        time.sleep(interval)
        waited += interval
        after = _get_key(smb_cfg, entity)

    assert after != before, (
        f'key for {entity} did not rotate within {timeout}s'
    )
    return before, after


@pytest.mark.key_rotation
class TestSMBKeyRotation:
    def test_rotate_all_daemons(self, smb_cfg):
        """Rotate the cephx key for every smb daemon and confirm the
        cluster keeps serving shares afterwards."""
        daemon_names = _smb_daemon_names(smb_cfg)
        assert daemon_names, 'no smb daemons found'

        for daemon_name in daemon_names:
            _rotate_key(smb_cfg, daemon_name)

        share_name = smbutil.get_shares(smb_cfg)[0]['name']
        with smbutil.connection(smb_cfg, share_name) as sharep:
            sharep.listdir()

    def test_rotate_preserves_caps(self, smb_cfg):
        """Rotating a daemon's key should not alter its cephx caps."""
        daemon_name = _smb_daemon_names(smb_cfg)[0]
        entity = _auth_entity(daemon_name)

        before_caps = _get_caps(smb_cfg, entity)
        _rotate_key(smb_cfg, daemon_name)
        after_caps = _get_caps(smb_cfg, entity)

        assert before_caps == after_caps
