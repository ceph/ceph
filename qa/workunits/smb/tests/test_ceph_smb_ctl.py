import pathlib
import time

import pytest

import cephutil
import smbutil

CEPH_SMB_CTL = 'ceph-smb-ctl'


class RemoteCtlBase:
    def _rcontrol(self, smb_cfg, args, **kwargs):
        raise NotImplementedError()

    def test_get_info(self, smb_cfg):
        jres = self._rcontrol(smb_cfg, ['info'], load_json=True)
        assert jres.returncode == 0
        assert jres.obj
        assert 'samba_info' in jres.obj
        assert 'version' in jres.obj['samba_info']
        assert 'clustered' in jres.obj['samba_info']

    def test_status_empty(self, smb_cfg):
        jres = self._rcontrol(smb_cfg, ['status'], load_json=True)
        assert jres.returncode == 0
        assert jres.obj
        assert 'server_timestamp' in jres.obj
        assert 'sessions' in jres.obj
        assert 'tree_connections' in jres.obj
        assert not jres.obj['sessions']
        assert not jres.obj['tree_connections']

    def test_status_connected(self, smb_cfg):
        share_name = smbutil.get_shares(smb_cfg)[0]['name']
        with smbutil.connection(smb_cfg, share_name) as sharep:
            sharep.listdir()  # trigger a tree connect in client lib
            time.sleep(0.2)
            jres = self._rcontrol(smb_cfg, ['status'], load_json=True)
        assert jres.returncode == 0
        assert jres.obj
        assert 'server_timestamp' in jres.obj
        assert 'sessions' in jres.obj
        assert 'tree_connections' in jres.obj
        assert len(jres.obj['sessions']) == 1
        assert len(jres.obj['tree_connections']) == 1
        assert (
            jres.obj['sessions'][0]['session_id']
            == jres.obj['tree_connections'][0]['session_id']
        )

    def test_config_dump_samba(self, smb_cfg):
        share_name = smbutil.get_shares(smb_cfg)[0]['name']
        res = self._rcontrol(
            smb_cfg, ['config-dump', 'samba'], capture_output=True, text=True
        )
        assert res.returncode == 0
        assert f'[{share_name}]' in res.stdout

    def test_config_dump_sambacc(self, smb_cfg):
        jres = self._rcontrol(
            smb_cfg, ['config-dump', 'sambacc'], load_json=True
        )
        assert jres.returncode == 0
        assert jres.obj

    def test_config_dump_cmp_hash(self, smb_cfg):
        res = self._rcontrol(
            smb_cfg,
            ['config-dump', '--sha256', 'samba'],
            capture_output=True,
            text=True,
        )
        assert res.returncode == 0
        lines = res.stdout.splitlines()
        digest_lines = [line for line in lines if 'digest =' in line]
        assert len(digest_lines) == 1
        parts = [s.strip() for s in digest_lines[0].split('=')]
        alg, digest = parts[-1].split(':', 1)
        assert alg == 'sha256'

        jres = self._rcontrol(
            smb_cfg,
            ['config-summary', '--sha256', 'samba'],
            load_json=True,
        )
        assert jres.returncode == 0
        assert 'digest' in jres.obj
        assert 'config_digest' in jres.obj['digest']
        assert jres.obj['digest']['config_digest'] == digest

    def test_config_shares_list(self, smb_cfg):
        jres = self._rcontrol(
            smb_cfg,
            ['config-shares-list', 'samba'],
            load_json=True,
        )
        assert jres.returncode == 0
        share_names = [v['name'] for v in smbutil.get_shares(smb_cfg)]
        assert jres.obj
        for share_name in share_names:
            assert share_name in jres.obj, f"missing {share_name}"

        # the shares list in sambacc should always map exactly to the
        # shares list in samba itself
        prev_names = jres.obj
        jres = self._rcontrol(
            smb_cfg,
            ['config-shares-list', 'sambacc'],
            load_json=True,
        )
        assert jres.returncode == 0
        assert sorted(jres.obj) == sorted(prev_names)

    def test_debug_get_set(self, smb_cfg):
        jres = self._rcontrol(
            smb_cfg,
            ['get-debug-level', 'smb'],
            load_json=True,
        )
        assert jres.returncode == 0, "get-debug-level smb failed"
        assert "debug_level" in jres.obj
        orig_debug_level = jres.obj["debug_level"]

        jres = self._rcontrol(
            smb_cfg,
            ['set-debug-level', 'smb', "10"],
            load_json=True,
        )
        assert jres.returncode == 0, "set-debug-level smb 10 failed"

        jres = self._rcontrol(
            smb_cfg,
            ['get-debug-level', 'smb'],
            load_json=True,
        )
        assert jres.returncode == 0, "get-debug-level smb failed"
        assert "debug_level" in jres.obj
        assert jres.obj["debug_level"] == "10"

        jres = self._rcontrol(
            smb_cfg,
            ['set-debug-level', 'smb', orig_debug_level],
            load_json=True,
        )
        assert jres.returncode == 0, "set-debug-level smb orig level failed"


@pytest.mark.ceph_smb_ctl_local
class TestCephSMBCtlLocal(RemoteCtlBase):
    def _rcontrol(self, smb_cfg, args, **kwargs):
        return cephutil.cephadm_shell_cmd(
            smb_cfg,
            [CEPH_SMB_CTL] + args,
            **kwargs,
        )


@pytest.mark.ceph_smb_ctl_remote
class TestCephSMBCtlRemote(RemoteCtlBase):
    def _rcontrol(self, smb_cfg, args, **kwargs):
        grpc_host = f"{smb_cfg.server.ip_address}:54445"
        ca_dir = pathlib.Path(smb_cfg.testdir) / 'ca'
        ca_mnt = pathlib.Path('/tls')
        assert (ca_dir / 'remote-control-client.crt').is_file()
        assert (ca_dir / 'remote-control-client.key').is_file()
        assert (ca_dir / 'rcroot.crt').is_file()
        _args = [
            CEPH_SMB_CTL,
            f'--address={grpc_host}',
            f"--tls-cert={ca_mnt}/remote-control-client.crt",
            f"--tls-key={ca_mnt}/remote-control-client.key",
            f"--tls-ca-cert={ca_mnt}/rcroot.crt",
        ]
        return cephutil.cephadm_shell_cmd(
            smb_cfg,
            _args + args,
            volumes=[f'{ca_dir}:{ca_mnt}:ro'],
            **kwargs,
        )
