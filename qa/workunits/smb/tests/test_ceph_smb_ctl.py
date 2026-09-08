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


def _get_obj(value):
    return value.to_simplified()


@pytest.mark.ceph_smb_ctl_remote
class TestCephSMBCtlAPI:
    def _client(self, smb_cfg):
        # alter paths to include python-common
        import sys

        curr = pathlib.Path('.').absolute()
        while curr.parent != pathlib.Path('/'):
            pcomm = curr / 'src/python-common'
            if pcomm.is_dir():
                sys.path.append(str(pcomm))
                break
            curr = curr.parent

        # import the needed packges
        try:
            import ceph.smb.ctl.client as rclient
            import ceph.smb.ctl.config as rconfig
        except ImportError:
            pytest.skip('failed to import ceph.smb.ctl.client OR dependency')

        # set up a grpc client w/in the test
        grpc_host = f"{smb_cfg.server.ip_address}:54445"
        ca_dir = pathlib.Path(smb_cfg.testdir) / 'ca'
        tls_cert = ca_dir / 'remote-control-client.crt'
        tls_key = ca_dir / 'remote-control-client.key'
        tls_ca_cert = ca_dir / 'rcroot.crt'
        assert tls_cert.is_file()
        assert tls_key.is_file()
        assert tls_ca_cert.is_file()

        cc = rconfig.Config(
            address=grpc_host,
            channel_type=rconfig.ChannelType.SECURE,
            tls_cert=rconfig.TLSPath.create(tls_cert),
            tls_key=rconfig.TLSPath.create(tls_key),
            tls_ca_cert=rconfig.TLSPath.create(tls_ca_cert),
        )
        return rclient.Client(cc)

    def test_get_info(self, smb_cfg):
        obj = _get_obj(self._client(smb_cfg).info())
        assert 'samba_info' in obj
        assert 'version' in obj['samba_info']
        assert 'clustered' in obj['samba_info']

    def test_status_empty(self, smb_cfg):
        obj = _get_obj(self._client(smb_cfg).status())
        assert 'server_timestamp' in obj
        assert 'sessions' in obj
        assert 'tree_connections' in obj
        assert not obj['sessions']
        assert not obj['tree_connections']

    def test_status_connected(self, smb_cfg):
        share_name = smbutil.get_shares(smb_cfg)[0]['name']
        with smbutil.connection(smb_cfg, share_name) as sharep:
            sharep.listdir()  # trigger a tree connect in client lib
            time.sleep(0.2)
            obj = _get_obj(self._client(smb_cfg).status())
        assert obj
        assert 'server_timestamp' in obj
        assert 'sessions' in obj
        assert 'tree_connections' in obj
        assert len(obj['sessions']) == 1
        assert len(obj['tree_connections']) == 1
        assert (
            obj['sessions'][0]['session_id']
            == obj['tree_connections'][0]['session_id']
        )

    def test_config_summary_change_poll(self, smb_cfg):
        source = 'CONFIG_FOR_SAMBA'
        obj = _get_obj(self._client(smb_cfg).config_summary(source))
        assert 'digest' in obj
        assert 'config_digest' in obj['digest']
        prev_digest = obj['digest']['config_digest']
        time.sleep(0.2)

        # no change
        obj = _get_obj(self._client(smb_cfg).config_summary(source))
        assert prev_digest == obj['digest']['config_digest']

        # change
        share_cfg = smbutil.get_shares(smb_cfg)[0]
        orig_comment = share_cfg.get('comment', '')
        share_cfg['comment'] = 'Foo bar baz asdf bar baz foo'
        assert share_cfg['comment'] != orig_comment
        smbutil.apply_share_config(smb_cfg, share_cfg, immediate=True)

        changed = False
        for _ in range(0, 150):
            time.sleep(0.5)
            obj = _get_obj(self._client(smb_cfg).config_summary(source))
            changed = prev_digest != obj['digest']['config_digest']
            if changed:
                break

        assert changed, "config digest never changed"
