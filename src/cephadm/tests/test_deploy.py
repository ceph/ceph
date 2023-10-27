import os
import pathlib
import unittest
from unittest import mock

from .fixtures import (
    cephadm_fs,
    import_cephadm,
    mock_podman,
    with_cephadm_ctx,
)


_cephadm = import_cephadm()


def _common_mp(monkeypatch):
    mocks = {}
    _call = mock.MagicMock(return_value=('', '', 0))
    monkeypatch.setattr('cephadmlib.container_types.call', _call)
    mocks['call'] = _call
    _call_throws = mock.MagicMock(return_value=0)
    monkeypatch.setattr(
        'cephadmlib.container_types.call_throws', _call_throws
    )
    mocks['call_throws'] = _call_throws
    _firewalld = mock.MagicMock()
    _firewalld().external_ports.get.return_value = []
    monkeypatch.setattr('cephadm.Firewalld', _firewalld)
    mocks['Firewalld'] = _firewalld
    _extract_uid_gid = mock.MagicMock()
    _extract_uid_gid.return_value = (8765, 8765)
    monkeypatch.setattr('cephadm.extract_uid_gid', _extract_uid_gid)
    mocks['extract_uid_gid'] = _extract_uid_gid
    _install_sysctl = mock.MagicMock()
    monkeypatch.setattr('cephadm.install_sysctl', _install_sysctl)
    mocks['install_sysctl'] = _install_sysctl
    return mocks


def test_deploy_nfs_container(cephadm_fs, monkeypatch):
    mocks = _common_mp(monkeypatch)
    _firewalld = mocks['Firewalld']
    fsid = 'b01dbeef-701d-9abe-0000-e1e5a47004a7'
    with with_cephadm_ctx([]) as ctx:
        ctx.container_engine = mock_podman()
        ctx.fsid = fsid
        ctx.name = 'nfs.fun'
        ctx.image = 'quay.io/ceph/ceph:latest'
        ctx.reconfig = False
        ctx.config_blobs = {
            'pool': 'foo',
            'files': {
                'ganesha.conf': 'FAKE',
            },
            'config': 'BALONEY',
            'keyring': 'BUNKUS',
        }
        _cephadm._common_deploy(ctx)

    with open(f'/var/lib/ceph/{fsid}/nfs.fun/unit.run') as f:
        runfile_lines = f.read().splitlines()
    assert 'podman' in runfile_lines[-1]
    assert runfile_lines[-1].endswith('quay.io/ceph/ceph:latest -F -L STDERR')
    _firewalld().open_ports.assert_called_with([2049])
    with open(f'/var/lib/ceph/{fsid}/nfs.fun/config') as f:
        assert f.read() == 'BALONEY'
    with open(f'/var/lib/ceph/{fsid}/nfs.fun/keyring') as f:
        assert f.read() == 'BUNKUS'
    with open(f'/var/lib/ceph/{fsid}/nfs.fun/etc/ganesha/ganesha.conf') as f:
        assert f.read() == 'FAKE'


def test_deploy_snmp_container(cephadm_fs, monkeypatch):
    mocks = _common_mp(monkeypatch)
    _firewalld = mocks['Firewalld']
    fsid = 'b01dbeef-701d-9abe-0000-e1e5a47004a7'
    with with_cephadm_ctx([]) as ctx:
        ctx.container_engine = mock_podman()
        ctx.fsid = fsid
        ctx.name = 'snmp-gateway.sunmop'
        ctx.image = 'quay.io/aaabbb/snmp:latest'
        ctx.reconfig = False
        ctx.config_blobs = {
            'destination': '192.168.100.10:8899',
            'config': 'XXXXXXX',
            'keyring': 'YYYYYY',
        }
        _cephadm._common_deploy(ctx)

    with open(f'/var/lib/ceph/{fsid}/snmp-gateway.sunmop/unit.run') as f:
        runfile_lines = f.read().splitlines()
    assert 'podman' in runfile_lines[-1]
    assert runfile_lines[-1].endswith(
        'quay.io/aaabbb/snmp:latest --web.listen-address=:9464 --snmp.destination=192.168.100.10:8899 --snmp.version=V2c --log.level=info --snmp.trap-description-template=/etc/snmp_notifier/description-template.tpl'
    )
    _firewalld().open_ports.assert_not_called()
    basedir = pathlib.Path(f'/var/lib/ceph/{fsid}/snmp-gateway.sunmop')
    assert basedir.is_dir()
    assert not (basedir / 'config').exists()
    assert not (basedir / 'keyring').exists()


def test_deploy_keepalived_container(cephadm_fs, monkeypatch):
    mocks = _common_mp(monkeypatch)
    _firewalld = mocks['Firewalld']
    _install_sysctl = mocks['install_sysctl']
    fsid = 'b01dbeef-701d-9abe-0000-e1e5a47004a7'
    with with_cephadm_ctx([]) as ctx:
        ctx.container_engine = mock_podman()
        ctx.fsid = fsid
        ctx.name = 'keepalived.uiop'
        ctx.image = 'quay.io/eeranimated/keepalived:latest'
        ctx.reconfig = False
        ctx.config_blobs = {
            'destination': '192.168.100.10:8899',
            'config': 'XXXXXXX',
            'keyring': 'YYYYYY',
            'files': {
                'keepalived.conf': 'neversayneveragain',
            },
        }
        _cephadm._common_deploy(ctx)

    basedir = pathlib.Path(f'/var/lib/ceph/{fsid}/keepalived.uiop')
    assert basedir.is_dir()
    with open(basedir / 'unit.run') as f:
        runfile_lines = f.read().splitlines()
    assert 'podman' in runfile_lines[-1]
    assert runfile_lines[-1].endswith('quay.io/eeranimated/keepalived:latest')
    _firewalld().open_ports.assert_not_called()
    assert not (basedir / 'config').exists()
    assert not (basedir / 'keyring').exists()
    with open(basedir / 'keepalived.conf') as f:
        assert f.read() == 'neversayneveragain'
    with open(basedir / 'keepalived.conf') as f:
        assert f.read() == 'neversayneveragain'
        si = os.fstat(f.fileno())
        assert (si.st_uid, si.st_gid) == (8765, 8765)
    assert (basedir / 'keepalived').is_dir()
    si = (basedir / 'keepalived').stat()
    assert (si.st_uid, si.st_gid) == (8765, 8765)
    assert _install_sysctl.call_count == 1
    assert len(_install_sysctl.call_args[0][-1].get_sysctl_settings()) > 1


def test_deploy_haproxy_container(cephadm_fs, monkeypatch):
    mocks = _common_mp(monkeypatch)
    _firewalld = mocks['Firewalld']
    _install_sysctl = mocks['install_sysctl']
    fsid = 'b01dbeef-701d-9abe-0000-e1e5a47004a7'
    with with_cephadm_ctx([]) as ctx:
        ctx.container_engine = mock_podman()
        ctx.fsid = fsid
        ctx.name = 'haproxy.yyz'
        ctx.image = 'quay.io/lfeuwbo/haproxy:latest'
        ctx.reconfig = False
        ctx.config_blobs = {
            'config': 'XXXXXXX',
            'keyring': 'YYYYYY',
            'files': {
                'haproxy.cfg': 'bifrost',
            },
        }
        _cephadm._common_deploy(ctx)

    basedir = pathlib.Path(f'/var/lib/ceph/{fsid}/haproxy.yyz')
    assert basedir.is_dir()
    with open(basedir / 'unit.run') as f:
        runfile_lines = f.read().splitlines()
    assert 'podman' in runfile_lines[-1]
    assert runfile_lines[-1].endswith(
        'quay.io/lfeuwbo/haproxy:latest haproxy -f /var/lib/haproxy/haproxy.cfg'
    )
    _firewalld().open_ports.assert_not_called()
    assert not (basedir / 'config').exists()
    assert not (basedir / 'keyring').exists()
    assert (basedir / 'haproxy').is_dir()
    si = (basedir / 'haproxy').stat()
    assert (si.st_uid, si.st_gid) == (8765, 8765)
    with open(basedir / 'haproxy/haproxy.cfg') as f:
        assert f.read() == 'bifrost'
        si = os.fstat(f.fileno())
        assert (si.st_uid, si.st_gid) == (8765, 8765)
    assert _install_sysctl.call_count == 1
    assert len(_install_sysctl.call_args[0][-1].get_sysctl_settings()) > 1


def test_deploy_iscsi_container(cephadm_fs, monkeypatch):
    mocks = _common_mp(monkeypatch)
    _firewalld = mocks['Firewalld']
    fsid = 'b01dbeef-701d-9abe-0000-e1e5a47004a7'
    with with_cephadm_ctx([]) as ctx:
        ctx.container_engine = mock_podman()
        ctx.fsid = fsid
        ctx.name = 'iscsi.wuzzy'
        ctx.image = 'quay.io/ayeaye/iscsi:latest'
        ctx.reconfig = False
        ctx.config_blobs = {
            'config': 'XXXXXXX',
            'keyring': 'YYYYYY',
            'files': {
                'iscsi-gateway.cfg': 'portal',
            },
        }
        _cephadm._common_deploy(ctx)

    basedir = pathlib.Path(f'/var/lib/ceph/{fsid}/iscsi.wuzzy')
    assert basedir.is_dir()
    with open(basedir / 'unit.run') as f:
        runfile_lines = f.read().splitlines()
    assert 'podman' in runfile_lines[-1]
    assert runfile_lines[-1].endswith('quay.io/ayeaye/iscsi:latest')
    _firewalld().open_ports.assert_not_called()
    with open(basedir / 'config') as f:
        assert f.read() == 'XXXXXXX'
    with open(basedir / 'keyring') as f:
        assert f.read() == 'YYYYYY'
    assert (basedir / 'configfs').is_dir()
    si = (basedir / 'configfs').stat()
    assert (si.st_uid, si.st_gid) == (8765, 8765)
    with open(basedir / 'iscsi-gateway.cfg') as f:
        assert f.read() == 'portal'
        si = os.fstat(f.fileno())
        assert (si.st_uid, si.st_gid) == (8765, 8765)


def test_deploy_nvmeof_container(cephadm_fs, monkeypatch):
    mocks = _common_mp(monkeypatch)
    _firewalld = mocks['Firewalld']
    fsid = 'b01dbeef-701d-9abe-0000-e1e5a47004a7'
    with with_cephadm_ctx([]) as ctx:
        ctx.container_engine = mock_podman()
        ctx.fsid = fsid
        ctx.name = 'nvmeof.andu'
        ctx.image = 'quay.io/ownf/nmve:latest'
        ctx.reconfig = False
        ctx.config_blobs = {
            'config': 'XXXXXXX',
            'keyring': 'YYYYYY',
            'files': {
                'ceph-nvmeof.conf': 'icantbeliveitsnotiscsi',
            },
        }
        _cephadm._common_deploy(ctx)

    basedir = pathlib.Path(f'/var/lib/ceph/{fsid}/nvmeof.andu')
    assert basedir.is_dir()
    with open(basedir / 'unit.run') as f:
        runfile_lines = f.read().splitlines()
    assert 'podman' in runfile_lines[-1]
    assert runfile_lines[-1].endswith('quay.io/ownf/nmve:latest')
    _firewalld().open_ports.assert_not_called()
    with open(basedir / 'config') as f:
        assert f.read() == 'XXXXXXX'
    with open(basedir / 'keyring') as f:
        assert f.read() == 'YYYYYY'
    assert (basedir / 'configfs').is_dir()
    si = (basedir / 'configfs').stat()
    assert (si.st_uid, si.st_gid) == (167, 167)
    with open(basedir / 'ceph-nvmeof.conf') as f:
        assert f.read() == 'icantbeliveitsnotiscsi'
        si = os.fstat(f.fileno())
        assert (si.st_uid, si.st_gid) == (167, 167)


def test_deploy_a_monitoring_container(cephadm_fs, monkeypatch):
    mocks = _common_mp(monkeypatch)
    _firewalld = mocks['Firewalld']
    _get_ip_addresses = mock.MagicMock(return_value=(['10.10.10.10'], []))
    monkeypatch.setattr('cephadm.get_ip_addresses', _get_ip_addresses)
    fsid = 'b01dbeef-701d-9abe-0000-e1e5a47004a7'
    with with_cephadm_ctx([]) as ctx:
        ctx.container_engine = mock_podman()
        ctx.fsid = fsid
        ctx.name = 'prometheus.fire'
        ctx.image = 'quay.io/titans/prometheus:latest'
        ctx.reconfig = False
        ctx.config_blobs = {
            'config': 'XXXXXXX',
            'keyring': 'YYYYYY',
            'files': {
                'prometheus.yml': 'bettercallherc',
            },
        }
        _cephadm._common_deploy(ctx)

    basedir = pathlib.Path(f'/var/lib/ceph/{fsid}/prometheus.fire')
    assert basedir.is_dir()
    with open(basedir / 'unit.run') as f:
        runfile_lines = f.read().splitlines()
    assert 'podman' in runfile_lines[-1]
    assert runfile_lines[-1].endswith(
        'quay.io/titans/prometheus:latest --config.file=/etc/prometheus/prometheus.yml --storage.tsdb.path=/prometheus --web.listen-address=:9095 --storage.tsdb.retention.time=15d --storage.tsdb.retention.size=0 --web.external-url=http://10.10.10.10:9095'
    )
    _firewalld().open_ports.assert_not_called()
    assert not (basedir / 'config').exists()
    assert not (basedir / 'keyring').exists()
    with open(basedir / 'etc/prometheus/prometheus.yml') as f:
        assert f.read() == 'bettercallherc'
        si = os.fstat(f.fileno())
        assert (si.st_uid, si.st_gid) == (8765, 8765)


def test_deploy_a_tracing_container(cephadm_fs, monkeypatch):
    mocks = _common_mp(monkeypatch)
    _firewalld = mocks['Firewalld']
    fsid = 'b01dbeef-701d-9abe-0000-e1e5a47004a7'
    with with_cephadm_ctx([]) as ctx:
        ctx.container_engine = mock_podman()
        ctx.fsid = fsid
        ctx.name = 'elasticsearch.band'
        ctx.image = 'quay.io/rubber/elasticsearch:latest'
        ctx.reconfig = False
        ctx.config_blobs = {
            'config': 'XXXXXXX',
            'keyring': 'YYYYYY',
            'files': {
                'prometheus.yml': 'bettercallherc',
            },
        }
        _cephadm._common_deploy(ctx)

    basedir = pathlib.Path(f'/var/lib/ceph/{fsid}/elasticsearch.band')
    assert basedir.is_dir()
    with open(basedir / 'unit.run') as f:
        runfile_lines = f.read().splitlines()
    assert 'podman' in runfile_lines[-1]
    assert runfile_lines[-1].endswith('quay.io/rubber/elasticsearch:latest')
    _firewalld().open_ports.assert_not_called()
    assert not (basedir / 'config').exists()
    assert not (basedir / 'keyring').exists()


def test_deploy_ceph_mgr_container(cephadm_fs, monkeypatch):
    mocks = _common_mp(monkeypatch)
    _firewalld = mocks['Firewalld']
    _make_var_run = mock.MagicMock()
    monkeypatch.setattr('cephadm.make_var_run', _make_var_run)
    fsid = 'b01dbeef-701d-9abe-0000-e1e5a47004a7'
    with with_cephadm_ctx([]) as ctx:
        ctx.container_engine = mock_podman()
        ctx.fsid = fsid
        ctx.name = 'mgr.foo'
        ctx.image = 'quay.io/ceph/ceph:latest'
        ctx.reconfig = False
        ctx.allow_ptrace = False
        ctx.osd_fsid = '00000000-0000-0000-0000-000000000000'
        ctx.config_blobs = {
            'config': 'XXXXXXX',
            'keyring': 'YYYYYY',
        }
        _cephadm._common_deploy(ctx)

    basedir = pathlib.Path(f'/var/lib/ceph/{fsid}/mgr.foo')
    assert basedir.is_dir()
    with open(basedir / 'unit.run') as f:
        runfile_lines = f.read().splitlines()
    assert 'podman' in runfile_lines[-1]
    assert runfile_lines[-1].endswith(
        'quay.io/ceph/ceph:latest -n mgr.foo -f --setuser ceph --setgroup ceph --default-log-to-file=false --default-log-to-journald=true --default-log-to-stderr=false'
    )
    _firewalld().open_ports.assert_not_called()
    with open(basedir / 'config') as f:
        assert f.read() == 'XXXXXXX'
    with open(basedir / 'keyring') as f:
        assert f.read() == 'YYYYYY'
    assert _make_var_run.call_count == 1
    assert _make_var_run.call_args[0][2] == 8765
    assert _make_var_run.call_args[0][3] == 8765


def test_deploy_ceph_exporter_container(cephadm_fs, monkeypatch):
    mocks = _common_mp(monkeypatch)
    _firewalld = mocks['Firewalld']
    _get_ip_addresses = mock.MagicMock(return_value=(['10.10.10.10'], []))
    monkeypatch.setattr('cephadm.get_ip_addresses', _get_ip_addresses)
    _make_var_run = mock.MagicMock()
    monkeypatch.setattr('cephadm.make_var_run', _make_var_run)
    fsid = 'b01dbeef-701d-9abe-0000-e1e5a47004a7'
    with with_cephadm_ctx([]) as ctx:
        ctx.container_engine = mock_podman()
        ctx.fsid = fsid
        ctx.name = 'ceph-exporter.zaq'
        ctx.image = 'quay.io/ceph/ceph:latest'
        ctx.reconfig = False
        ctx.allow_ptrace = False
        ctx.osd_fsid = '00000000-0000-0000-0000-000000000000'
        ctx.config_blobs = {
            'config': 'XXXXXXX',
            'keyring': 'YYYYYY',
            'prio-limit': 12,
        }

        # ceph-exporter is weird and special. it requires the "sock dir"
        # to already exist. that dir defaults to /var/run/ceph
        vrc = pathlib.Path('/var/run/ceph')
        (vrc / fsid).mkdir(parents=True)

        _cephadm._common_deploy(ctx)

    basedir = pathlib.Path(f'/var/lib/ceph/{fsid}/ceph-exporter.zaq')
    assert basedir.is_dir()
    with open(basedir / 'unit.run') as f:
        runfile_lines = f.read().splitlines()
    assert 'podman' in runfile_lines[-1]
    assert runfile_lines[-1].endswith(
        'quay.io/ceph/ceph:latest -n client.ceph-exporter.zaq -f --sock-dir=/var/run/ceph/ --addrs=0.0.0.0 --port=9926 --prio-limit=12 --stats-period=5'
    )
    assert '--entrypoint /usr/bin/ceph-exporter' in runfile_lines[-1]
    _firewalld().open_ports.assert_not_called()
    with open(basedir / 'config') as f:
        assert f.read() == 'XXXXXXX'
    with open(basedir / 'keyring') as f:
        assert f.read() == 'YYYYYY'
