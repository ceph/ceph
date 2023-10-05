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


def test_deploy_nfs_container(cephadm_fs, monkeypatch):
    _call = mock.MagicMock(return_value=('', '', 0))
    monkeypatch.setattr('cephadmlib.container_types.call', _call)
    _firewalld = mock.MagicMock()
    _firewalld().external_ports.get.return_value = []
    monkeypatch.setattr('cephadm.Firewalld', _firewalld)
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
    _call = mock.MagicMock(return_value=('', '', 0))
    monkeypatch.setattr('cephadmlib.container_types.call', _call)
    _call_throws = mock.MagicMock(return_value=0)
    monkeypatch.setattr(
        'cephadmlib.container_types.call_throws', _call_throws
    )
    _firewalld = mock.MagicMock()
    _firewalld().external_ports.get.return_value = []
    monkeypatch.setattr('cephadm.Firewalld', _firewalld)
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
