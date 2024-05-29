from unittest import mock

import pytest

from .fixtures import import_cephadm

from cephadmlib import daemon_form
from cephadmlib import daemon_identity
from cephadmlib import daemons

_cephadm = import_cephadm()


@pytest.mark.parametrize(
    'daemon_type,cls',
    [
        ('container', _cephadm.CustomContainer),
        ('grafana', _cephadm.Monitoring),
        ('iscsi', _cephadm.CephIscsi),
        ('jaeger-agent', _cephadm.Tracing),
        ('jaeger-query', _cephadm.Tracing),
        ('mds', _cephadm.Ceph),
        ('mon', _cephadm.Ceph),
        ('nfs', _cephadm.NFSGanesha),
        ('nvmeof', _cephadm.CephNvmeof),
        ('osd', daemons.OSD),
        ('prometheus', _cephadm.Monitoring),
        ('snmp-gateway', _cephadm.SNMPGateway),
    ],
)
def test_choose_daemon_form(daemon_type, cls):
    daemon_form.choose(daemon_type) == cls


def test_choose_daemon_form_error():
    with pytest.raises(daemon_form.UnexpectedDaemonTypeError):
        daemon_form.choose('__nope__nope__')


@pytest.mark.parametrize(
    'dt,is_sdf',
    [
        ('haproxy', True),
        ('iscsi', False),
        ('keepalived', True),
        ('mon', False),
        ('nfs', False),
        ('nvmeof', True),
        ('osd', True),
    ],
)
def test_is_sysctl_daemon_form(dt, is_sdf):
    uuid = 'daeb985e-58c7-11ee-a536-201e8814f771'
    ctx = mock.MagicMock()
    ctx.config_blobs = {
        'files': _dmock(),
        'pool': 'swimming',
        'destination': 'earth',
    }
    ident = daemon_identity.DaemonIdentity(uuid, dt, 'abc')
    inst = daemon_form.create(ctx, ident)
    assert isinstance(inst, daemon_form.SysctlDaemonForm) == is_sdf


def test_can_create_all_daemon_forms(monkeypatch):
    uuid = 'daeb985e-58c7-11ee-a536-201e8814f771'
    ctx = mock.MagicMock()
    ctx.config_blobs = {
        'files': _dmock(),
        'pool': 'swimming',
        'destination': 'earth',
    }
    _os_path_isdir = mock.MagicMock(return_value=True)
    monkeypatch.setattr('os.path.isdir', _os_path_isdir)
    dtypes = _cephadm.get_supported_daemons()
    for daemon_type in dtypes:
        if daemon_type == 'agent':
            # skip agent because it is special & not currently a daemonform
            continue
        ident = daemon_identity.DaemonIdentity(uuid, daemon_type, 'xyz')
        inst = daemon_form.create(ctx, ident)
        assert inst.identity.daemon_type == daemon_type


def _dmock():
    dmock = mock.MagicMock()
    dmock.__contains__.return_value = True
    dmock.__getitem__.return_value = ''
    return dmock
