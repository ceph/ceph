import os

from cephadmlib.daemon_identity import DaemonIdentity
from cephadmlib.daemons.ceph import Ceph, Crash, crash_client_name

FSID = 'b01dbeef-701d-9abe-0000-e1e5a47004a7'
FQDN_ENTITY = 'client.crash.node1.example.org'
FQDN_KEYRING = '[%s]\nkey = AQ==\n' % FQDN_ENTITY


class _Ctx:
    def __init__(self, keyring=None, auth_entity=None,
                 data_dir='/var/lib/ceph', log_dir='/var/log/ceph'):
        self.data_dir = data_dir
        # get_ceph_mounts_for_type mkdir's this when it is missing.
        self.log_dir = log_dir
        self.config_blobs = {}
        if keyring is not None:
            self.config_blobs['keyring'] = keyring
            self.config_blobs['config'] = '[global]\nfsid = %s\n' % FSID
        # auth entity passed by the mgr in the _orch deploy payload
        self.auth_entity = auth_entity


def _crash(ctx, daemon_id='node1'):
    ident = DaemonIdentity(FSID, 'crash', daemon_id)
    return Crash(ctx, ident), ident


def _deploy_keyring_on_disk(ctx, ident, keyring):
    data_dir = ident.data_dir(ctx.data_dir)
    os.makedirs(data_dir, exist_ok=True)
    with open(os.path.join(data_dir, 'keyring'), 'w') as f:
        f.write(keyring)


def test_crash_auth_entity_from_deploy_payload_wins(tmp_path):
    ctx = _Ctx(
        # a disagreeing keyring blob must not override the payload entity
        keyring='[client.crash.other.example.org]\nkey = AQ==\n',
        auth_entity=FQDN_ENTITY,
        data_dir=str(tmp_path),
        log_dir=str(tmp_path),
    )
    daemon, ident = _crash(ctx)
    assert crash_client_name(ctx, ident) == FQDN_ENTITY

    args = []
    daemon.customize_process_args(ctx, args)
    assert args == ['-n', FQDN_ENTITY]

    mounts = Ceph.get_ceph_mounts(ctx, ident)
    keyring_mount = mounts[ident.data_dir(ctx.data_dir) + '/keyring']
    assert keyring_mount == '/etc/ceph/ceph.%s.keyring' % FQDN_ENTITY


def test_crash_fqdn_keyring_sets_client_name_and_mount(tmp_path):
    ctx = _Ctx(FQDN_KEYRING, data_dir=str(tmp_path), log_dir=str(tmp_path))
    daemon, ident = _crash(ctx)
    assert crash_client_name(ctx, ident) == FQDN_ENTITY

    args = []
    daemon.customize_process_args(ctx, args)
    assert args == ['-n', FQDN_ENTITY]

    mounts = Ceph.get_ceph_mounts(ctx, ident)
    keyring_mount = mounts[ident.data_dir(ctx.data_dir) + '/keyring']
    assert keyring_mount == '/etc/ceph/ceph.%s.keyring' % FQDN_ENTITY


def test_crash_reads_entity_from_keyring_deployed_on_disk(tmp_path):
    # no config blobs and no payload entity: e.g. cephadm shell --name
    # crash.node1 on a host where the daemon was already deployed
    ctx = _Ctx(data_dir=str(tmp_path), log_dir=str(tmp_path))
    daemon, ident = _crash(ctx)
    _deploy_keyring_on_disk(ctx, ident, FQDN_KEYRING)

    args = []
    daemon.customize_process_args(ctx, args)
    assert args == ['-n', FQDN_ENTITY]

    mounts = Ceph.get_ceph_mounts(ctx, ident)
    keyring_mount = mounts[ident.data_dir(ctx.data_dir) + '/keyring']
    assert keyring_mount == '/etc/ceph/ceph.%s.keyring' % FQDN_ENTITY


def test_crash_short_hostname_keeps_daemon_id_entity(tmp_path):
    keyring = '[client.crash.node1]\nkey = AQ==\n'
    ctx = _Ctx(keyring, data_dir=str(tmp_path), log_dir=str(tmp_path))
    daemon, ident = _crash(ctx)

    args = []
    daemon.customize_process_args(ctx, args)
    assert args == ['-n', 'client.crash.node1']

    mounts = Ceph.get_ceph_mounts(ctx, ident)
    keyring_mount = mounts[ident.data_dir(ctx.data_dir) + '/keyring']
    assert keyring_mount == '/etc/ceph/ceph.client.crash.node1.keyring'


def test_crash_without_keyring_falls_back_to_daemon_id(tmp_path):
    ctx = _Ctx(data_dir=str(tmp_path), log_dir=str(tmp_path))
    daemon, ident = _crash(ctx)

    args = []
    daemon.customize_process_args(ctx, args)
    assert args == ['-n', 'client.crash.node1']


def test_crash_ignores_non_crash_or_unsafe_keyring_entity(tmp_path):
    for keyring in (
        '[client.admin]\nkey = AQ==\n',
        '[client.crash.]\nkey = AQ==\n',
        '[client.crash.../etc/passwd]\nkey = AQ==\n',
    ):
        ctx = _Ctx(keyring, data_dir=str(tmp_path))
        assert crash_client_name(ctx, DaemonIdentity(FSID, 'crash', 'node1')) \
            == 'client.crash.node1'
        # the same content already deployed on disk is ignored as well
        ctx = _Ctx(data_dir=str(tmp_path))
        ident = DaemonIdentity(FSID, 'crash', 'node1')
        _deploy_keyring_on_disk(ctx, ident, keyring)
        assert crash_client_name(ctx, ident) == 'client.crash.node1'


def test_crash_ignores_unsafe_payload_auth_entity(tmp_path):
    ctx = _Ctx(
        keyring=FQDN_KEYRING,
        auth_entity='client.admin',
        data_dir=str(tmp_path),
    )
    ident = DaemonIdentity(FSID, 'crash', 'node1')
    assert crash_client_name(ctx, ident) == FQDN_ENTITY
