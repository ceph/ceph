from cephadmlib.daemon_identity import DaemonIdentity
from cephadmlib.daemons.ceph import Ceph, Crash, crash_client_name

FSID = 'b01dbeef-701d-9abe-0000-e1e5a47004a7'
FQDN_ENTITY = 'client.crash.node1.example.org'
FQDN_KEYRING = '[%s]\nkey = AQ==\n' % FQDN_ENTITY


class _Ctx:
    def __init__(self, keyring=None, log_dir='/var/log/ceph'):
        self.data_dir = '/var/lib/ceph'
        # get_ceph_mounts_for_type mkdir's this when it is missing.
        self.log_dir = log_dir
        self.config_blobs = {}
        if keyring is not None:
            self.config_blobs['keyring'] = keyring
            self.config_blobs['config'] = '[global]\nfsid = %s\n' % FSID


def _crash(ctx, daemon_id='node1'):
    ident = DaemonIdentity(FSID, 'crash', daemon_id)
    return Crash(ctx, ident), ident


def test_crash_fqdn_keyring_sets_client_name_and_mount(tmp_path):
    ctx = _Ctx(FQDN_KEYRING, log_dir=str(tmp_path))
    daemon, ident = _crash(ctx)
    assert crash_client_name(ctx, ident.daemon_id) == FQDN_ENTITY

    args = []
    daemon.customize_process_args(ctx, args)
    assert args == ['-n', FQDN_ENTITY]

    mounts = Ceph.get_ceph_mounts(ctx, ident)
    keyring_mount = mounts[ident.data_dir(ctx.data_dir) + '/keyring']
    assert keyring_mount == '/etc/ceph/ceph.%s.keyring' % FQDN_ENTITY


def test_crash_short_hostname_keeps_daemon_id_entity(tmp_path):
    keyring = '[client.crash.node1]\nkey = AQ==\n'
    ctx = _Ctx(keyring, log_dir=str(tmp_path))
    daemon, ident = _crash(ctx)

    args = []
    daemon.customize_process_args(ctx, args)
    assert args == ['-n', 'client.crash.node1']

    mounts = Ceph.get_ceph_mounts(ctx, ident)
    keyring_mount = mounts[ident.data_dir(ctx.data_dir) + '/keyring']
    assert keyring_mount == '/etc/ceph/ceph.client.crash.node1.keyring'


def test_crash_without_keyring_falls_back_to_daemon_id():
    ctx = _Ctx()
    daemon, ident = _crash(ctx)

    args = []
    daemon.customize_process_args(ctx, args)
    assert args == ['-n', 'client.crash.node1']


def test_crash_ignores_non_crash_or_unsafe_keyring_entity():
    for keyring in (
        '[client.admin]\nkey = AQ==\n',
        '[client.crash.]\nkey = AQ==\n',
        '[client.crash.../etc/passwd]\nkey = AQ==\n',
    ):
        ctx = _Ctx(keyring)
        assert crash_client_name(ctx, 'node1') == 'client.crash.node1'
