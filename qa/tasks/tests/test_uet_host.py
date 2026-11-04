"""
Tests of the uet_host task, which builds the libfabric uet provider over
the reference provider's software core on bare-metal nodes.
"""
import types

import pytest

from tasks import ernic, uet_host


FI_VERSION = 'fi_info: 1.22.0\nlibfabric: 1.22.0\nlibfabric api: 1.22\n'

# fi_info -p uet on a node whose provider lists every interface that is up
FI_INFO = '''provider: uet
    fabric: uet
    domain: ens1f0np0
    version: 0.1
    type: FI_EP_RDM
    protocol: FI_PROTO_UNSPEC
provider: uet
    fabric: uet
    domain: eno1
    version: 0.1
    type: FI_EP_RDM
    protocol: FI_PROTO_UNSPEC
'''


def test_hold_env():
    assert uet_host.hold_env(None) == dict()
    assert uet_host.hold_env(dict()) == dict()
    assert uet_host.hold_env(dict(ms=600, dir='tx', types=['rudi_resp', 'ack'],
                                  every=10, dup=True)) == dict(
        UET_PKT_HOLD_MS='600', UET_PKT_HOLD_DIR='tx',
        UET_PKT_HOLD_TYPES='rudi_resp,ack', UET_PKT_HOLD_EVERY='10',
        UET_PKT_HOLD_DUP='1')
    with pytest.raises(RuntimeError, match='needs ms'):
        uet_host.hold_env(dict(every=10))
    with pytest.raises(RuntimeError, match='unknown hold options'):
        uet_host.hold_env(dict(ms=10, seconds=3))


def test_environment():
    assert uet_host.environment(dict()) == dict()
    assert uet_host.environment(dict(pds='rud')) == dict(FI_UET_RUDI='0')
    env = uet_host.environment(dict(hold=dict(ms=600), env=dict(X=1)))
    assert env == dict(UET_PKT_HOLD_MS='600', X='1')
    with pytest.raises(RuntimeError):
        uet_host.environment(dict(pds='rod'))


def test_nft_script():
    add = uet_host.nft_script(True)
    assert 'udp dport 4793 drop' in add
    assert 'meta l4proto 253 drop' in add
    assert 'hook input' in add
    assert uet_host.nft_script(False) == \
        'sudo nft delete table inet ceph_uet_host 2>/dev/null || true'


def test_check_fi_info():
    entries = uet_host.check_fi_info(FI_INFO, 'ens1f0np0')
    assert [e['domain'] for e in entries] == ['ens1f0np0']
    with pytest.raises(RuntimeError, match='no RDM endpoint on eth9'):
        uet_host.check_fi_info(FI_INFO, 'eth9')
    with pytest.raises(RuntimeError):
        uet_host.check_fi_info('fi_getinfo: -61 (No data available)\n', 'eno1')


def test_client_domain():
    assert uet_host.client_domain(['ens1f0np0', 'ens1f0np0']) == 'ens1f0np0'
    assert uet_host.client_domain(['ens1f0np0', 'ens2f0np0']) is None


class FakeRemote(object):
    def __init__(self, name, addr, iface='ens1f0np0', tools=('get', 'put')):
        self.name = f"ubuntu@{name}"
        self.shortname = name
        self.interface = iface
        self.os = types.SimpleNamespace(package_type='rpm')
        self.addr = addr
        self.tools = [f"ceph_test_rgw_ofi_{t}" for t in tools]
        self.scripts = []
        self.runs = []
        self.fi_version = FI_VERSION

    def sh(self, args, timeout=None):
        script, rest = args[2], args[4:]
        self.scripts.append((script, rest))
        if script == uet_host.ADDR_SCRIPT:
            return self.addr + '\n' if rest == [self.interface] else ''
        if script == ernic.FI_VERSION_SCRIPT:
            return self.fi_version
        if script == ernic.PROVIDER_DIR_SCRIPT:
            return '/usr/lib64/libfabric\n'
        if script.endswith(ernic.FI_INFO_SCRIPT):
            return FI_INFO.replace('ens1f0np0', self.interface)
        if script == uet_host.NET_RAW_SCRIPT:
            return f"/usr/bin/{rest[0]}\n" if rest[0] in self.tools else ''
        if script == ernic.FETCH_SCRIPT:
            return '0123abc prov: something\n'
        return ''

    def run(self, args, **kwargs):
        self.runs.append(args)


class FakeCluster(object):
    def __init__(self, roles):
        self.roles = roles

    @property
    def remotes(self):
        return dict(self.roles)

    def only(self, role):
        return FakeCluster(dict((r, roles) for r, roles in self.roles.items()
                                if role in roles))


def make_ctx(b_iface='ens1f0np0', overrides=None):
    a = FakeRemote('mako01', '172.21.2.101/20')
    b = FakeRemote('mako02', '172.21.2.102/20', iface=b_iface, tools=())
    ctx = types.SimpleNamespace(
        cluster=FakeCluster({a: ['osd.0'], b: ['client.1']}),
        config=dict(overrides=overrides or dict()),
    )
    return ctx, a, b


def test_task():
    ctx, a, b = make_ctx(overrides=dict(uet_host=dict(pds='rud')))
    config = dict(uet_prov=dict(repo='https://example.com/uet-ref-prov',
                                ref='wip-uet-hold'),
                  hold=dict(ms=600, types='rudi_resp', every=10))
    with uet_host.task(ctx, config):
        overrides = ctx.config['overrides']
        assert overrides['ceph']['conf']['global'] == dict(
            oob_network='172.21.0.0/20', oob_network_interface='ens1f0np0')
        for task_name, key in ernic.ENV_OVERRIDES:
            assert overrides[task_name][key]['FI_UET_RUDI'] == '0'
            assert overrides[task_name][key]['UET_PKT_HOLD_MS'] == '600'
            assert overrides[task_name][key]['UET_PKT_HOLD_TYPES'] == \
                'rudi_resp'
        assert overrides['workunit']['env']['OOB_DOMAIN'] == 'ens1f0np0'
        assert ctx.uet_host[a.name]['domain'] == 'ens1f0np0'
        assert ctx.uet_host[a.name]['addr'] == '172.21.2.101/20'
        assert ctx.uet_host[a.name]['net_raw'] == [
            '/usr/bin/ceph_test_rgw_ofi_get', '/usr/bin/ceph_test_rgw_ofi_put']
        assert ctx.uet_host[b.name]['net_raw'] == []
        # the kernel filter, and the build of the reference core
        assert ['bash', '-c', uet_host.nft_script(True)] in a.runs
        assert uet_host.BUILD_SCRIPT in [s for s, _ in a.scripts]
        install = [args for s, args in a.scripts
                   if s == uet_host.INSTALL_SCRIPT]
        assert install[0][1] == '/usr/lib64/libfabric'
        assert ['sudo', 'dnf', 'install', '-y'] + \
            uet_host.DEFAULT_PACKAGES['rpm'] in a.runs
    # everything goes at the end
    assert ['bash', '-c', uet_host.nft_script(False)] in a.runs
    assert ['sudo', 'setcap', '-r', '/usr/bin/ceph_test_rgw_ofi_get'] in a.runs
    assert any(args[:2] == ['rm', '-rf'] and
               '/usr/lib64/libfabric/libuet-fi.so' in args for args in a.runs)


def test_task_interfaces_of_other_names():
    # no OOB_DOMAIN when the nodes' interfaces have different names, and
    # no oob_network_interface either
    ctx, a, b = make_ctx(b_iface='ens2f0np0')
    with uet_host.task(ctx, dict(drop_kernel_udp=False)):
        overrides = ctx.config['overrides']
        assert 'OOB_DOMAIN' not in overrides.get('workunit', {}).get('env', {})
        assert overrides['ceph']['conf']['global'] == dict(
            oob_network='172.21.0.0/20')
        assert ['bash', '-c', uet_host.nft_script(True)] not in a.runs


def test_task_old_libfabric_fails_and_cleans_up():
    ctx, a, b = make_ctx()
    a.fi_version = 'fi_info: 1.17.0\nlibfabric: 1.17.0\n'
    with pytest.raises(RuntimeError, match='libfabric 1.17'):
        with uet_host.task(ctx, dict()):
            pass
    assert any(args[:2] == ['rm', '-rf'] for args in a.runs)


def test_task_interface_without_address():
    ctx, a, b = make_ctx()
    a.addr = ''
    with pytest.raises(RuntimeError, match='has no IPv4 address'):
        with uet_host.task(ctx, dict()):
            pass


def test_task_suite_network_wins():
    ctx, a, b = make_ctx(overrides=dict(ceph=dict(conf=dict(global_={}))))
    ctx.config['overrides'] = dict(ceph=dict(conf=dict(
        **{'global': {'oob network': '10.0.0.0/8'}})))
    with uet_host.task(ctx, dict()):
        conf = ctx.config['overrides']['ceph']['conf']['global']
        assert conf['oob network'] == '10.0.0.0/8'
        assert 'oob_network' not in conf
