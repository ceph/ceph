"""
Tests of the ernic task, which checks rocm-ernic guests and installs the
libfabric uet provider over libuet_ernic, or checks the verbs provider.
"""
import shutil
import subprocess
import types

import pytest

from tasks import ernic


# what CHECK_SCRIPT printed in a guest with rocm-ernic's ionic modules
GOOD = '''pci=0000:01:00.0
driver=ionic
rdma=rocm-rdma-ernic0
netdev=rocm-ernic0
addr=10.77.0.61/24
mtu=1500
port=ACTIVE
modules=ionic ionic_rdma ib_uverbs
memlock=unlimited
'''

# fi_info --version in an Ubuntu 26.04 guest
FI_VERSION = 'fi_info: 2.1.0\nlibfabric: 2.1.0\nlibfabric api: 2.1\n'

# fi_info -p uet in that guest
FI_INFO = '''provider: uet
    fabric: uet
    domain: rocm-rdma-ernic0
    version: 0.1
    type: FI_EP_RDM
    protocol: FI_PROTO_UNSPEC
'''

# fi_info -p 'verbs;ofi_rxm' -t FI_EP_RDM in a guest that also has another
# RDMA device
VERBS_FI_INFO = '''provider: verbs;ofi_rxm
    fabric: IB-0xfe80000000000000
    domain: rocm-rdma-ernic0
    version: 1.0
    type: FI_EP_RDM
    protocol: FI_PROTO_RXM
provider: verbs;ofi_rxm
    fabric: IB-0xfe80000000000000
    domain: mlx5_0
    version: 1.0
    type: FI_EP_RDM
    protocol: FI_PROTO_RXM
'''


def test_check_nic_good():
    found = ernic.parse_kv(GOOD)
    assert found['rdma'] == 'rocm-rdma-ernic0'
    assert ernic.check_nic(found) == []


def test_check_nic_stock_image():
    # the mainline ionic driver does not bind rocm-ernic's device
    found = ernic.parse_kv('pci=0000:01:00.0\ndriver=\nrdma=\nnetdev=\n'
                           'modules=\nmemlock=8192\n')
    problems = ernic.check_nic(found)
    assert any('not ionic' in p for p in problems)
    assert any('ionic_rdma module' in p for p in problems)
    assert any('unlimited' in p for p in problems)
    assert not any('unlimited' in p for p in ernic.check_nic(found, False))


def test_check_nic_no_device():
    assert ernic.check_nic(ernic.parse_kv('pci=\nmodules=\n')) == [
        'no emulated ionic device (0x1dd8:0x100a) on the PCI bus']


def test_check_nic_port_down():
    found = ernic.parse_kv(GOOD.replace('port=ACTIVE', 'port=DOWN'))
    assert ernic.check_nic(found) == [
        'RDMA port 1 of rocm-rdma-ernic0 is DOWN']


def test_fi_info():
    entries = ernic.check_fi_info(FI_INFO)
    assert entries == [dict(provider='uet', fabric='uet',
                            domain='rocm-rdma-ernic0', version='0.1',
                            type='FI_EP_RDM', protocol='FI_PROTO_UNSPEC')]
    with pytest.raises(RuntimeError):
        ernic.check_fi_info('fi_getinfo: -61 (No data available)\n')


def test_libfabric_version():
    assert ernic.libfabric_version(FI_VERSION) == (2, 1)
    assert ernic.libfabric_version('fi_info: 1.17.0\nlibfabric: 1.17.0\n') \
        == (1, 17)
    assert ernic.libfabric_version('bash: fi_info: command not found') is None
    ernic.check_libfabric(FI_VERSION)
    with pytest.raises(RuntimeError, match='libfabric 1.17'):
        ernic.check_libfabric('libfabric: 1.17.0\n')


def test_oob_overrides():
    assert ernic.oob_overrides(['10.88.0.61/24', '10.88.1.62/24'],
                               ['rocm-ernic0', 'eth1']) == dict(
        oob_network='10.88.0.0/24, 10.88.1.0/24')


def test_environment():
    assert ernic.environment(dict()) == dict()
    assert ernic.environment(dict(pds='rud')) == dict(FI_UET_RUDI='0')
    assert ernic.environment(dict(pds='rudi', env=dict(X=1))) == dict(X='1')
    with pytest.raises(RuntimeError):
        ernic.environment(dict(pds='rod'))


def test_fi_info_verbs():
    entries = ernic.check_fi_info(VERBS_FI_INFO, 'verbs')
    assert [e['domain'] for e in entries] == ['rocm-rdma-ernic0', 'mlx5_0']
    with pytest.raises(RuntimeError):
        ernic.check_fi_info(FI_INFO, 'verbs')


def test_environment_verbs():
    assert ernic.environment(dict(provider='verbs')) == dict()
    assert ernic.environment(dict(provider='verbs', env=dict(X=1))) == dict(X='1')
    # pds is a uet setting
    with pytest.raises(RuntimeError):
        ernic.environment(dict(provider='verbs', pds='rud'))


class FakeRemote(object):
    def __init__(self, name, addr):
        self.name = f"ubuntu@{name}"
        self.shortname = name
        self.os = types.SimpleNamespace(package_type='deb')
        self.addr = addr
        self.scripts = []
        self.runs = []
        self.files = dict()
        self.fi_version = FI_VERSION
        self.verbs_fi_info = VERBS_FI_INFO

    def sh(self, args, timeout=None):
        script = args[2] if isinstance(args, list) else args
        self.scripts.append((script, args[4:] if isinstance(args, list) else []))
        if script == ernic.CHECK_SCRIPT:
            return GOOD.replace('10.77.0.61/24', self.addr)
        if script == ernic.PROVIDER_DIR_SCRIPT:
            return '/usr/lib/x86_64-linux-gnu/libfabric\n'
        if script.endswith(ernic.FI_INFO_SCRIPT):
            return FI_INFO
        if script == ernic.FI_VERSION_SCRIPT:
            return self.fi_version
        if script == ernic.VERBS_FI_INFO_SCRIPT:
            return self.verbs_fi_info
        return ''

    def run(self, args, **kwargs):
        self.runs.append(args)

    def write_file(self, path, data):
        self.files[path] = data


class FakeCluster(object):
    def __init__(self, roles):
        self.roles = roles

    @property
    def remotes(self):
        return dict((r, roles) for r, roles in self.roles.items())

    def only(self, role):
        return FakeCluster(dict((r, roles) for r, roles in self.roles.items()
                                if role in roles))


def make_ctx():
    a = FakeRemote('vm1', '10.77.0.61/24')
    b = FakeRemote('vm2', '10.77.0.62/24')
    ctx = types.SimpleNamespace(
        cluster=FakeCluster({a: ['osd.0'], b: ['client.0']}),
        config=dict(overrides=dict(ernic=dict(pds='rud'))),
    )
    return ctx, a, b


def test_task():
    ctx, a, b = make_ctx()
    config = dict(rocm_ernic=dict(repo='https://example.com/rocm-ernic', ref='abc'),
                  uet_prov=dict(repo='https://example.com/uet-ref-prov', ref='def'))
    with ernic.task(ctx, config):
        conf = ctx.config['overrides']['ceph']['conf']['global']
        assert conf == dict(oob_network='10.77.0.0/24',
                            oob_network_interface='rocm-ernic0')
        assert ctx.ernic[a.name]['domain'] == 'rocm-rdma-ernic0'
        scripts = [s for s, _ in a.scripts if s != ernic.FETCH_SCRIPT]
        assert scripts[:3] == [ernic.CHECK_SCRIPT, ernic.FI_VERSION_SCRIPT,
                               ernic.BUILD_SCRIPT]
        fetches = [args for s, args in a.scripts if s == ernic.FETCH_SCRIPT]
        assert [f[:2] for f in fetches] == [
            ['https://example.com/rocm-ernic', 'abc'],
            ['https://example.com/uet-ref-prov', 'def']]
        # pds: rud reaches the daemons, the gateways and the workunits
        overrides = ctx.config['overrides']
        assert overrides['ceph']['daemon_env'] == dict(FI_UET_RUDI='0')
        assert overrides['rgw']['env'] == dict(FI_UET_RUDI='0')
        assert overrides['workunit']['env'] == dict(FI_UET_RUDI='0')
    # the provider goes again
    assert '/usr/lib/x86_64-linux-gnu/libfabric/libuet-fi.so' in b.runs[-1]


def test_task_keeps_a_suites_environment():
    ctx, a, b = make_ctx()
    ctx.config['overrides']['ernic']['env'] = dict(FI_LOG_LEVEL='warn')
    ctx.config['overrides']['rgw'] = dict(env=dict(FI_UNIVERSE_SIZE=16))
    with ernic.task(ctx, dict(rocm_ernic=dict(repo='r'), uet_prov=dict(repo='u'))):
        assert ctx.config['overrides']['rgw']['env'] == dict(
            FI_UNIVERSE_SIZE=16, FI_UET_RUDI='0', FI_LOG_LEVEL='warn')


def test_task_keeps_a_suites_network():
    ctx, a, b = make_ctx()
    ctx.config['overrides']['ceph'] = dict(conf=dict(global_={}))
    ctx.config['overrides']['ceph']['conf'] = {
        'global': {'oob network': '192.168.0.0/16'}}
    with ernic.task(ctx, dict(rocm_ernic=dict(repo='r'), uet_prov=dict(repo='u'))):
        conf = ctx.config['overrides']['ceph']['conf']['global']
        assert conf['oob network'] == '192.168.0.0/16'
        assert 'oob_network' not in conf


def test_task_old_libfabric():
    ctx, a, b = make_ctx()
    b.fi_version = 'fi_info: 1.17.0\nlibfabric: 1.17.0\nlibfabric api: 1.17\n'
    with pytest.raises(RuntimeError, match='need 1.20 or later'):
        with ernic.task(ctx, dict(rocm_ernic=dict(repo='r'),
                                  uet_prov=dict(repo='u'))):
            pass


def test_task_bad_guest():
    ctx, a, b = make_ctx()
    a.sh = lambda args, timeout=None: 'pci=0000:01:00.0\ndriver=\nmodules=\n'
    with pytest.raises(RuntimeError, match='not a usable ernic guest'):
        with ernic.task(ctx, dict()):
            pass


def test_rgw_endpoint():
    ctx, a, b = make_ctx()
    ctx.rgw = types.SimpleNamespace(role_endpoints={
        'osd.0': types.SimpleNamespace(url=lambda: 'http://vm1:80/')})
    with ernic.rgw_endpoint(ctx, dict(rgw='osd.0', clients=['client.0'])):
        assert list(b.files.values()) == ['http://vm1:80']


@pytest.mark.skipif(shutil.which('git') is None, reason='needs git')
def test_fetch_script(tmp_path):
    # a repo with two commits on main, and a tag on the first
    repo = tmp_path / 'repo'
    env = dict(GIT_AUTHOR_NAME='t', GIT_AUTHOR_EMAIL='t@example.com',
               GIT_COMMITTER_NAME='t', GIT_COMMITTER_EMAIL='t@example.com',
               HOME=str(tmp_path), PATH='/usr/bin:/bin')

    def git(*args, cwd=repo):
        return subprocess.run(['git'] + list(args), cwd=cwd, env=env,
                              check=True, capture_output=True,
                              text=True).stdout.strip()

    repo.mkdir()
    git('init', '-q', '-b', 'main')
    for n in ('one', 'two'):
        (repo / 'f').write_text(n)
        git('add', 'f')
        git('commit', '-q', '-m', n)
    first = git('rev-parse', 'HEAD~1')
    git('tag', 'v1', first)
    tip = git('rev-parse', 'HEAD')
    url = 'file://' + str(repo)
    # an abbreviated commit id that is not a branch's tip, as in the
    # docstring, a full one, a branch and a tag
    for n, (ref, want) in enumerate([(first[:7], first), (first, first),
                                     ('main', tip), ('v1', first)]):
        dest = tmp_path / ('dest%d' % n)
        out = subprocess.run(['bash', '-c', ernic.FETCH_SCRIPT, 'ernic', url,
                              ref, str(dest)], env=env, check=True,
                             capture_output=True, text=True).stdout
        assert out.split()[0] == want, ref
        assert git('rev-parse', 'HEAD', cwd=dest) == want


def test_task_verbs():
    ctx, a, b = make_ctx()
    ctx.config['overrides']['ernic'] = dict()
    with ernic.task(ctx, dict(provider='verbs')):
        conf = ctx.config['overrides']['ceph']['conf']['global']
        assert conf == dict(oob_network='10.77.0.0/24',
                            oob_network_interface='rocm-ernic0')
        # the endpoint on the ionic RDMA device, not on the other one
        assert ctx.ernic[a.name]['domain'] == 'rocm-rdma-ernic0'
        scripts = [s for s, _ in a.scripts]
        assert scripts == [ernic.CHECK_SCRIPT, ernic.FI_VERSION_SCRIPT,
                           ernic.VERBS_FI_INFO_SCRIPT]
        # no pds and no env: the daemons get no environment from the task
        assert 'daemon_env' not in ctx.config['overrides']['ceph']
    # nothing was built or installed, so nothing goes
    assert b.runs and not any('libuet-fi.so' in str(r) for r in b.runs)


def test_task_verbs_not_on_the_nic():
    ctx, a, b = make_ctx()
    ctx.config['overrides']['ernic'] = dict()
    a.verbs_fi_info = VERBS_FI_INFO.split('provider:', 2)[0] + \
        'provider:' + VERBS_FI_INFO.split('provider:', 2)[2]
    with pytest.raises(RuntimeError, match='no RDM endpoint on rocm-rdma-ernic0'):
        with ernic.task(ctx, dict(provider='verbs')):
            pass


def test_task_bad_provider():
    ctx, a, b = make_ctx()
    with pytest.raises(RuntimeError, match='uet or verbs'):
        with ernic.task(ctx, dict(provider='psm3')):
            pass
