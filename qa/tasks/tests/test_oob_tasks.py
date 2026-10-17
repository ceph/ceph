"""
Tests of the qa tasks and options for out-of-band transfers (S3 over
RDMA).
"""
import json
import signal
import types

from io import StringIO

import pytest

from tasks import ceph_manager, netem, oob_check, rdma_rxe, rgw_thrash


# netem

def test_percent():
    assert netem.percent(1) == '1%'
    assert netem.percent(0.5) == '0.5%'
    assert netem.percent('0.05%') == '0.05%'
    with pytest.raises(ValueError):
        netem.percent('150%')
    with pytest.raises(ValueError):
        netem.percent('lots')


def test_fault_args_off_by_default():
    assert netem.fault_args({}) == []
    # a delay alone stays with static_delay
    assert netem.fault_args({'delay': '10ms'}) == []


def test_fault_args():
    assert netem.fault_args({'loss': '1%', 'corrupt': 0.1}) == \
        ['loss', '1%', 'corrupt', '0.1%']
    assert netem.fault_args({'delay': '2ms', 'duplicate': '1%'}) == \
        ['delay', '2ms', 'duplicate', '1%']


def test_fault_args_reorder_needs_a_delay():
    assert netem.fault_args({'reorder': '1%'}) == \
        ['delay', netem.REORDER_DELAY, 'reorder', '1%']
    assert netem.fault_args({'reorder': '1%', 'delay': '3ms'}) == \
        ['delay', '3ms', 'reorder', '1%']


def test_fault_cmds():
    root, child, flt = netem.fault_cmds('eth0', '10.0.0.2',
                                        ['loss', '1%'])
    assert root[:9] == ['sudo', 'tc', 'qdisc', 'replace', 'dev', 'eth0',
                        'root', 'handle', '1:']
    assert 'bands' in root and root[root.index('bands') + 1] == '4'
    # the default priomap keeps every other packet off band 4
    priomap = root[root.index('priomap') + 1:]
    assert len(priomap) == 16 and '3' not in priomap
    assert child[-3:] == ['netem', 'loss', '1%']
    assert child[child.index('parent') + 1] == '1:4'
    assert flt[flt.index('dst') + 1] == '10.0.0.2/32'
    assert flt[-2:] == ['flowid', '1:4']


def test_route_dev():
    out = '10.0.0.2 dev enp3s0f0 src 10.0.0.1 uid 1000 \\    cache'
    assert netem.route_dev(out) == 'enp3s0f0'
    assert netem.route_dev('RTNETLINK answers: Network is unreachable') \
        is None


class FakeNetemRemote(object):

    def __init__(self, name, route):
        self.name = self.shortname = self.hostname = name
        self.route = route

    def sh(self, args, **kwargs):
        assert args[:4] == ['ip', '-o', 'route', 'get']
        return self.route


@pytest.mark.parametrize('config', [
    {'delay': '10ms'},
    {'delay_range': ['10ms', '20ms']},
    {'loss': '1%'},
])
def test_netem_auto_iface(monkeypatch, config):
    # 'iface: auto' is resolved for every kind of config, and the
    # teardown uses the same interface
    calls = []
    monkeypatch.setattr(netem.socket, 'gethostbyname', lambda name: '10.0.0.2')
    monkeypatch.setattr(netem, 'static_delay', lambda r, h, iface, d:
                        calls.append(('static_delay', iface)))
    monkeypatch.setattr(netem, 'variable_delay', lambda r, h, iface, d:
                        calls.append(('variable_delay', iface)))
    monkeypatch.setattr(netem, 'static_faults', lambda r, h, iface, a:
                        calls.append(('static_faults', iface)))
    monkeypatch.setattr(netem, 'delete_dev', lambda r, iface:
                        calls.append(('delete_dev', iface)))
    remotes = {
        'client.0': FakeNetemRemote('host1', '10.0.0.2 dev enp3s0 src 10.0.0.1'),
        'client.1': FakeNetemRemote('host2', ''),
    }
    ctx = types.SimpleNamespace(cluster=types.SimpleNamespace(
        only=lambda role: types.SimpleNamespace(
            remotes={remotes[role]: [role]})))
    config = dict(config, clients=['client.0'], iface='auto',
                  dst_client='client.1')
    with netem.task(ctx, config):
        assert len(calls) == 1 and calls[0][1] == 'enp3s0'
    assert calls[-1] == ('delete_dev', 'enp3s0')


# the OSD thrasher's SIGSTOP

def test_conf_value():
    conf = {'global': {'osd_oob_cutoff_late_tolerance': '500'},
            'osd': {'osd oob cutoff late tolerance': 2000}}
    assert ceph_manager.conf_value(conf, 'osd_oob_cutoff_late_tolerance') \
        == 2000
    assert ceph_manager.conf_value({'global': conf['global']},
                                   'osd oob cutoff late tolerance') == '500'
    assert ceph_manager.conf_value({}, 'osd_oob_cutoff_late_tolerance') \
        is None


def test_millisecs_to_seconds():
    assert ceph_manager.millisecs_to_seconds(1000) == 1.0
    assert ceph_manager.millisecs_to_seconds('1500') == 1.5
    assert ceph_manager.millisecs_to_seconds('250ms') == 0.25
    assert ceph_manager.millisecs_to_seconds('2s') == 2.0


def test_sigstop_duration():
    def bounds(low, high):
        return (low, high)
    # longer than the tolerance by a second at least
    assert ceph_manager.sigstop_duration(1.0, 0.5, 10, bounds) == (2.0, 10.0)
    assert ceph_manager.sigstop_duration(1.0, 3, 10, bounds) == (3.0, 10.0)
    assert ceph_manager.sigstop_duration(15.0, 2, 10, bounds) == (16.0, 16.0)
    d = ceph_manager.sigstop_duration(1.0, 2, 10)
    assert 2.0 <= d <= 10.0


class FakeManager(object):

    def __init__(self, conf):
        self.signals = []
        self.ctx = types.SimpleNamespace(
            ceph={'ceph': types.SimpleNamespace(conf=conf)})

    def signal_osd(self, osd, sig, silent=False):
        self.signals.append((osd, sig))


def fake_thrasher(conf, config):
    thrasher = object.__new__(ceph_manager.OSDThrasher)
    thrasher.ceph_manager = FakeManager(conf)
    thrasher.cluster = 'ceph'
    thrasher.config = config
    thrasher.live_osds = [3]
    thrasher.logger = types.SimpleNamespace(info=lambda *a, **k: None)
    return thrasher


def test_sigstop_osd(monkeypatch):
    slept = []
    monkeypatch.setattr(ceph_manager.time, 'sleep', slept.append)
    thrasher = fake_thrasher({'osd': {'osd oob cutoff late tolerance': 4000}},
                             {'sigstop_min': 1, 'sigstop_max': 6})
    thrasher.sigstop_osd()
    assert thrasher.ceph_manager.signals == [(3, signal.SIGSTOP),
                                             (3, signal.SIGCONT)]
    assert len(slept) == 1 and 5.0 <= slept[0] <= 6.0


def test_sigstop_osd_always_continues(monkeypatch):
    def interrupted(seconds):
        raise KeyboardInterrupt()
    monkeypatch.setattr(ceph_manager.time, 'sleep', interrupted)
    thrasher = fake_thrasher({}, {})
    with pytest.raises(KeyboardInterrupt):
        thrasher.sigstop_osd(osd=7)
    assert thrasher.ceph_manager.signals == [(7, signal.SIGSTOP),
                                             (7, signal.SIGCONT)]


# rdma_rxe

RDMA_LINKS = '''[{"ifindex":2,"ifname":"rxe0","port":1,"state":"ACTIVE",
"physical_state":"LINK_UP","netdev":"eth0","netdev_index":65}
,{"ifindex":3,"ifname":"mlx5_0","port":1,"state":"DOWN",
"physical_state":"DISABLED","netdev":"enp1s0","netdev_index":69}
]'''


def test_link_of_netdev():
    assert rdma_rxe.link_of_netdev(RDMA_LINKS, 'eth0') == 'rxe0'
    assert rdma_rxe.link_of_netdev(RDMA_LINKS, 'enp1s0') == 'mlx5_0'
    assert rdma_rxe.link_of_netdev(RDMA_LINKS, 'eth1') is None
    assert rdma_rxe.link_of_netdev('', 'eth0') is None


def test_port_active():
    assert rdma_rxe.port_active('\t\t\tstate:\t\t\tPORT_ACTIVE (4)\n')
    assert not rdma_rxe.port_active('\t\t\tstate:\t\t\tPORT_DOWN (1)\n')


def test_fi_info_has_domain():
    out = ('provider: verbs;ofi_rxm\n    fabric: IB-0xfe80000000000000\n'
           '    domain: rxe0\n    version: 203.10\n')
    assert rdma_rxe.fi_info_has_domain(out, 'rxe0')
    assert not rdma_rxe.fi_info_has_domain(out, 'rxe')
    assert not rdma_rxe.fi_info_has_domain('fi_getinfo: -61 (No data available)',
                                           'rxe0')


class FakeRxeRemote(object):
    """ a host for rdma_rxe: what it runs, and what its commands give """

    def __init__(self, package_type='rpm', has_module=True, loaded=False,
                 port='PORT_ACTIVE (4)'):
        self.name = self.shortname = 'host1'
        self.interface = 'eth0'
        self.os = types.SimpleNamespace(package_type=package_type)
        self.has_module = has_module
        self.loaded = loaded
        self.port = port
        self.links = []
        self.runs = []

    def run(self, args, check_status=True, **kwargs):
        self.runs.append(args)
        status = 0
        if args[:3] == ['sudo', 'modinfo', '-n']:
            status = 0 if self.has_module else 1
        elif args == ['sudo', 'modprobe', 'rdma_rxe']:
            self.loaded = True
        elif args == ['sudo', 'modprobe', '-r', 'rdma_rxe']:
            self.loaded = False
        elif args[:4] == ['sudo', 'rdma', 'link', 'add']:
            self.links.append({'ifname': args[4], 'netdev': args[-1]})
        elif args[:4] == ['sudo', 'rdma', 'link', 'delete']:
            self.links = [link for link in self.links
                          if link['ifname'] != args[4]]
        return types.SimpleNamespace(exitstatus=status)

    def sh(self, args, check_status=True):
        self.runs.append(args)
        if args == ['uname', '-r']:
            return '5.14.0-570.el9.x86_64\n'
        if args == ['ls', '/sys/module']:
            return 'ib_core\nrdma_rxe\n' if self.loaded else 'ib_core\n'
        if args[:3] == ['sudo', 'rdma', '-j']:
            return json.dumps(self.links)
        if args[0] == 'ibv_devinfo':
            return 'state:\t\t\t%s\n' % self.port
        if args[-2:] == ['-p', 'verbs']:
            return ''.join('domain: %s\n' % link['ifname']
                           for link in self.links)
        return ''


def test_module_package():
    assert rdma_rxe.module_package('rpm', '5.14.0-570.el9.x86_64') == \
        'kernel-modules-extra-5.14.0-570.el9.x86_64'
    assert rdma_rxe.module_package('deb', '6.8.0-45-generic') == \
        'linux-modules-extra-6.8.0-45-generic'
    assert rdma_rxe.module_package('other', '1.0') is None


def test_load_module():
    remote = FakeRxeRemote()
    rdma_rxe.load_module(remote)
    assert ['sudo', 'modprobe', 'rdma_rxe'] in remote.runs
    assert not [a for a in remote.runs if 'install' in a]


def test_load_module_installs_the_running_kernels_package():
    # the package of the running kernel, not the newest one
    remote = FakeRxeRemote(has_module=False)
    rdma_rxe.load_module(remote)
    install = remote.runs.index(['sudo', 'dnf', 'install', '-y',
                                 'kernel-modules-extra-5.14.0-570.el9.x86_64'])
    assert remote.runs.index(['sudo', 'modprobe', 'rdma_rxe']) > install
    remote = FakeRxeRemote(package_type='deb', has_module=False)
    rdma_rxe.load_module(remote)
    assert ['sudo', 'env', 'DEBIAN_FRONTEND=noninteractive', 'apt-get',
            'install', '-y',
            'linux-modules-extra-5.14.0-570.el9.x86_64'] in remote.runs


def rxe_ctx(remote):
    return types.SimpleNamespace(
        config={},
        cluster=types.SimpleNamespace(remotes={remote: ['osd.0']}))


def test_rdma_rxe_task():
    remote = FakeRxeRemote()
    ctx = rxe_ctx(remote)
    with rdma_rxe.task(ctx, {}):
        assert ctx.rdma_rxe == {'host1': 'rxe0'}
        assert remote.links == [{'ifname': 'rxe0', 'netdev': 'eth0'}]
        assert remote.loaded
    assert remote.links == []
    assert not remote.loaded


def test_rdma_rxe_task_keeps_what_it_found():
    remote = FakeRxeRemote(loaded=True)
    remote.links = [{'ifname': 'mlx5_0', 'netdev': 'eth0'}]
    ctx = rxe_ctx(remote)
    with rdma_rxe.task(ctx, {}):
        assert ctx.rdma_rxe == {'host1': 'mlx5_0'}
    assert remote.links == [{'ifname': 'mlx5_0', 'netdev': 'eth0'}]
    assert remote.loaded


def test_rdma_rxe_task_deletes_a_link_whose_check_failed(monkeypatch):
    monkeypatch.setattr(rdma_rxe.time, 'sleep', lambda seconds: None)
    remote = FakeRxeRemote(port='PORT_DOWN (1)')
    with pytest.raises(RuntimeError, match='is not active'):
        with rdma_rxe.task(rxe_ctx(remote), {'timeout': 0}):
            pass
    assert ['sudo', 'rdma', 'link', 'delete', 'rxe0'] in remote.runs
    assert remote.links == []
    assert not remote.loaded


def test_rdma_rxe_task_hardware_uses_the_nic():
    # a ConnectX port: no module, no link added, the NIC's device used
    remote = FakeRxeRemote()
    remote.links = [{'ifname': 'mlx5_0', 'netdev': 'eth0'}]
    ctx = rxe_ctx(remote)
    with rdma_rxe.task(ctx, {'hardware': True}):
        assert ctx.rdma_rxe == {'host1': 'mlx5_0'}
    assert remote.links == [{'ifname': 'mlx5_0', 'netdev': 'eth0'}]
    assert not remote.loaded
    assert not [a for a in remote.runs if 'modprobe' in a or 'modinfo' in a]
    assert not [a for a in remote.runs if a[:4] == ['sudo', 'rdma', 'link', 'add']]


def test_rdma_rxe_task_hardware_fails_without_a_nic():
    # a host whose interface has no RDMA device does not get soft-RoCE
    remote = FakeRxeRemote()
    with pytest.raises(RuntimeError, match='eth0 has no RDMA device'):
        with rdma_rxe.task(rxe_ctx(remote), {'hardware': True}):
            pass
    assert remote.links == []
    assert not remote.loaded


# oob_check

def test_parse_status():
    assert oob_check.parse_status('{"available": true}') == {'available': True}
    assert oob_check.parse_status('') is None
    assert oob_check.parse_status(None) is None
    assert oob_check.parse_status('no valid command found') is None


def test_transport_up():
    status = {'available': True,
              'transport': 'provider tcp, wire xnet.1, fabric 127.0.0.1/32'}
    assert oob_check.transport_up(status)
    assert oob_check.transport_up(status, 'provider tcp')
    assert not oob_check.transport_up(status, 'provider verbs')
    assert not oob_check.transport_up(dict(status, available=False))
    assert not oob_check.transport_up(None)


def test_status_failures():
    statuses = {
        0: {'unsafe': False, 'gather_crc_mismatch': 0, 'cutoffs_failed': 0},
        1: {'unsafe': True, 'last_error': 'close took 3 s',
            'gather_crc_mismatch': 0, 'cutoffs_failed': 1},
        2: None,
        3: {'unsafe': False, 'gather_crc_mismatch': 2},
    }
    failures = oob_check.status_failures(statuses, oob_check.ZERO)
    assert failures == ['osd.1 is unsafe: close took 3 s',
                        'osd.1: cutoffs_failed is 1',
                        'osd.3: gather_crc_mismatch is 2']
    assert oob_check.status_failures({0: statuses[0], 2: None},
                                     oob_check.ZERO) == []


def test_health_failures():
    health = {'checks': {
        'OOB_DELIVERY_UNSAFE': {'summary': {'message': '1 osd unsafe'}},
        'OOB_CUTOFF_LATE': {'summary': {'message': 'late'}}}}
    assert oob_check.health_failures(health, oob_check.HEALTH_CHECKS) == \
        ['health check OOB_DELIVERY_UNSAFE: 1 osd unsafe']
    assert oob_check.health_failures({'checks': {}},
                                     oob_check.HEALTH_CHECKS) == []


def test_log_grep_cmd():
    logs = oob_check.OSD_LOGS.format(cluster='ceph')
    cmd = oob_check.log_grep_cmd(logs, ["it's here", 'b'])
    assert cmd[:3] == ['sudo', 'sh', '-c']
    assert "-e 'it'\"'\"'s here' -e b" in cmd[3]
    # the glob stays unquoted, for the shell to expand
    assert ' /var/log/ceph/ceph-osd.*.log ' in cmd[3]


class FakeProc(object):

    def __init__(self, out, exitstatus=0):
        self.stdout = StringIO(out)
        self.exitstatus = exitstatus


class FakeOSDs(object):
    """ OSDs whose 'ofi status' comes up after some polls """

    def __init__(self, polls):
        self.polls = polls

    def get_osd_dump(self):
        return [{'osd': osd, 'up': 1} for osd in sorted(self.polls)] + \
            [{'osd': 9, 'up': 0}]

    def osd_admin_socket(self, osd, command, check_status=True, timeout=0,
                         stdout=None):
        if self.polls[osd] > 0:
            self.polls[osd] -= 1
            return FakeProc('', 22)
        return FakeProc(json.dumps({'available': True,
                                    'transport': 'provider tcp, wire xnet.1'}))


def fake_check(manager, config):
    ctx = types.SimpleNamespace(managers={'ceph': manager})
    return oob_check.OOBCheck(ctx, config)


def test_wait_for_transports(monkeypatch):
    monkeypatch.setattr(oob_check.time, 'sleep', lambda s: None)
    manager = FakeOSDs({0: 0, 1: 2})
    fake_check(manager, {'transport': 'provider tcp'}).wait_for_transports()
    assert manager.polls == {0: 0, 1: 0}


def test_wait_for_transports_times_out(monkeypatch):
    monkeypatch.setattr(oob_check.time, 'sleep', lambda s: None)
    manager = FakeOSDs({0: 0, 1: 0})
    with pytest.raises(RuntimeError, match='osd.0, osd.1'):
        fake_check(manager, {'transport': 'provider verbs',
                             'timeout': 0}).wait_for_transports()


# rgw_thrash

def test_pick():
    actions = [('a', 1.0), ('b', 2.0)]
    assert rgw_thrash.pick(actions, 0.5) == 'a'
    assert rgw_thrash.pick(actions, 1.5) == 'b'
    assert rgw_thrash.pick(actions, 2.99) == 'b'
    assert rgw_thrash.pick(actions, 3.5) is None
    assert rgw_thrash.pick([('a', 0), ('b', 1)], 0) == 'b'


def test_client_pattern_spares_its_own_command_line():
    import re
    pattern = re.compile(rgw_thrash.CLIENT_PATTERN)
    assert pattern.search('ceph_test_rgw_ofi_soak run tcp - 10.0.0.1')
    assert pattern.search('/usr/bin/ceph_test_rgw_ofi_get tcp -')
    # what sudo and pkill run with
    assert not pattern.search('sudo pkill -KILL -f ' + rgw_thrash.CLIENT_PATTERN)


class FakeRemote(object):

    name = shortname = 'host1'


class FakeDaemon(object):

    def __init__(self):
        self.id_ = 'client.0'
        self.proc = object()
        self.events = []

    def signal(self, sig, silent=False):
        self.events.append(('signal', sig))

    def reset(self):
        self.events.append(('reset',))
        self.proc = None

    def start(self):
        self.events.append(('start',))


def test_kill_rgw(monkeypatch):
    waited = []
    monkeypatch.setattr(rgw_thrash.run, 'wait',
                        lambda procs, timeout=None: waited.append(procs))
    answered = []
    monkeypatch.setattr(rgw_thrash, 'wait_for_radosgw',
                        lambda url, remote: answered.append(url))
    daemon = FakeDaemon()
    remote = FakeRemote()
    endpoint = types.SimpleNamespace(url=lambda: 'http://host1:80/')
    ctx = types.SimpleNamespace(
        cluster=types.SimpleNamespace(
            only=lambda role: types.SimpleNamespace(remotes={remote: [role]})),
        rgw=types.SimpleNamespace(role_endpoints={'client.0': endpoint}))
    thrasher = rgw_thrash.RGWThrasher(ctx, {'max_revive_delay': 0}, 'ceph',
                                      [daemon])
    assert thrasher.client_remotes == {remote}
    thrasher.kill_rgw()
    assert daemon.events == [('signal', signal.SIGKILL), ('reset',),
                             ('start',)]
    assert len(waited) == 1
    assert answered == ['http://host1:80/']
    assert thrasher.stats['kill_rgw'] == 1
