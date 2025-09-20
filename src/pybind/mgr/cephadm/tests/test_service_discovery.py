from unittest.mock import MagicMock
from cephadm.services.service_discovery import Root
from cephadm.services.service_registry import service_registry


class FakeDaemonDescription:
    def __init__(self, ip, ports, hostname, service_name='', daemon_type='', daemon_id=''):
        self.ip = ip
        self.ports = ports
        self.hostname = hostname
        self._service_name = service_name
        self.daemon_type = daemon_type
        self.daemon_id = daemon_id if daemon_id else hostname

    def service_name(self):
        return self._service_name


class FakeCache:
    def get_daemons_by_service(self, service_type):
        if service_type == 'ceph-exporter':
            return [FakeDaemonDescription('1.2.3.4', [9926], 'node0'),
                    FakeDaemonDescription('1.2.3.5', [9926], 'node1')]
        if service_type == 'nvmeof':
            return [FakeDaemonDescription('1.2.3.4', [10008], 'node0'),
                    FakeDaemonDescription('1.2.3.5', [10008], 'node1')]

        if service_type == 'nfs':
            return [FakeDaemonDescription('1.2.3.4', [9587], 'node0'),
                    FakeDaemonDescription('1.2.3.5', [9587], 'node1')]

        if service_type == 'smb':
            return [FakeDaemonDescription('1.2.3.4', [9922], 'node0'),
                    FakeDaemonDescription('1.2.3.5', [9922], 'node1')]

        if service_type == 'container.custom-container':
            return [FakeDaemonDescription('1.2.3.4', [9123], 'node0'),
                    FakeDaemonDescription('1.2.3.5', [9123], 'node1')]

        if service_type == 'mgr':
            return [FakeDaemonDescription('1.2.3.4', [9922], 'node0', daemon_type='mgr', daemon_id='fake_active_mgr'),
                    FakeDaemonDescription('1.2.3.5', [9922], 'node1', daemon_type='mgr', daemon_id='fake_standby_mgr')]

        return [FakeDaemonDescription('1.2.3.4', [9100], 'node0'),
                FakeDaemonDescription('1.2.3.5', [9200], 'node1')]

    def get_daemons_by_type(self, daemon_type):
        if daemon_type == 'ingress':
            return [FakeDaemonDescription('1.2.3.4', [9100], 'node0', 'ingress', 'haproxy'),
                    FakeDaemonDescription('1.2.3.5', [9200], 'node1', 'ingress', 'haproxy')]
        else:
            return [FakeDaemonDescription('1.2.3.4', [1234], 'node0', daemon_type, daemon_type),
                    FakeDaemonDescription('1.2.3.5', [1234], 'node1', daemon_type, daemon_type)]


class FakeInventory:
    def get_addr(self, name: str):
        return '1.2.3.4'


class FakeNFSServiceSpec:
    def __init__(self, port):
        self.monitoring_port = None
        self.monitoring_ip_addrs = None
        self.monitoring_networks = None


class FakeIngressServiceSpec:
    def __init__(self, port):
        self.monitor_port = port


class FakeServiceSpec:
    def __init__(self, port):
        self.monitor_port = port

    def metrics_exporter_port(self):
        # TODO: for smb only
        return 9922


class FakeSpecDescription:
    def __init__(self, service, port):
        if service == 'ingress':
            self.spec = FakeIngressServiceSpec(port)
        elif service == 'nfs':
            self.spec = FakeNFSServiceSpec(port)
        else:
            self.spec = FakeServiceSpec(port)


class FakeSpecStore():
    def __init__(self, mgr):
        self.mgr = mgr
        self._specs = {'ingress': FakeSpecDescription('ingress', 9049), 'nfs': FakeSpecDescription('nfs', 9587), 'smb': FakeSpecDescription('smb', 9922)}

    def __contains__(self, name):
        return name in self._specs

    def __getitem__(self, name):
        return self._specs[name]


class FakeMgr:
    def __init__(self):
        self.config = ''
        self.check_mon_command = MagicMock(side_effect=self._check_mon_command)
        self.mon_command = MagicMock(side_effect=self._check_mon_command)
        self.template = MagicMock()
        self.log = MagicMock()
        self.inventory = FakeInventory()
        self.cache = FakeCache()
        self.spec_store = FakeSpecStore(self)
        service_registry.init_services(self)

    def get_mgr_id(self):
        return 'mgr-1'

    def _check_mon_command(self, cmd_dict, inbuf=None):
        prefix = cmd_dict.get('prefix')
        if prefix == 'get-cmd':
            return 0, self.config, ''
        if prefix == 'set-cmd':
            self.config = cmd_dict.get('value')
            return 0, 'value set', ''
        return -1, '', 'error'

    def get_module_option_ex(self, module, option, default_value):
        return "9283"

    def daemon_is_self(self, d_type, d_id) -> bool:
        if d_type == 'mgr' and d_id == 'fake_active_mgr':
            return True
        return False

    def get_fqdn(self, hostname: str) -> str:
        return hostname


class TestServiceDiscovery:

    def test_get_sd_config_prometheus(self):
        mgr = FakeMgr()
        root = Root(mgr, 5000, '0.0.0.0')
        cfg = root.get_sd_config('mgr-prometheus')

        # check response structure
        assert cfg
        for entry in cfg:
            assert 'labels' in entry
            assert 'targets' in entry

        # check content
        assert cfg[0]['targets'] == ['node0:9283']

    def test_get_sd_config_node_exporter(self):
        mgr = FakeMgr()
        root = Root(mgr, 5000, '0.0.0.0')
        cfg = root.get_sd_config('node-exporter')

        # check response structure
        assert cfg
        for entry in cfg:
            assert 'labels' in entry
            assert 'targets' in entry

        # check content
        assert cfg[0]['targets'] == ['1.2.3.4:9100']
        assert cfg[0]['labels'] == {'instance': 'node0'}
        assert cfg[1]['targets'] == ['1.2.3.5:9200']
        assert cfg[1]['labels'] == {'instance': 'node1'}

    def test_get_sd_config_alertmgr(self):
        mgr = FakeMgr()
        root = Root(mgr, 5000, '0.0.0.0')
        cfg = root.get_sd_config('alertmanager')

        # check response structure
        assert cfg
        for entry in cfg:
            assert 'labels' in entry
            assert 'targets' in entry

        # check content
        assert cfg[0]['targets'] == ['1.2.3.4:9100', '1.2.3.5:9200']

    def test_get_sd_config_haproxy(self):
        mgr = FakeMgr()
        root = Root(mgr, 5000, '0.0.0.0')
        cfg = root.get_sd_config('haproxy')

        # check response structure
        assert cfg
        for entry in cfg:
            assert 'labels' in entry
            assert 'targets' in entry

        # check content
        assert cfg[0]['targets'] == ['1.2.3.4:9049']
        assert cfg[0]['labels'] == {'instance': 'node0', 'ingress': 'ingress'}
        assert cfg[1]['labels'] == {'instance': 'node1', 'ingress': 'ingress'}

    def test_get_sd_config_ceph_exporter(self):
        mgr = FakeMgr()
        root = Root(mgr, 5000, '0.0.0.0')
        cfg = root.get_sd_config('ceph-exporter')

        # check response structure
        assert cfg
        for entry in cfg:
            assert 'labels' in entry
            assert 'targets' in entry

        # check content
        assert cfg[0]['targets'] == ['1.2.3.4:9926']

    def test_get_sd_config_nvmeof(self):
        mgr = FakeMgr()
        root = Root(mgr, 5000, '0.0.0.0')
        cfg = root.get_sd_config('nvmeof')

        # check response structure
        assert cfg
        for entry in cfg:
            assert 'labels' in entry
            assert 'targets' in entry

        # check content
        assert cfg[0]['targets'] == ['1.2.3.4:10008']

    def test_get_sd_config_nfs(self):
        mgr = FakeMgr()
        root = Root(mgr, 5000, '0.0.0.0')
        cfg = root.get_sd_config('nfs')

        # check response structure
        assert cfg
        for entry in cfg:
            assert 'labels' in entry
            assert 'targets' in entry

        # check content
        assert cfg[0]['targets'] == ['1.2.3.4:9587']

    def test_get_sd_config_smb(self):
        mgr = FakeMgr()
        root = Root(mgr, 5000, '0.0.0.0')
        cfg = root.get_sd_config('smb')

        # check response structure
        assert cfg
        for entry in cfg:
            assert 'labels' in entry
            assert 'targets' in entry

        # check content
        assert cfg[0]['targets'] == ['1.2.3.4:9922']

    def test_get_sd_config_custom_container(self):
        mgr = FakeMgr()
        root = Root(mgr, 5000, '0.0.0.0')
        cfg = root.get_sd_config('container.custom-container')

        # check response structure
        assert cfg
        for entry in cfg:
            assert 'labels' in entry
            assert 'targets' in entry

        # check content
        assert cfg[0]['targets'] == ['1.2.3.4:9123']

    def test_get_sd_config_invalid_service(self):
        mgr = FakeMgr()
        root = Root(mgr, 5000, '0.0.0.0')
        cfg = root.get_sd_config('invalid-service')
        assert cfg == []
