from unittest.mock import MagicMock
from cephadm.service_discovery import Root


class FakeDaemonDescription:
    def __init__(self, ip, ports, hostname, service_name='', daemon_type=''):
        self.ip = ip
        self.ports = ports
        self.hostname = hostname
        self._service_name = service_name
        self.daemon_type = daemon_type

    def service_name(self):
        return self._service_name


class FakeCache:
    def get_daemons_by_service(self, service_type):
        if service_type == 'ceph-exporter':
            return [FakeDaemonDescription('1.2.3.4', [9926], 'node0'),
                    FakeDaemonDescription('1.2.3.5', [9926], 'node1')]

        return [FakeDaemonDescription('1.2.3.4', [9100], 'node0'),
                FakeDaemonDescription('1.2.3.5', [9200], 'node1')]

    def get_daemons_by_type(self, daemon_type):
        return [FakeDaemonDescription('1.2.3.4', [9100], 'node0', 'ingress', 'haproxy'),
                FakeDaemonDescription('1.2.3.5', [9200], 'node1', 'ingress', 'haproxy')]


class FakeInventory:
    def get_addr(self, name: str):
        return '1.2.3.4'


class FakeServiceSpec:
    def __init__(self, port):
        self.monitor_port = port


class FakeSpecDescription:
    def __init__(self, port):
        self.spec = FakeServiceSpec(port)


class FakeSpecStore():
    def __init__(self, mgr):
        self.mgr = mgr
        self._specs = {'ingress': FakeSpecDescription(9049)}

    def __contains__(self, name):
        return name in self._specs

    def __getitem__(self, name):
        return self._specs['ingress']


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

    def get_mgr_id(self):
        return 'mgr-1'

    def list_servers(self):

        servers = [
            {'hostname': 'node0',
             'ceph_version': '16.2',
             'services': [{'type': 'mgr', 'id': 'mgr-1'}, {'type': 'mon'}]},
            {'hostname': 'node1',
             'ceph_version': '16.2',
             'services': [{'type': 'mgr', 'id': 'mgr-2'}, {'type': 'mon'}]}
        ]

        return servers

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
        assert cfg[0]['labels'] == {'instance': 'ingress'}

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

    def test_get_sd_config_invalid_service(self):
        mgr = FakeMgr()
        root = Root(mgr, 5000, '0.0.0.0')
        cfg = root.get_sd_config('invalid-service')
        assert cfg == []
