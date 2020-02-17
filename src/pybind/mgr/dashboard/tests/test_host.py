import unittest

try:
    import mock
except ImportError:
    from unittest import mock

from orchestrator import InventoryNode

from . import ControllerTestCase
from ..controllers.host import get_hosts, Host
from .. import mgr


class HostControllerTest(ControllerTestCase):
    URL_HOST = '/api/host'

    @classmethod
    def setup_server(cls):
        # pylint: disable=protected-access
        Host._cp_config['tools.authenticate.on'] = False
        cls.setup_controllers([Host])

    @mock.patch('dashboard.controllers.host.get_hosts')
    def test_host_list(self, mock_get_hosts):
        hosts = [
            {
                'hostname': 'host-0',
                'sources': {
                    'ceph': True, 'orchestrator': False
                }
            },
            {
                'hostname': 'host-1',
                'sources': {
                    'ceph': False, 'orchestrator': True
                }
            },
            {
                'hostname': 'host-2',
                'sources': {
                    'ceph': True, 'orchestrator': True
                }
            }
        ]

        def _get_hosts(from_ceph=True, from_orchestrator=True):
            _hosts = []
            if from_ceph:
                _hosts.append(hosts[0])
            if from_orchestrator:
                _hosts.append(hosts[1])
                _hosts.append(hosts[2])
            return _hosts
        mock_get_hosts.side_effect = _get_hosts

        self._get(self.URL_HOST)
        self.assertStatus(200)
        self.assertJsonBody(hosts)

        self._get('{}?sources=ceph'.format(self.URL_HOST))
        self.assertStatus(200)
        self.assertJsonBody([hosts[0]])

        self._get('{}?sources=orchestrator'.format(self.URL_HOST))
        self.assertStatus(200)
        self.assertJsonBody(hosts[1:])

        self._get('{}?sources=ceph,orchestrator'.format(self.URL_HOST))
        self.assertStatus(200)
        self.assertJsonBody(hosts)


class TestHosts(unittest.TestCase):

    @mock.patch('dashboard.controllers.orchestrator.OrchClient.instance')
    def test_get_hosts(self, instance):
        mgr.list_servers.return_value = [{'hostname': 'node1'}, {'hostname': 'localhost'}]

        fake_client = mock.Mock()
        fake_client.available.return_value = True
        fake_client.hosts.list.return_value = [
            InventoryNode('node1'), InventoryNode('node2')]
        instance.return_value = fake_client

        hosts = get_hosts()
        self.assertEqual(len(hosts), 3)
        check_sources = {
            'localhost': {'ceph': True, 'orchestrator': False},
            'node1': {'ceph': True, 'orchestrator': True},
            'node2': {'ceph': False, 'orchestrator': True}
        }
        for host in hosts:
            hostname = host['hostname']
            sources = host['sources']
            self.assertDictEqual(sources, check_sources[hostname])
