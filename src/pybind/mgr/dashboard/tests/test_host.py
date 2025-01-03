import unittest
from unittest import mock

from orchestrator import DaemonDescription, HostSpec

from .. import mgr
from ..controllers._version import APIVersion
from ..controllers.host import Host, HostUi, get_device_osd_map, get_hosts, get_inventories
from ..tests import ControllerTestCase, patch_orch
from ..tools import NotificationQueue, TaskManager


class HostControllerTest(ControllerTestCase):
    URL_HOST = '/api/host'

    @classmethod
    def setup_server(cls):
        NotificationQueue.start_queue()
        TaskManager.init()
        cls.setup_controllers([Host])

    @classmethod
    def tearDownClass(cls):
        NotificationQueue.stop()

    @mock.patch('dashboard.controllers.host.get_hosts')
    def test_host_list_with_sources(self, mock_get_hosts):
        hosts = [{
            'hostname': 'host-0',
            'sources': {
                'ceph': True,
                'orchestrator': False
            }
        }, {
            'hostname': 'host-1',
            'sources': {
                'ceph': False,
                'orchestrator': True
            }
        }, {
            'hostname': 'host-2',
            'sources': {
                'ceph': True,
                'orchestrator': True
            }
        }]

        def _get_hosts(sources=None):
            if sources == 'ceph':
                return [hosts[0]]
            if sources == 'orchestrator':
                return hosts[1:]
            if sources == 'ceph, orchestrator':
                return [hosts[2]]
            return hosts

        with patch_orch(True, hosts=hosts):
            mock_get_hosts.side_effect = _get_hosts
            self._get(self.URL_HOST, version=APIVersion(1, 1))
            self.assertStatus(200)
            self.assertJsonBody(hosts)

            self._get('{}?sources=ceph'.format(self.URL_HOST), version=APIVersion(1, 1))
            self.assertStatus(200)
            self.assertJsonBody([hosts[0]])

            self._get('{}?sources=orchestrator'.format(self.URL_HOST), version=APIVersion(1, 1))
            self.assertStatus(200)
            self.assertJsonBody(hosts[1:])

            self._get('{}?sources=ceph,orchestrator'.format(self.URL_HOST),
                      version=APIVersion(1, 1))
            self.assertStatus(200)
            self.assertJsonBody(hosts)

    @mock.patch('dashboard.controllers.host.get_hosts')
    def test_host_list_with_facts(self, mock_get_hosts):
        hosts_without_facts = [{
            'hostname': 'host-0',
            'sources': {
                'ceph': True,
                'orchestrator': False
            }
        }, {
            'hostname': 'host-1',
            'sources': {
                'ceph': False,
                'orchestrator': True
            }
        }]

        hosts_facts = [{
            'hostname': 'host-0',
            'cpu_count': 1,
            'memory_total_kb': 1024
        }, {
            'hostname': 'host-1',
            'cpu_count': 2,
            'memory_total_kb': 1024
        }]

        hosts_with_facts = [{
            'hostname': 'host-0',
            'sources': {
                'ceph': True,
                'orchestrator': False
            },
            'cpu_count': 1,
            'memory_total_kb': 1024,
            'services': [],
            'service_instances': [{'type': 'mon', 'count': 1}]
        }, {
            'hostname': 'host-1',
            'sources': {
                'ceph': False,
                'orchestrator': True
            },
            'cpu_count': 2,
            'memory_total_kb': 1024,
            'services': [],
            'service_instances': [{'type': 'mon', 'count': 1}]
        }]
        # test with orchestrator available
        with patch_orch(True, hosts=hosts_without_facts) as fake_client:
            mock_get_hosts.return_value = hosts_without_facts

            def get_facts_mock(hostname: str):
                if hostname == 'host-0':
                    return [hosts_facts[0]]
                return [hosts_facts[1]]
            fake_client.hosts.get_facts.side_effect = get_facts_mock
            # test with ?facts=true
            self._get('{}?facts=true'.format(self.URL_HOST), version=APIVersion(1, 3))
            self.assertStatus(200)
            self.assertHeader('Content-Type',
                              APIVersion(1, 3).to_mime_type())
            self.assertJsonBody(hosts_with_facts)

            # test with ?facts=false
            self._get('{}?facts=false'.format(self.URL_HOST), version=APIVersion(1, 3))
            self.assertStatus(200)
            self.assertHeader('Content-Type',
                              APIVersion(1, 3).to_mime_type())
            self.assertJsonBody(hosts_without_facts)

        # test with orchestrator available but orch backend!=cephadm
        with patch_orch(True, missing_features=['get_facts']) as fake_client:
            mock_get_hosts.return_value = hosts_without_facts
            # test with ?facts=true
            self._get('{}?facts=true'.format(self.URL_HOST), version=APIVersion(1, 3))
            self.assertStatus(400)

        # test with no orchestrator available
        with patch_orch(False):
            mock_get_hosts.return_value = hosts_without_facts

            # test with ?facts=true
            self._get('{}?facts=true'.format(self.URL_HOST), version=APIVersion(1, 3))
            self.assertStatus(400)

            # test with ?facts=false
            self._get('{}?facts=false'.format(self.URL_HOST), version=APIVersion(1, 3))
            self.assertStatus(200)
            self.assertHeader('Content-Type',
                              APIVersion(1, 3).to_mime_type())
            self.assertJsonBody(hosts_without_facts)

    def test_get_1(self):
        mgr.list_servers.return_value = []

        with patch_orch(False):
            self._get('{}/node1'.format(self.URL_HOST))
            self.assertStatus(404)

    def test_get_2(self):
        mgr.list_servers.return_value = [{
            'hostname': 'node1',
            'services': []
        }]

        with patch_orch(False):
            self._get('{}/node1'.format(self.URL_HOST))
            self.assertStatus(200)
            self.assertIn('labels', self.json_body())
            self.assertIn('status', self.json_body())
            self.assertIn('addr', self.json_body())

    def test_get_3(self):
        mgr.list_servers.return_value = []

        with patch_orch(True, hosts=[HostSpec('node1')]):
            self._get('{}/node1'.format(self.URL_HOST))
            self.assertStatus(200)
            self.assertIn('labels', self.json_body())
            self.assertIn('status', self.json_body())
            self.assertIn('addr', self.json_body())

    def test_populate_service_instances(self):
        mgr.list_servers.return_value = []

        node1_daemons = [
            DaemonDescription(
                hostname='node1',
                daemon_type='mon',
                daemon_id='a'
            ),
            DaemonDescription(
                hostname='node1',
                daemon_type='mon',
                daemon_id='b'
            )
        ]

        node2_daemons = [
            DaemonDescription(
                hostname='node2',
                daemon_type='mgr',
                daemon_id='x'
            ),
            DaemonDescription(
                hostname='node2',
                daemon_type='mon',
                daemon_id='c'
            )
        ]

        node1_instances = [{
            'type': 'mon',
            'count': 2
        }]

        node2_instances = [{
            'type': 'mgr',
            'count': 1
        }, {
            'type': 'mon',
            'count': 1
        }]

        # test with orchestrator available
        with patch_orch(True,
                        hosts=[HostSpec('node1'), HostSpec('node2')]) as fake_client:
            fake_client.services.list_daemons.return_value = node1_daemons
            self._get('{}/node1'.format(self.URL_HOST))
            self.assertStatus(200)
            self.assertIn('service_instances', self.json_body())
            self.assertEqual(self.json_body()['service_instances'], node1_instances)

            fake_client.services.list_daemons.return_value = node2_daemons
            self._get('{}/node2'.format(self.URL_HOST))
            self.assertStatus(200)
            self.assertIn('service_instances', self.json_body())
            self.assertEqual(self.json_body()['service_instances'], node2_instances)

        # test with no orchestrator available
        with patch_orch(False):
            mgr.list_servers.return_value = [{
                'hostname': 'node1',
                'services': [{
                    'type': 'mon',
                    'id': 'a'
                }, {
                    'type': 'mgr',
                    'id': 'b'
                }]
            }]
            self._get('{}/node1'.format(self.URL_HOST))
            self.assertStatus(200)
            self.assertIn('service_instances', self.json_body())
            self.assertEqual(self.json_body()['service_instances'],
                             [{
                                 'type': 'mon',
                                 'count': 1
                             }, {
                                 'type': 'mgr',
                                 'count': 1
                             }])

    @mock.patch('dashboard.controllers.host.add_host')
    def test_add_host(self, mock_add_host):
        with patch_orch(True):
            payload = {
                'hostname': 'node0',
                'addr': '192.0.2.0',
                'labels': 'mon',
                'status': 'maintenance'
            }
            self._post(self.URL_HOST, payload, version=APIVersion(0, 1))
            self.assertStatus(201)
            mock_add_host.assert_called()

    def test_set_labels(self):
        mgr.list_servers.return_value = []
        orch_hosts = [
            HostSpec('node0', labels=['aaa', 'bbb'])
        ]
        with patch_orch(True, hosts=orch_hosts) as fake_client:
            fake_client.hosts.remove_label = mock.Mock()
            fake_client.hosts.add_label = mock.Mock()

            payload = {'update_labels': True, 'labels': ['bbb', 'ccc']}
            self._put('{}/node0'.format(self.URL_HOST), payload, version=APIVersion(0, 1))
            self.assertStatus(200)
            self.assertHeader('Content-Type',
                              'application/vnd.ceph.api.v0.1+json')
            fake_client.hosts.remove_label.assert_called_once_with('node0', 'aaa')
            fake_client.hosts.add_label.assert_called_once_with('node0', 'ccc')

            # return 400 if type other than List[str]
            self._put('{}/node0'.format(self.URL_HOST),
                      {'update_labels': True, 'labels': 'ddd'},
                      version=APIVersion(0, 1))
            self.assertStatus(400)

    def test_host_maintenance(self):
        mgr.list_servers.return_value = []
        orch_hosts = [
            HostSpec('node0'),
            HostSpec('node1')
        ]
        with patch_orch(True, hosts=orch_hosts):
            # enter maintenance mode
            self._put('{}/node0'.format(self.URL_HOST), {'maintenance': True},
                      version=APIVersion(0, 1))
            self.assertStatus(200)
            self.assertHeader('Content-Type',
                              'application/vnd.ceph.api.v0.1+json')

            # force enter maintenance mode
            self._put('{}/node1'.format(self.URL_HOST), {'maintenance': True, 'force': True},
                      version=APIVersion(0, 1))
            self.assertStatus(200)

            # exit maintenance mode
            self._put('{}/node0'.format(self.URL_HOST), {'maintenance': True},
                      version=APIVersion(0, 1))
            self.assertStatus(200)
            self._put('{}/node1'.format(self.URL_HOST), {'maintenance': True},
                      version=APIVersion(0, 1))
            self.assertStatus(200)

        # maintenance without orchestrator service
        with patch_orch(False):
            self._put('{}/node0'.format(self.URL_HOST), {'maintenance': True},
                      version=APIVersion(0, 1))
            self.assertStatus(503)

    @mock.patch('dashboard.controllers.host.time')
    def test_identify_device(self, mock_time):
        url = '{}/host-0/identify_device'.format(self.URL_HOST)
        with patch_orch(True) as fake_client:
            payload = {
                'device': '/dev/sdz',
                'duration': '1'
            }
            self._task_post(url, payload)
            self.assertStatus(200)
            mock_time.sleep.assert_called()
            calls = [
                mock.call('host-0', '/dev/sdz', 'ident', True),
                mock.call('host-0', '/dev/sdz', 'ident', False),
            ]
            fake_client.blink_device_light.assert_has_calls(calls)

    @mock.patch('dashboard.controllers.host.get_inventories')
    def test_inventory(self, mock_get_inventories):
        inventory_url = '{}/host-0/inventory'.format(self.URL_HOST)
        with patch_orch(True):
            tests = [
                {
                    'url': inventory_url,
                    'inventories': [{'a': 'b'}],
                    'refresh': None,
                    'resp': {'a': 'b'}
                },
                {
                    'url': '{}?refresh=true'.format(inventory_url),
                    'inventories': [{'a': 'b'}],
                    'refresh': "true",
                    'resp': {'a': 'b'}
                },
                {
                    'url': inventory_url,
                    'inventories': [],
                    'refresh': None,
                    'resp': {}
                },
            ]
            for test in tests:
                mock_get_inventories.reset_mock()
                mock_get_inventories.return_value = test['inventories']
                self._get(test['url'])
                mock_get_inventories.assert_called_once_with(['host-0'], test['refresh'])
                self.assertEqual(self.json_body(), test['resp'])
                self.assertStatus(200)

        # list without orchestrator service
        with patch_orch(False):
            self._get(inventory_url)
            self.assertStatus(503)

    def test_host_drain(self):
        mgr.list_servers.return_value = []
        orch_hosts = [
            HostSpec('node0')
        ]
        with patch_orch(True, hosts=orch_hosts):
            self._put('{}/node0'.format(self.URL_HOST), {'drain': True},
                      version=APIVersion(0, 1))
            self.assertStatus(200)
            self.assertHeader('Content-Type',
                              'application/vnd.ceph.api.v0.1+json')

        # maintenance without orchestrator service
        with patch_orch(False):
            self._put('{}/node0'.format(self.URL_HOST), {'drain': True},
                      version=APIVersion(0, 1))
            self.assertStatus(503)


class HostUiControllerTest(ControllerTestCase):
    URL_HOST = '/ui-api/host'

    @classmethod
    def setup_server(cls):
        cls.setup_controllers([HostUi])

    def test_labels(self):
        orch_hosts = [
            HostSpec('node1', labels=['foo']),
            HostSpec('node2', labels=['foo', 'bar'])
        ]

        with patch_orch(True, hosts=orch_hosts):
            self._get('{}/labels'.format(self.URL_HOST))
            self.assertStatus(200)
            labels = self.json_body()
            labels.sort()
            self.assertListEqual(labels, ['bar', 'foo'])

    @mock.patch('dashboard.controllers.host.get_inventories')
    def test_inventory(self, mock_get_inventories):
        inventory_url = '{}/inventory'.format(self.URL_HOST)
        with patch_orch(True):
            tests = [
                {
                    'url': inventory_url,
                    'refresh': None
                },
                {
                    'url': '{}?refresh=true'.format(inventory_url),
                    'refresh': "true"
                },
            ]
            for test in tests:
                mock_get_inventories.reset_mock()
                mock_get_inventories.return_value = [{'a': 'b'}]
                self._get(test['url'])
                mock_get_inventories.assert_called_once_with(None, test['refresh'])
                self.assertEqual(self.json_body(), [{'a': 'b'}])
                self.assertStatus(200)

        # list without orchestrator service
        with patch_orch(False):
            self._get(inventory_url)
            self.assertStatus(503)


class TestHosts(unittest.TestCase):
    def test_get_hosts(self):
        mgr.list_servers.return_value = [{
            'hostname': 'node1',
            'services': []
        }, {
            'hostname': 'localhost',
            'services': []
        }]
        orch_hosts = [
            HostSpec('node1', labels=['foo', 'bar']),
            HostSpec('node2', labels=['bar'])
        ]

        with patch_orch(True, hosts=orch_hosts):
            hosts = get_hosts()
            self.assertEqual(len(hosts), 2)
            checks = {
                'node1': {
                    'sources': {
                        'ceph': False,
                        'orchestrator': True
                    },
                    'labels': ['foo', 'bar']
                },
                'node2': {
                    'sources': {
                        'ceph': False,
                        'orchestrator': True
                    },
                    'labels': ['bar']
                }
            }
            for host in hosts:
                hostname = host['hostname']
                self.assertDictEqual(host['sources'], checks[hostname]['sources'])
                self.assertListEqual(host['labels'], checks[hostname]['labels'])

    @mock.patch('dashboard.controllers.host.mgr.get')
    def test_get_device_osd_map(self, mgr_get):
        mgr_get.side_effect = lambda key: {
            'osd_metadata': {
                '0': {
                    'hostname': 'node0',
                    'devices': 'nvme0n1,sdb',
                },
                '1': {
                    'hostname': 'node0',
                    'devices': 'nvme0n1,sdc',
                },
                '2': {
                    'hostname': 'node1',
                    'devices': 'sda',
                },
                '3': {
                    'hostname': 'node2',
                    'devices': '',
                }
            }
        }[key]

        device_osd_map = get_device_osd_map()
        mgr.get.assert_called_with('osd_metadata')
        # sort OSD IDs to make assertDictEqual work
        for devices in device_osd_map.values():
            for host in devices.keys():
                devices[host] = sorted(devices[host])
        self.assertDictEqual(device_osd_map, {
            'node0': {
                'nvme0n1': [0, 1],
                'sdb': [0],
                'sdc': [1],
            },
            'node1': {
                'sda': [2]
            }
        })

    @mock.patch('dashboard.controllers.host.str_to_bool')
    @mock.patch('dashboard.controllers.host.get_device_osd_map')
    def test_get_inventories(self, mock_get_device_osd_map, mock_str_to_bool):
        mock_get_device_osd_map.return_value = {
            'host-0': {
                'nvme0n1': [1, 2],
                'sdb': [1],
                'sdc': [2]
            },
            'host-1': {
                'sdb': [3]
            }
        }
        inventory = [
            {
                'name': 'host-0',
                'addr': '1.2.3.4',
                'devices': [
                    {'path': 'nvme0n1'},
                    {'path': '/dev/sdb'},
                    {'path': '/dev/sdc'},
                ]
            },
            {
                'name': 'host-1',
                'addr': '1.2.3.5',
                'devices': [
                    {'path': '/dev/sda'},
                    {'path': 'sdb'},
                ]
            }
        ]

        with patch_orch(True, inventory=inventory) as orch_client:
            mock_str_to_bool.return_value = True

            hosts = ['host-0', 'host-1']
            inventories = get_inventories(hosts, 'true')
            mock_str_to_bool.assert_called_with('true')
            orch_client.inventory.list.assert_called_once_with(hosts=hosts, refresh=True)
            self.assertEqual(len(inventories), 2)
            host0 = inventories[0]
            self.assertEqual(host0['name'], 'host-0')
            self.assertEqual(host0['addr'], '1.2.3.4')
            # devices should be sorted by path name, so
            # /dev/sdb, /dev/sdc, nvme0n1
            self.assertEqual(host0['devices'][0]['osd_ids'], [1])
            self.assertEqual(host0['devices'][1]['osd_ids'], [2])
            self.assertEqual(host0['devices'][2]['osd_ids'], [1, 2])
            host1 = inventories[1]
            self.assertEqual(host1['name'], 'host-1')
            self.assertEqual(host1['addr'], '1.2.3.5')
            # devices should be sorted by path name, so
            # /dev/sda, sdb
            self.assertEqual(host1['devices'][0]['osd_ids'], [])
            self.assertEqual(host1['devices'][1]['osd_ids'], [3])
