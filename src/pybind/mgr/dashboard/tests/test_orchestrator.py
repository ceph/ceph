import unittest
try:
    import mock
except ImportError:
    from unittest import mock

from orchestrator import InventoryNode, ServiceDescription

from . import ControllerTestCase
from .. import mgr
from ..controllers.orchestrator import get_device_osd_map
from ..controllers.orchestrator import Orchestrator
from ..controllers.orchestrator import OrchestratorInventory
from ..controllers.orchestrator import OrchestratorOsd
from ..controllers.orchestrator import OrchestratorService


class OrchestratorControllerTest(ControllerTestCase):
    URL_STATUS = '/api/orchestrator/status'
    URL_INVENTORY = '/api/orchestrator/inventory'
    URL_SERVICE = '/api/orchestrator/service'
    URL_OSD = '/api/orchestrator/osd'

    @classmethod
    def setup_server(cls):
        # pylint: disable=protected-access
        Orchestrator._cp_config['tools.authenticate.on'] = False
        OrchestratorInventory._cp_config['tools.authenticate.on'] = False
        OrchestratorService._cp_config['tools.authenticate.on'] = False
        OrchestratorOsd._cp_config['tools.authenticate.on'] = False
        cls.setup_controllers([Orchestrator,
                               OrchestratorInventory,
                               OrchestratorService,
                               OrchestratorOsd])

    @mock.patch('dashboard.controllers.orchestrator.OrchClient.instance')
    def test_status_get(self, instance):
        status = {'available': False, 'description': ''}

        fake_client = mock.Mock()
        fake_client.status.return_value = status
        instance.return_value = fake_client

        self._get(self.URL_STATUS)
        self.assertStatus(200)
        self.assertJsonBody(status)

    def _set_inventory(self, mock_instance, inventory):
        # pylint: disable=unused-argument
        def _list_inventory(hosts=None, refresh=False):
            nodes = []
            for node in inventory:
                if hosts is None or node['name'] in hosts:
                    nodes.append(InventoryNode.from_json(node))
            return nodes
        mock_instance.inventory.list.side_effect = _list_inventory

    @mock.patch('dashboard.controllers.orchestrator.get_device_osd_map')
    @mock.patch('dashboard.controllers.orchestrator.OrchClient.instance')
    def test_inventory_list(self, instance, get_dev_osd_map):
        get_dev_osd_map.return_value = {
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
                'devices': [
                    {'path': 'nvme0n1'},
                    {'path': '/dev/sdb'},
                    {'path': '/dev/sdc'},
                ]
            },
            {
                'name': 'host-1',
                'devices': [
                    {'path': '/dev/sda'},
                    {'path': 'sdb'},
                ]
            }
        ]
        fake_client = mock.Mock()
        fake_client.available.return_value = True
        self._set_inventory(fake_client, inventory)
        instance.return_value = fake_client

        # list
        self._get(self.URL_INVENTORY)
        self.assertStatus(200)
        resp = self.json_body()
        self.assertEqual(len(resp), 2)
        host0 = resp[0]
        self.assertEqual(host0['name'], 'host-0')
        self.assertEqual(host0['devices'][0]['osd_ids'], [1, 2])
        self.assertEqual(host0['devices'][1]['osd_ids'], [1])
        self.assertEqual(host0['devices'][2]['osd_ids'], [2])
        host1 = resp[1]
        self.assertEqual(host1['name'], 'host-1')
        self.assertEqual(host1['devices'][0]['osd_ids'], [])
        self.assertEqual(host1['devices'][1]['osd_ids'], [3])

        # list with existent hostname
        self._get('{}?hostname=host-0'.format(self.URL_INVENTORY))
        self.assertStatus(200)
        self.assertEqual(self.json_body()[0]['name'], 'host-0')

        # list with non-existent inventory
        self._get('{}?hostname=host-10'.format(self.URL_INVENTORY))
        self.assertStatus(200)
        self.assertJsonBody([])

        # list without orchestrator service
        fake_client.available.return_value = False
        self._get(self.URL_INVENTORY)
        self.assertStatus(503)

    def _set_services(self, mock_instance, services):
        # pylint: disable=unused-argument
        def _list_services(service_type=None, service_id=None, node_name=None):
            service_descs = []
            for service in services:
                if node_name is None or service['nodename'] == node_name:
                    desc = ServiceDescription(nodename=service['nodename'],
                                              service_type=service['service_type'],
                                              service_instance=service['service_instance'])
                    service_descs.append(desc)
            return service_descs
        mock_instance.services.list.side_effect = _list_services

    @mock.patch('dashboard.controllers.orchestrator.OrchClient.instance')
    def test_service_list(self, instance):
        services = []
        for i in range(3):
            for service_type in ['mon', 'mgr', 'osd']:
                services.append(
                    {
                        'nodename': 'host-{}'.format(i),
                        'service_type': service_type,
                        'service_instance': 'x'
                    }
                )

        fake_client = mock.Mock()
        fake_client.available.return_value = True
        self._set_services(fake_client, services)
        instance.return_value = fake_client

        # list
        self._get(self.URL_SERVICE)
        self.assertStatus(200)
        self.assertJsonBody(services)

        # list with existent service
        self._get('{}?hostname=host-0'.format(self.URL_SERVICE))
        self.assertStatus(200)
        self.assertJsonBody([svc for svc in services if svc['nodename'] == 'host-0'])

        # list with non-existent service
        self._get('{}?hostname=host-10'.format(self.URL_SERVICE))
        self.assertStatus(200)
        self.assertJsonBody([])

        # list without orchestrator service
        fake_client.available.return_value = False
        self._get(self.URL_SERVICE)
        self.assertStatus(503)

    @mock.patch('dashboard.controllers.orchestrator.OrchClient.instance')
    def test_osd_create(self, instance):
        # with orchestrator service
        fake_client = mock.Mock()
        fake_client.available.return_value = False
        instance.return_value = fake_client
        self._post(self.URL_OSD, {})
        self.assertStatus(503)

        # without orchestrator service
        fake_client.available.return_value = True
        # incorrect drive group
        self._post(self.URL_OSD, {'drive_group': {}})
        self.assertStatus(400)

        # correct drive group
        dg = {
            'host_pattern': '*'
        }
        self._post(self.URL_OSD, {'drive_group': dg})
        self.assertStatus(201)


class TestOrchestrator(unittest.TestCase):
    def test_get_device_osd_map(self):
        mgr.get.side_effect = lambda key: {
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
            for node in devices.keys():
                devices[node] = sorted(devices[node])
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
