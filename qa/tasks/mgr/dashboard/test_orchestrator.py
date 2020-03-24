# -*- coding: utf-8 -*-
from __future__ import absolute_import
import json

from tasks.mgr.dashboard.helper import DashboardTestCase


test_data = {
    'inventory': [
        {
            'name': 'test-host0',
            'addr': '1.2.3.4',
            'devices': [
                {
                    'path': '/dev/sda',
                }
            ]
        },
        {
            'name': 'test-host1',
            'addr': '1.2.3.5',
            'devices': [
                {
                    'path': '/dev/sdb',
                }
            ]
        }
    ],
    'daemons': [
        {
            'nodename': 'test-host0',
            'daemon_type': 'mon',
            'daemon_id': 'a'
        },
        {
            'nodename': 'test-host0',
            'daemon_type': 'mgr',
            'daemon_id': 'x'
        },
        {
            'nodename': 'test-host0',
            'daemon_type': 'osd',
            'daemon_id': '0'
        },
        {
            'nodename': 'test-host1',
            'daemon_type': 'osd',
            'daemon_id': '1'
        }
    ]
}


class OrchestratorControllerTest(DashboardTestCase):

    AUTH_ROLES = ['cluster-manager']

    URL_STATUS = '/api/orchestrator/status'
    URL_INVENTORY = '/api/orchestrator/inventory'
    URL_OSD = '/api/orchestrator/osd'


    @property
    def test_data_inventory(self):
        return test_data['inventory']

    @property
    def test_data_daemons(self):
        return test_data['daemons']

    @classmethod
    def setUpClass(cls):
        super(OrchestratorControllerTest, cls).setUpClass()
        cls._load_module('test_orchestrator')
        cmd = ['orch', 'set', 'backend', 'test_orchestrator']
        cls.mgr_cluster.mon_manager.raw_cluster_cmd(*cmd)

        cmd = ['test_orchestrator', 'load_data', '-i', '-']
        cls.mgr_cluster.mon_manager.raw_cluster_cmd_result(*cmd, stdin=json.dumps(test_data))

    @classmethod
    def tearDownClass(cls):
        cmd = ['test_orchestrator', 'load_data', '-i', '-']
        cls.mgr_cluster.mon_manager.raw_cluster_cmd_result(*cmd, stdin='{}')

    def _validate_inventory(self, data, resp_data):
        self.assertEqual(data['name'], resp_data['name'])
        self.assertEqual(len(data['devices']), len(resp_data['devices']))

        if not data['devices']:
            return
        test_devices = sorted(data['devices'], key=lambda d: d['path'])
        resp_devices = sorted(resp_data['devices'], key=lambda d: d['path'])

        for test, resp in zip(test_devices, resp_devices):
            self._validate_device(test, resp)

    def _validate_device(self, data, resp_data):
        for key, value in data.items():
            self.assertEqual(value, resp_data[key])

    def _validate_daemon(self, data, resp_data):
        for key, value in data.items():
            self.assertEqual(value, resp_data[key])

    @DashboardTestCase.RunAs('test', 'test', ['block-manager'])
    def test_access_permissions(self):
        self._get(self.URL_STATUS)
        self.assertStatus(200)
        self._get(self.URL_INVENTORY)
        self.assertStatus(403)

    def test_status_get(self):
        data = self._get(self.URL_STATUS)
        self.assertTrue(data['available'])

    def test_inventory_list(self):
        # get all inventory
        data = self._get(self.URL_INVENTORY)
        self.assertStatus(200)

        sorting_key = lambda node: node['name']
        test_inventory = sorted(self.test_data_inventory, key=sorting_key)
        resp_inventory = sorted(data, key=sorting_key)
        self.assertEqual(len(test_inventory), len(resp_inventory))
        for test, resp in zip(test_inventory, resp_inventory):
            self._validate_inventory(test, resp)

        # get inventory by hostname
        node = self.test_data_inventory[-1]
        data = self._get('{}?hostname={}'.format(self.URL_INVENTORY, node['name']))
        self.assertStatus(200)
        self.assertEqual(len(data), 1)
        self._validate_inventory(node, data[0])
