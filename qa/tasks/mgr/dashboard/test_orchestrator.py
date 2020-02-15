# -*- coding: utf-8 -*-
from __future__ import absolute_import
import json

from .helper import DashboardTestCase


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
    URL_SERVICE = '/api/orchestrator/service'
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
        self._get(self.URL_SERVICE)
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

    def test_daemon_list(self):
        # get all daemons
        data = self._get(self.URL_SERVICE)
        self.assertStatus(200)

        sorting_key = lambda svc: '%(nodename)s.%(daemon_type)s.%(daemon_id)s' % svc
        test_daemons = sorted(self.test_data_daemons, key=sorting_key)
        resp_daemons = sorted(data, key=sorting_key)
        self.assertEqual(len(test_daemons), len(resp_daemons))
        for test, resp in zip(test_daemons, resp_daemons):
            self._validate_daemon(test, resp)

        # get service by hostname
        nodename = self.test_data_daemons[-1]['nodename']
        test_daemons = sorted(filter(lambda svc: svc['nodename'] == nodename, test_daemons),
                          key=sorting_key)
        data = self._get('{}?hostname={}'.format(self.URL_SERVICE, nodename))
        resp_daemons = sorted(data, key=sorting_key)
        for test, resp in zip(test_daemons, resp_daemons):
            self._validate_daemon(test, resp)

    def test_create_osds(self):
        data = {
            'drive_group': {
                'host_pattern': '*',
                'data_devices': {
                    'vendor': 'abc',
                    'model': 'cba',
                    'rotational': True,
                    'size': '4 TB'
                },
                'wal_devices': {
                    'vendor': 'def',
                    'model': 'fed',
                    'rotational': False,
                    'size': '1 TB'
                },
                'db_devices': {
                    'vendor': 'ghi',
                    'model': 'ihg',
                    'rotational': False,
                    'size': '512 GB'
                },
                'wal_slots': 5,
                'db_slots': 5,
                'encrypted': True
            }
        }
        self._post(self.URL_OSD, data)
        self.assertStatus(201)
