# -*- coding: utf-8 -*-
from __future__ import absolute_import
import json

from .helper import DashboardTestCase, JList, JObj
from .test_orchestrator import test_data


class HostControllerTest(DashboardTestCase):

    AUTH_ROLES = ['read-only']

    URL_HOST = '/api/host'

    @classmethod
    def setUpClass(cls):
        super(HostControllerTest, cls).setUpClass()
        cls._load_module("test_orchestrator")

        cmd = ['orch', 'set', 'backend', 'test_orchestrator']
        cls.mgr_cluster.mon_manager.raw_cluster_cmd(*cmd)

        cmd = ['test_orchestrator', 'load_data', '-i', '-']
        cls.mgr_cluster.mon_manager.raw_cluster_cmd_result(*cmd, stdin=json.dumps(test_data))

    @classmethod
    def tearDownClass(cls):
        cmd = ['test_orchestrator', 'load_data', '-i', '-']
        cls.mgr_cluster.mon_manager.raw_cluster_cmd_result(*cmd, stdin='{}')

    @DashboardTestCase.RunAs('test', 'test', ['block-manager'])
    def test_access_permissions(self):
        self._get(self.URL_HOST)
        self.assertStatus(403)

    def test_host_list(self):
        data = self._get(self.URL_HOST)
        self.assertStatus(200)

        orch_hostnames = {inventory_node['name'] for inventory_node in test_data['inventory']}

        for server in data:
            self.assertIn('services', server)
            self.assertIn('hostname', server)
            self.assertIn('ceph_version', server)
            self.assertIsNotNone(server['hostname'])
            self.assertIsNotNone(server['ceph_version'])
            for service in server['services']:
                self.assertIn('type', service)
                self.assertIn('id', service)
                self.assertIsNotNone(service['type'])
                self.assertIsNotNone(service['id'])

            self.assertIn('sources', server)
            in_ceph, in_orchestrator = server['sources']['ceph'], server['sources']['orchestrator']
            if in_ceph:
                self.assertGreaterEqual(len(server['services']), 1)
                if not in_orchestrator:
                    self.assertNotIn(server['hostname'], orch_hostnames)
            if in_orchestrator:
                self.assertEqual(len(server['services']), 0)
                self.assertIn(server['hostname'], orch_hostnames)

    def test_host_list_with_sources(self):
        data = self._get('{}?sources=orchestrator'.format(self.URL_HOST))
        self.assertStatus(200)
        test_hostnames = {inventory_node['name'] for inventory_node in test_data['inventory']}
        resp_hostnames = {host['hostname'] for host in data}
        self.assertEqual(test_hostnames, resp_hostnames)

        data = self._get('{}?sources=ceph'.format(self.URL_HOST))
        self.assertStatus(200)
        test_hostnames = {inventory_node['name'] for inventory_node in test_data['inventory']}
        resp_hostnames = {host['hostname'] for host in data}
        self.assertEqual(len(test_hostnames.intersection(resp_hostnames)), 0)

    def test_host_devices(self):
        hosts = self._get('{}'.format(self.URL_HOST))
        hosts = [host['hostname'] for host in hosts if host['hostname'] != '']
        assert hosts[0]
        data = self._get('{}/devices'.format('{}/{}'.format(self.URL_HOST, hosts[0])))
        self.assertStatus(200)
        self.assertSchema(data, JList(JObj({
            'daemons': JList(str),
            'devid': str,
            'location': JList(JObj({
                'host': str,
                'dev': str,
                'path': str
            }))
        })))

    def test_host_daemons(self):
        hosts = self._get('{}'.format(self.URL_HOST))
        hosts = [host['hostname'] for host in hosts if host['hostname'] != '']
        assert hosts[0]
        data = self._get('{}/daemons'.format('{}/{}'.format(self.URL_HOST, hosts[0])))
        self.assertStatus(200)
        self.assertSchema(data, JList(JObj({
            'hostname': str,
            'daemon_id': str,
            'daemon_type': str
        })))

    def test_host_smart(self):
        hosts = self._get('{}'.format(self.URL_HOST))
        hosts = [host['hostname'] for host in hosts if host['hostname'] != '']
        assert hosts[0]
        data = self._get('{}/smart'.format('{}/{}'.format(self.URL_HOST, hosts[0])))
        self.assertStatus(200)


class HostControllerNoOrchestratorTest(DashboardTestCase):
    def test_host_create(self):
        self._post('/api/host?hostname=foo')
        self.assertStatus(503)
        self.assertError(code='orchestrator_status_unavailable',
                         component='orchestrator')

    def test_host_delete(self):
        self._delete('/api/host/bar')
        self.assertStatus(503)
        self.assertError(code='orchestrator_status_unavailable',
                         component='orchestrator')
