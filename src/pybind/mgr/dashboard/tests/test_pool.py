# -*- coding: utf-8 -*-
# pylint: disable=protected-access
import time

try:
    import mock
except ImportError:
    import unittest.mock as mock

from .. import mgr
from ..controllers.pool import Pool
from ..controllers.task import Task
from ..tests import ControllerTestCase
from ..tools import NotificationQueue, TaskManager


class MockTask(object):
    percentages = []

    def set_progress(self, percentage):
        self.percentages.append(percentage)


class PoolControllerTest(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        cls.setup_controllers([Pool, Task])

    @mock.patch('dashboard.services.progress.get_progress_tasks')
    @mock.patch('dashboard.controllers.pool.Pool._get')
    @mock.patch('dashboard.services.ceph_service.CephService.send_command')
    def test_creation(self, send_command, _get, get_progress_tasks):
        _get.side_effect = [{
            'pool_name': 'test-pool',
            'pg_num': 64,
            'pg_num_target': 63,
            'pg_placement_num': 64,
            'pg_placement_num_target': 63
        }, {
            'pool_name': 'test-pool',
            'pg_num': 64,
            'pg_num_target': 64,
            'pg_placement_num': 64,
            'pg_placement_num_target': 64
        }]
        NotificationQueue.start_queue()
        TaskManager.init()

        def _send_cmd(*args, **kwargs):  # pylint: disable=unused-argument
            time.sleep(3)

        send_command.side_effect = _send_cmd
        get_progress_tasks.return_value = [], []

        self._task_post('/api/pool', {
            'pool': 'test-pool',
            'pool_type': 1,
            'pg_num': 64
        }, 10)
        self.assertStatus(201)
        self.assertEqual(_get.call_count, 2)
        NotificationQueue.stop()

    def _create_pool(self, send_command, payload):
        send_command.return_value = {}
        NotificationQueue.start_queue()
        TaskManager.init()
        try:
            with mock.patch('dashboard.controllers.pool.Pool._wait_for_pgs'), \
                    mock.patch('dashboard.controllers.pool.Pool._set_pool_values'):
                self._task_post('/api/pool', payload, 10)
        finally:
            NotificationQueue.stop()
        self.assertStatus(201)
        create_calls = [c for c in send_command.call_args_list
                        if c[0][1] == 'osd pool create']
        self.assertEqual(len(create_calls), 1)
        return create_calls[0][1]

    @mock.patch('dashboard.services.ceph_service.CephService.send_command')
    def test_creation_replicated_sends_no_ec_or_zone_params(self, send_command):
        kwargs = self._create_pool(send_command, {
            'pool': 'rep-pool',
            'pool_type': 'replicated',
            'pg_num': 32,
            'size': 3
        })
        self.assertEqual(kwargs['pool'], 'rep-pool')
        self.assertEqual(kwargs['pg_num'], 32)
        # None values are dropped by CephService.send_command, so the mon applies its defaults
        for name in ['rule', 'num_zones', 'replica', 'min_size', 'root', 'k', 'm',
                     'osd_failure_domain', 'zone_failure_domain', 'erasure_code_profile']:
            self.assertIsNone(kwargs.get(name), name)
        self.assertNotIn('class', kwargs)

    @mock.patch('dashboard.services.ceph_service.CephService.send_command')
    def test_creation_sends_rule_name_as_rule(self, send_command):
        kwargs = self._create_pool(send_command, {
            'pool': 'rep-pool',
            'pool_type': 'replicated',
            'pg_num': 32,
            'rule_name': 'my-rule'
        })
        self.assertEqual(kwargs['rule'], 'my-rule')

    @mock.patch('dashboard.services.ceph_service.CephService.send_command')
    def test_creation_with_zones_and_failure_domains(self, send_command):
        kwargs = self._create_pool(send_command, {
            'pool': 'zone-pool',
            'pool_type': 'replicated',
            'pg_num': 32,
            'num_zones': 2,
            'replica': '2',
            'min_size': '1',
            'root': 'default',
            'crush_device_class': 'ssd',
            'osd_failure_domain': 'host',
            'zone_failure_domain': 'datacenter'
        })
        self.assertEqual(kwargs['num_zones'], 2)
        self.assertEqual(kwargs['replica'], 2)
        self.assertEqual(kwargs['min_size'], 1)
        self.assertEqual(kwargs['root'], 'default')
        self.assertEqual(kwargs['class'], 'ssd')
        self.assertEqual(kwargs['osd_failure_domain'], 'host')
        self.assertEqual(kwargs['zone_failure_domain'], 'datacenter')
        self.assertIsNone(kwargs['k'])
        self.assertIsNone(kwargs['m'])

    @mock.patch('dashboard.services.ceph_service.CephService.send_command')
    def test_creation_with_num_zones_3_is_not_supported(self, send_command):
        send_command.return_value = {}
        NotificationQueue.start_queue()
        TaskManager.init()
        try:
            with mock.patch('dashboard.controllers.pool.Pool._wait_for_pgs'), \
                    mock.patch('dashboard.controllers.pool.Pool._set_pool_values'):
                self._task_post('/api/pool', {
                    'pool': 'zone-pool',
                    'pool_type': 'replicated',
                    'pg_num': 32,
                    'num_zones': 3,
                }, 10)
        finally:
            NotificationQueue.stop()
        self.assertStatus(400)
        self.assertIn('num_zones = 3', self.json_body().get('detail', ''))

    @mock.patch('dashboard.services.ceph_service.CephService.send_command')
    def test_creation_erasure_with_k_and_m(self, send_command):
        kwargs = self._create_pool(send_command, {
            'pool': 'ec-pool',
            'pool_type': 'erasure',
            'pg_num': 32,
            'k': '4',
            'm': '2',
            'osd_failure_domain': 'host'
        })
        # String values from the request are converted to integers
        self.assertEqual(kwargs['k'], 4)
        self.assertEqual(kwargs['m'], 2)
        self.assertEqual(kwargs['osd_failure_domain'], 'host')
        self.assertIsNone(kwargs['erasure_code_profile'])

    @mock.patch('dashboard.services.ceph_service.CephService.send_command')
    def test_creation_erasure_with_profile(self, send_command):
        kwargs = self._create_pool(send_command, {
            'pool': 'ec-pool',
            'pool_type': 'erasure',
            'pg_num': 32,
            'erasure_code_profile': 'ecp1'
        })
        self.assertEqual(kwargs['erasure_code_profile'], 'ecp1')
        self.assertIsNone(kwargs['k'])
        self.assertIsNone(kwargs['m'])

    @mock.patch('dashboard.controllers.pool.Pool._get')
    def test_wait_for_pgs_without_waiting(self, _get):
        _get.side_effect = [{
            'pool_name': 'test-pool',
            'pg_num': 32,
            'pg_num_target': 32,
            'pg_placement_num': 32,
            'pg_placement_num_target': 32
        }]
        Pool._wait_for_pgs('test-pool')
        self.assertEqual(_get.call_count, 1)

    @mock.patch('dashboard.controllers.pool.Pool._get')
    def test_wait_for_pgs_with_waiting(self, _get):
        task = MockTask()
        orig_method = TaskManager.current_task
        TaskManager.current_task = mock.MagicMock()
        TaskManager.current_task.return_value = task
        _get.side_effect = [{
            'pool_name': 'test-pool',
            'pg_num': 64,
            'pg_num_target': 32,
            'pg_placement_num': 64,
            'pg_placement_num_target': 64
        }, {
            'pool_name': 'test-pool',
            'pg_num': 63,
            'pg_num_target': 32,
            'pg_placement_num': 62,
            'pg_placement_num_target': 32
        }, {
            'pool_name': 'test-pool',
            'pg_num': 48,
            'pg_num_target': 32,
            'pg_placement_num': 48,
            'pg_placement_num_target': 32
        }, {
            'pool_name': 'test-pool',
            'pg_num': 48,
            'pg_num_target': 32,
            'pg_placement_num': 33,
            'pg_placement_num_target': 32
        }, {
            'pool_name': 'test-pool',
            'pg_num': 33,
            'pg_num_target': 32,
            'pg_placement_num': 32,
            'pg_placement_num_target': 32
        }, {
            'pool_name': 'test-pool',
            'pg_num': 32,
            'pg_num_target': 32,
            'pg_placement_num': 32,
            'pg_placement_num_target': 32
        }]
        Pool._wait_for_pgs('test-pool')
        self.assertEqual(_get.call_count, 6)
        self.assertEqual(task.percentages, [0, 5, 50, 73, 98])
        TaskManager.current_task = orig_method

    @mock.patch('dashboard.controllers.osd.CephService.get_pool_list_with_stats')
    @mock.patch('dashboard.controllers.osd.CephService.get_pool_list')
    def test_pool_list(self, get_pool_list, get_pool_list_with_stats):
        get_pool_list.return_value = [{
            'type': 3,
            'crush_rule': 1,
            'application_metadata': {
                'test_key': 'test_metadata'
            },
            'pool_name': 'test_name'
        }]
        mgr.get.side_effect = lambda key: {
            'osd_map_crush': {
                'rules': [{
                    'rule_id': 1,
                    'rule_name': 'test-rule'
                }]
            }
        }[key]
        Pool._pool_list()
        mgr.get.assert_called_with('osd_map_crush')
        self.assertEqual(get_pool_list.call_count, 1)
        # with stats
        get_pool_list_with_stats.return_value = get_pool_list.return_value
        Pool._pool_list(attrs='type', stats='True')
        self.assertEqual(get_pool_list_with_stats.call_count, 1)

    @mock.patch('dashboard.controllers.pool.Pool._get')
    @mock.patch('dashboard.services.ceph_service.CephService.send_command')
    def test_set_pool_name(self, send_command, _get):
        _get.return_value = {
            'options': {
                'compression_min_blob_size': '1'
            },
            'application_metadata': ['data1', 'data2']
        }

        def _send_cmd(*args, **kwargs):  # pylint: disable=unused-argument
            pass

        send_command.side_effect = _send_cmd
        NotificationQueue.start_queue()
        TaskManager.init()
        self._task_put('/api/pool/test-pool', {
            "flags": "ec_overwrites",
            "application_metadata": ['data3', 'data2'],
            "configuration": "test-conf",
            "compression_mode": 'unset',
            'compression_min_blob_size': '1',
            'compression_max_blob_size': '1',
            'compression_required_ratio': '1',
            'pool': 'test-pool',
            'pg_num': 64
        })
        NotificationQueue.stop()
        self.assertEqual(_get.call_count, 1)
        self.assertEqual(send_command.call_count, 10)
