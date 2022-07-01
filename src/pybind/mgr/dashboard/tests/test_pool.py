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
