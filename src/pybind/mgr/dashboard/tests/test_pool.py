# -*- coding: utf-8 -*-
# pylint: disable=protected-access
import time
try:
    import mock
except ImportError:
    import unittest.mock as mock

from . import ControllerTestCase
from ..controllers.pool import Pool
from ..controllers.task import Task
from ..tools import NotificationQueue, TaskManager


class MockTask(object):
    percentages = []

    def set_progress(self, percentage):
        self.percentages.append(percentage)


class PoolControllerTest(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        Task._cp_config['tools.authenticate.on'] = False
        Pool._cp_config['tools.authenticate.on'] = False
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
