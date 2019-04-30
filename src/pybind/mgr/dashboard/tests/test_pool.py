# -*- coding: utf-8 -*-
# pylint: disable=protected-access
import mock

from . import ControllerTestCase
from ..controllers.pool import Pool


class MockTask(object):
    percentages = []

    def set_progress(self, percentage):
        self.percentages.append(percentage)


class PoolControllerTest(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        Pool._cp_config['tools.authenticate.on'] = False
        cls.setup_controllers([Pool])

    def test_creation(self):
        self._post('/api/pool', {
            'pool': 'test-pool',
            'pool_type': 1,
            'pg_num': 64
        })
        self.assertStatus(202)
        self.assertJsonBody({'name': 'pool/create', 'metadata': {'pool_name': 'test-pool'}})

    @mock.patch('dashboard.controllers.pool.Pool._get')
    def test_wait_for_pgs_without_waiting(self, _get):
        _get.side_effect = [{
            'pool_name': 'test-pool',
            'pg_num': 32,
            'pg_num_target': 32,
            'pg_placement_num': 32,
            'pg_placement_num_target': 32
        }]
        pool = Pool()
        pool._wait_for_pgs('test-pool')
        self.assertEqual(_get.call_count, 1)

    @mock.patch('dashboard.controllers.pool.Pool._get')
    @mock.patch('dashboard.tools.TaskManager.current_task')
    def test_wait_for_pgs_with_waiting(self, taskMock, _get):
        task = MockTask()
        taskMock.return_value = task
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
        pool = Pool()
        pool._wait_for_pgs('test-pool')
        self.assertEqual(_get.call_count, 6)
        self.assertEqual(task.percentages, [0, 5, 50, 73, 98])
