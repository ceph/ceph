# -*- coding: utf-8 -*-

import time

try:
    import mock
except ImportError:
    import unittest.mock as mock

from ..controllers import RESTController, Router, Task
from ..controllers.task import Task as TaskController
from ..services import progress
from ..tests import ControllerTestCase
from ..tools import NotificationQueue, TaskManager


@Router('/test/task', secure=False)
class TaskTest(RESTController):
    sleep_time = 0.0

    @Task('task/create', {'param': '{param}'}, wait_for=1.0)
    def create(self, param):
        time.sleep(TaskTest.sleep_time)
        return {'my_param': param}

    @Task('task/set', {'param': '{2}'}, wait_for=1.0)
    def set(self, key, param=None):
        time.sleep(TaskTest.sleep_time)
        return {'key': key, 'my_param': param}

    @Task('task/delete', ['{key}'], wait_for=1.0)
    def delete(self, key):
        # pylint: disable=unused-argument
        time.sleep(TaskTest.sleep_time)

    @Task('task/foo', ['{param}'])
    @RESTController.Collection('POST', path='/foo')
    def foo_post(self, param):
        return {'my_param': param}

    @Task('task/bar', ['{key}', '{param}'])
    @RESTController.Resource('PUT', path='/bar')
    def bar_put(self, key, param=None):
        return {'my_param': param, 'key': key}

    @Task('task/query', ['{param}'])
    @RESTController.Collection('POST', query_params=['param'])
    def query(self, param=None):
        return {'my_param': param}


class TaskControllerTest(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        # pylint: disable=protected-access
        progress.get_progress_tasks = mock.MagicMock()
        progress.get_progress_tasks.return_value = ([], [])

        NotificationQueue.start_queue()
        TaskManager.init()
        cls.setup_controllers([TaskTest, TaskController])

    @classmethod
    def tearDownClass(cls):
        NotificationQueue.stop()

    def setUp(self):
        TaskTest.sleep_time = 0.0

    def test_create_task(self):
        self._task_post('/test/task', {'param': 'hello'})
        self.assertJsonBody({'my_param': 'hello'})

    def test_long_set_task(self):
        TaskTest.sleep_time = 2.0
        self._task_put('/test/task/2', {'param': 'hello'})
        self.assertJsonBody({'key': '2', 'my_param': 'hello'})

    def test_delete_task(self):
        self._task_delete('/test/task/hello')

    def test_foo_task(self):
        self._task_post('/test/task/foo', {'param': 'hello'})
        self.assertJsonBody({'my_param': 'hello'})

    def test_bar_task(self):
        self._task_put('/test/task/3/bar', {'param': 'hello'})
        self.assertJsonBody({'my_param': 'hello', 'key': '3'})

    def test_query_param(self):
        self._task_post('/test/task/query')
        self.assertJsonBody({'my_param': None})
