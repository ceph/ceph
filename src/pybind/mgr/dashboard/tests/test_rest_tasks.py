# -*- coding: utf-8 -*-
# pylint: disable=blacklisted-name

import time

from .helper import ControllerTestCase
from ..controllers import ApiController, RESTController, Task
from ..controllers.task import Task as TaskController
from ..tools import NotificationQueue, TaskManager


@ApiController('test/task')
class TaskTest(RESTController):
    sleep_time = 0.0

    @Task('task/create', {'param': '{param}'}, wait_for=1.0)
    @RESTController.args_from_json
    def create(self, param):
        time.sleep(TaskTest.sleep_time)
        return {'my_param': param}

    @Task('task/set', {'param': '{2}'}, wait_for=1.0)
    @RESTController.args_from_json
    def set(self, key, param=None):
        time.sleep(TaskTest.sleep_time)
        return {'key': key, 'my_param': param}

    @Task('task/delete', ['{key}'], wait_for=1.0)
    @RESTController.args_from_json
    def delete(self, key):
        # pylint: disable=unused-argument
        time.sleep(TaskTest.sleep_time)


class TaskControllerTest(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        # pylint: disable=protected-access
        NotificationQueue.start_queue()
        TaskManager.init()
        TaskController._cp_config['tools.authenticate.on'] = False
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
