# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json
import unittest
import threading
import time
from collections import defaultdict

from ..tools import NotificationQueue, TaskManager, TaskExecutor


class MyTask(object):
    class CallbackExecutor(TaskExecutor):
        def __init__(self, fail, progress):
            super(MyTask.CallbackExecutor, self).__init__()
            self.fail = fail
            self.progress = progress

        def init(self, task):
            super(MyTask.CallbackExecutor, self).init(task)
            args = [self.callback]
            args.extend(self.task.fn_args)
            self.task.fn_args = args

        def callback(self, result):
            self.task.set_progress(self.progress)
            if self.fail:
                self.finish(None, Exception("Task Unexpected Exception"))
            else:
                self.finish(result, None)

    # pylint: disable=too-many-arguments
    def __init__(self, op_seconds, wait=False, fail=False, progress=50,
                 is_async=False, handle_ex=False):
        self.op_seconds = op_seconds
        self.wait = wait
        self.fail = fail
        self.progress = progress
        self.is_async = is_async
        self.handle_ex = handle_ex
        self._event = threading.Event()

    def _handle_exception(self, ex):
        return {'status': 409, 'detail': str(ex)}

    def run(self, ns, timeout=None):
        args = ['dummy arg']
        kwargs = {'dummy': 'arg'}
        h_ex = self._handle_exception if self.handle_ex else None
        if not self.is_async:
            task = TaskManager.run(
                ns, self.metadata(), self.task_op, args, kwargs,
                exception_handler=h_ex)
        else:
            task = TaskManager.run(
                ns, self.metadata(), self.task_async_op, args, kwargs,
                executor=MyTask.CallbackExecutor(self.fail, self.progress),
                exception_handler=h_ex)
        return task.wait(timeout)

    def task_op(self, *args, **kwargs):
        time.sleep(self.op_seconds)
        TaskManager.current_task().set_progress(self.progress)
        if self.fail:
            raise Exception("Task Unexpected Exception")
        if self.wait:
            self._event.wait()
        return {'args': list(args), 'kwargs': kwargs}

    def task_async_op(self, callback, *args, **kwargs):
        if self.fail == "premature":
            raise Exception("Task Unexpected Exception")

        def _run_bg():
            time.sleep(self.op_seconds)
            if self.wait:
                self._event.wait()
            callback({'args': list(args), 'kwargs': kwargs})

        worker = threading.Thread(target=_run_bg)
        worker.start()

    def resume(self):
        self._event.set()

    def metadata(self):
        return {
            'op_seconds': self.op_seconds,
            'wait': self.wait,
            'fail': self.fail,
            'progress': self.progress,
            'is_async': self.is_async,
            'handle_ex': self.handle_ex
        }


class TaskTest(unittest.TestCase):

    TASK_FINISHED_MAP = defaultdict(threading.Event)

    @classmethod
    def _handle_task(cls, task):
        cls.TASK_FINISHED_MAP[task.name].set()

    @classmethod
    def wait_for_task(cls, name):
        cls.TASK_FINISHED_MAP[name].wait()

    @classmethod
    def setUpClass(cls):
        NotificationQueue.start_queue()
        TaskManager.init()
        NotificationQueue.register(cls._handle_task, 'cd_task_finished',
                                   priority=100)

    @classmethod
    def tearDownClass(cls):
        NotificationQueue.deregister(cls._handle_task, 'cd_task_finished')
        NotificationQueue.stop()

    def setUp(self):
        TaskManager.FINISHED_TASK_SIZE = 10
        TaskManager.FINISHED_TASK_TTL = 60.0

    def assertTaskResult(self, result):
        self.assertEqual(result,
                         {'args': ['dummy arg'], 'kwargs': {'dummy': 'arg'}})

    def test_fast_task(self):
        task1 = MyTask(1)
        state, result = task1.run('test1/task1')
        self.assertEqual(state, TaskManager.VALUE_DONE)
        self.assertTaskResult(result)
        self.wait_for_task('test1/task1')
        _, fn_t = TaskManager.list('test1/*')
        self.assertEqual(len(fn_t), 1)
        self.assertIsNone(fn_t[0].exception)
        self.assertTaskResult(fn_t[0].ret_value)
        self.assertEqual(fn_t[0].progress, 100)

    def test_slow_task(self):
        task1 = MyTask(1)
        state, result = task1.run('test2/task1', 0.5)
        self.assertEqual(state, TaskManager.VALUE_EXECUTING)
        self.assertIsNone(result)
        self.wait_for_task('test2/task1')
        _, fn_t = TaskManager.list('test2/*')
        self.assertEqual(len(fn_t), 1)
        self.assertIsNone(fn_t[0].exception)
        self.assertTaskResult(fn_t[0].ret_value)
        self.assertEqual(fn_t[0].progress, 100)

    def test_fast_task_with_failure(self):
        task1 = MyTask(1, fail=True, progress=40)

        with self.assertRaises(Exception) as ctx:
            task1.run('test3/task1')

        self.assertEqual(str(ctx.exception), "Task Unexpected Exception")
        self.wait_for_task('test3/task1')
        _, fn_t = TaskManager.list('test3/*')
        self.assertEqual(len(fn_t), 1)
        self.assertIsNone(fn_t[0].ret_value)
        self.assertEqual(str(fn_t[0].exception), "Task Unexpected Exception")
        self.assertEqual(fn_t[0].progress, 40)

    def test_slow_task_with_failure(self):
        task1 = MyTask(1, fail=True, progress=70)
        state, result = task1.run('test4/task1', 0.5)
        self.assertEqual(state, TaskManager.VALUE_EXECUTING)
        self.assertIsNone(result)
        self.wait_for_task('test4/task1')
        _, fn_t = TaskManager.list('test4/*')
        self.assertEqual(len(fn_t), 1)
        self.assertIsNone(fn_t[0].ret_value)
        self.assertEqual(str(fn_t[0].exception), "Task Unexpected Exception")
        self.assertEqual(fn_t[0].progress, 70)

    def test_executing_tasks_list(self):
        task1 = MyTask(0, wait=True, progress=30)
        task2 = MyTask(0, wait=True, progress=60)
        state, result = task1.run('test5/task1', 0.5)
        self.assertEqual(state, TaskManager.VALUE_EXECUTING)
        self.assertIsNone(result)
        ex_t, _ = TaskManager.list()
        self.assertEqual(len(ex_t), 1)
        self.assertEqual(ex_t[0].name, 'test5/task1')
        self.assertEqual(ex_t[0].progress, 30)
        state, result = task2.run('test5/task2', 0.5)
        self.assertEqual(state, TaskManager.VALUE_EXECUTING)
        self.assertIsNone(result)
        ex_t, _ = TaskManager.list('test5/*')
        self.assertEqual(len(ex_t), 2)
        for task in ex_t:
            if task.name == 'test5/task1':
                self.assertEqual(task.progress, 30)
            elif task.name == 'test5/task2':
                self.assertEqual(task.progress, 60)
        task2.resume()
        self.wait_for_task('test5/task2')
        ex_t, _ = TaskManager.list()
        self.assertEqual(len(ex_t), 1)
        self.assertEqual(ex_t[0].name, 'test5/task1')
        task1.resume()
        self.wait_for_task('test5/task1')
        ex_t, _ = TaskManager.list()
        self.assertEqual(len(ex_t), 0)

    def test_task_idempotent(self):
        task1 = MyTask(0, wait=True)
        task1_clone = MyTask(0, wait=True)
        state, result = task1.run('test6/task1', 0.5)
        self.assertEqual(state, TaskManager.VALUE_EXECUTING)
        self.assertIsNone(result)
        ex_t, _ = TaskManager.list()
        self.assertEqual(len(ex_t), 1)
        self.assertEqual(ex_t[0].name, 'test6/task1')
        state, result = task1_clone.run('test6/task1', 0.5)
        self.assertEqual(state, TaskManager.VALUE_EXECUTING)
        self.assertIsNone(result)
        ex_t, _ = TaskManager.list()
        self.assertEqual(len(ex_t), 1)
        self.assertEqual(ex_t[0].name, 'test6/task1')
        task1.resume()
        self.wait_for_task('test6/task1')
        ex_t, fn_t = TaskManager.list('test6/*')
        self.assertEqual(len(ex_t), 0)
        self.assertEqual(len(fn_t), 1)

    def test_finished_cleanup(self):
        TaskManager.FINISHED_TASK_SIZE = 2
        TaskManager.FINISHED_TASK_TTL = 0.5
        task1 = MyTask(0)
        task2 = MyTask(0)
        state, result = task1.run('test7/task1')
        self.assertEqual(state, TaskManager.VALUE_DONE)
        self.assertTaskResult(result)
        self.wait_for_task('test7/task1')
        state, result = task2.run('test7/task2')
        self.assertEqual(state, TaskManager.VALUE_DONE)
        self.assertTaskResult(result)
        self.wait_for_task('test7/task2')
        time.sleep(1)
        _, fn_t = TaskManager.list('test7/*')
        self.assertEqual(len(fn_t), 2)
        for idx, task in enumerate(fn_t):
            self.assertEqual(task.name,
                             "test7/task{}".format(len(fn_t)-idx))
        task3 = MyTask(0)
        state, result = task3.run('test7/task3')
        self.assertEqual(state, TaskManager.VALUE_DONE)
        self.assertTaskResult(result)
        self.wait_for_task('test7/task3')
        time.sleep(1)
        _, fn_t = TaskManager.list('test7/*')
        self.assertEqual(len(fn_t), 3)
        for idx, task in enumerate(fn_t):
            self.assertEqual(task.name,
                             "test7/task{}".format(len(fn_t)-idx))
        _, fn_t = TaskManager.list('test7/*')
        self.assertEqual(len(fn_t), 2)
        for idx, task in enumerate(fn_t):
            self.assertEqual(task.name,
                             "test7/task{}".format(len(fn_t)-idx+1))

    def test_task_serialization_format(self):
        task1 = MyTask(0, wait=True, progress=20)
        task2 = MyTask(1)
        task1.run('test8/task1', 0.5)
        task2.run('test8/task2', 0.5)
        self.wait_for_task('test8/task2')
        ex_t, fn_t = TaskManager.list_serializable('test8/*')
        self.assertEqual(len(ex_t), 1)
        self.assertEqual(len(fn_t), 1)

        try:
            json.dumps(ex_t)
        except ValueError as ex:
            self.fail("Failed to serialize executing tasks: {}".format(str(ex)))

        try:
            json.dumps(fn_t)
        except ValueError as ex:
            self.fail("Failed to serialize finished tasks: {}".format(str(ex)))

        # validate executing tasks attributes
        self.assertEqual(len(ex_t[0].keys()), 4)
        self.assertEqual(ex_t[0]['name'], 'test8/task1')
        self.assertEqual(ex_t[0]['metadata'], task1.metadata())
        self.assertIsNotNone(ex_t[0]['begin_time'])
        self.assertEqual(ex_t[0]['progress'], 20)
        # validate finished tasks attributes
        self.assertEqual(len(fn_t[0].keys()), 9)
        self.assertEqual(fn_t[0]['name'], 'test8/task2')
        self.assertEqual(fn_t[0]['metadata'], task2.metadata())
        self.assertIsNotNone(fn_t[0]['begin_time'])
        self.assertIsNotNone(fn_t[0]['end_time'])
        self.assertGreaterEqual(fn_t[0]['duration'], 1.0)
        self.assertEqual(fn_t[0]['progress'], 100)
        self.assertTrue(fn_t[0]['success'])
        self.assertTaskResult(fn_t[0]['ret_value'])
        self.assertIsNone(fn_t[0]['exception'])
        task1.resume()
        self.wait_for_task('test8/task1')

    def test_fast_async_task(self):
        task1 = MyTask(1, is_async=True)
        state, result = task1.run('test9/task1')
        self.assertEqual(state, TaskManager.VALUE_DONE)
        self.assertTaskResult(result)
        self.wait_for_task('test9/task1')
        _, fn_t = TaskManager.list('test9/*')
        self.assertEqual(len(fn_t), 1)
        self.assertIsNone(fn_t[0].exception)
        self.assertTaskResult(fn_t[0].ret_value)
        self.assertEqual(fn_t[0].progress, 100)

    def test_slow_async_task(self):
        task1 = MyTask(1, is_async=True)
        state, result = task1.run('test10/task1', 0.5)
        self.assertEqual(state, TaskManager.VALUE_EXECUTING)
        self.assertIsNone(result)
        self.wait_for_task('test10/task1')
        _, fn_t = TaskManager.list('test10/*')
        self.assertEqual(len(fn_t), 1)
        self.assertIsNone(fn_t[0].exception)
        self.assertTaskResult(fn_t[0].ret_value)
        self.assertEqual(fn_t[0].progress, 100)

    def test_fast_async_task_with_failure(self):
        task1 = MyTask(1, fail=True, progress=40, is_async=True)

        with self.assertRaises(Exception) as ctx:
            task1.run('test11/task1')

        self.assertEqual(str(ctx.exception), "Task Unexpected Exception")
        self.wait_for_task('test11/task1')
        _, fn_t = TaskManager.list('test11/*')
        self.assertEqual(len(fn_t), 1)
        self.assertIsNone(fn_t[0].ret_value)
        self.assertEqual(str(fn_t[0].exception), "Task Unexpected Exception")
        self.assertEqual(fn_t[0].progress, 40)

    def test_slow_async_task_with_failure(self):
        task1 = MyTask(1, fail=True, progress=70, is_async=True)
        state, result = task1.run('test12/task1', 0.5)
        self.assertEqual(state, TaskManager.VALUE_EXECUTING)
        self.assertIsNone(result)
        self.wait_for_task('test12/task1')
        _, fn_t = TaskManager.list('test12/*')
        self.assertEqual(len(fn_t), 1)
        self.assertIsNone(fn_t[0].ret_value)
        self.assertEqual(str(fn_t[0].exception), "Task Unexpected Exception")
        self.assertEqual(fn_t[0].progress, 70)

    def test_fast_async_task_with_premature_failure(self):
        task1 = MyTask(1, fail="premature", progress=40, is_async=True)

        with self.assertRaises(Exception) as ctx:
            task1.run('test13/task1')

        self.assertEqual(str(ctx.exception), "Task Unexpected Exception")
        self.wait_for_task('test13/task1')
        _, fn_t = TaskManager.list('test13/*')
        self.assertEqual(len(fn_t), 1)
        self.assertIsNone(fn_t[0].ret_value)
        self.assertEqual(str(fn_t[0].exception), "Task Unexpected Exception")

    def test_task_serialization_format_on_failure(self):
        task1 = MyTask(1, fail=True)
        task1.run('test14/task1', 0.5)
        self.wait_for_task('test14/task1')
        ex_t, fn_t = TaskManager.list_serializable('test14/*')
        self.assertEqual(len(ex_t), 0)
        self.assertEqual(len(fn_t), 1)
        # validate finished tasks attributes

        try:
            json.dumps(fn_t)
        except TypeError as ex:
            self.fail("Failed to serialize finished tasks: {}".format(str(ex)))

        self.assertEqual(len(fn_t[0].keys()), 9)
        self.assertEqual(fn_t[0]['name'], 'test14/task1')
        self.assertEqual(fn_t[0]['metadata'], task1.metadata())
        self.assertIsNotNone(fn_t[0]['begin_time'])
        self.assertIsNotNone(fn_t[0]['end_time'])
        self.assertGreaterEqual(fn_t[0]['duration'], 1.0)
        self.assertEqual(fn_t[0]['progress'], 50)
        self.assertFalse(fn_t[0]['success'])
        self.assertIsNotNone(fn_t[0]['exception'])
        self.assertEqual(fn_t[0]['exception'],
                         {"detail": "Task Unexpected Exception"})

    def test_task_serialization_format_on_failure_with_handler(self):
        task1 = MyTask(1, fail=True, handle_ex=True)
        task1.run('test15/task1', 0.5)
        self.wait_for_task('test15/task1')
        ex_t, fn_t = TaskManager.list_serializable('test15/*')
        self.assertEqual(len(ex_t), 0)
        self.assertEqual(len(fn_t), 1)
        # validate finished tasks attributes

        try:
            json.dumps(fn_t)
        except TypeError as ex:
            self.fail("Failed to serialize finished tasks: {}".format(str(ex)))

        self.assertEqual(len(fn_t[0].keys()), 9)
        self.assertEqual(fn_t[0]['name'], 'test15/task1')
        self.assertEqual(fn_t[0]['metadata'], task1.metadata())
        self.assertIsNotNone(fn_t[0]['begin_time'])
        self.assertIsNotNone(fn_t[0]['end_time'])
        self.assertGreaterEqual(fn_t[0]['duration'], 1.0)
        self.assertEqual(fn_t[0]['progress'], 50)
        self.assertFalse(fn_t[0]['success'])
        self.assertIsNotNone(fn_t[0]['exception'])
        self.assertEqual(fn_t[0]['exception'],
                         {"status": 409,
                          "detail": "Task Unexpected Exception"})
