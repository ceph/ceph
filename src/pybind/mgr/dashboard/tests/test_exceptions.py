# -*- coding: utf-8 -*-
from __future__ import absolute_import

import time

import rados

from . import ControllerTestCase
from ..services.ceph_service import SendCommandError
from ..controllers import RESTController, Controller, Task, Endpoint
from ..services.exception import handle_rados_error, handle_send_command_error, \
    serialize_dashboard_exception
from ..tools import ViewCache, TaskManager, NotificationQueue


# pylint: disable=W0613
@Controller('foo', secure=False)
class FooResource(RESTController):

    @Endpoint()
    @handle_rados_error('foo')
    def no_exception(self, param1, param2):
        return [param1, param2]

    @Endpoint()
    @handle_rados_error('foo')
    def error_foo_controller(self):
        raise rados.OSError('hi', errno=-42)

    @Endpoint()
    @handle_send_command_error('foo')
    def error_send_command(self):
        raise SendCommandError('hi', 'prefix', {}, -42)

    @Endpoint()
    def error_generic(self):
        raise rados.Error('hi')

    @Endpoint()
    def vc_no_data(self):
        @ViewCache(timeout=0)
        def _no_data():
            time.sleep(0.2)

        _no_data()
        assert False

    @handle_rados_error('foo')
    @Endpoint()
    def vc_exception(self):
        @ViewCache(timeout=10)
        def _raise():
            raise rados.OSError('hi', errno=-42)

        _raise()
        assert False

    @Endpoint()
    def internal_server_error(self):
        return 1/0

    @handle_send_command_error('foo')
    def list(self):
        raise SendCommandError('list', 'prefix', {}, -42)

    @Endpoint()
    @Task('task_exceptions/task_exception', {1: 2}, 1.0,
          exception_handler=serialize_dashboard_exception)
    @handle_rados_error('foo')
    def task_exception(self):
        raise rados.OSError('hi', errno=-42)

    @Endpoint()
    def wait_task_exception(self):
        ex, _ = TaskManager.list('task_exceptions/task_exception')
        return bool(len(ex))


# pylint: disable=C0102
class Root(object):
    foo = FooResource()


class RESTControllerTest(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        NotificationQueue.start_queue()
        TaskManager.init()
        cls.setup_controllers([FooResource])

    def test_no_exception(self):
        self._get('/foo/no_exception/a/b')
        self.assertStatus(200)
        self.assertJsonBody(
            ['a', 'b']
        )

    def test_error_foo_controller(self):
        self._get('/foo/error_foo_controller')
        self.assertStatus(400)
        self.assertJsonBody(
            {'detail': '[errno -42] hi', 'code': "42", 'component': 'foo'}
        )

    def test_error_send_command(self):
        self._get('/foo/error_send_command')
        self.assertStatus(400)
        self.assertJsonBody(
            {'detail': '[errno -42] hi', 'code': "42", 'component': 'foo'}
        )

    def test_error_send_command_list(self):
        self._get('/foo/')
        self.assertStatus(400)
        self.assertJsonBody(
            {'detail': '[errno -42] list', 'code': "42", 'component': 'foo'}
        )

    def test_error_foo_generic(self):
        self._get('/foo/error_generic')
        self.assertJsonBody({'detail': 'hi', 'code': 'Error', 'component': None})
        self.assertStatus(400)

    def test_viewcache_no_data(self):
        self._get('/foo/vc_no_data')
        self.assertStatus(200)
        self.assertJsonBody({'status': ViewCache.VALUE_NONE, 'value': None})

    def test_viewcache_exception(self):
        self._get('/foo/vc_exception')
        self.assertStatus(400)
        self.assertJsonBody(
            {'detail': '[errno -42] hi', 'code': "42", 'component': 'foo'}
        )

    def test_task_exception(self):
        self._get('/foo/task_exception')
        self.assertStatus(400)
        self.assertJsonBody(
            {'detail': '[errno -42] hi', 'code': "42", 'component': 'foo',
             'task': {'name': 'task_exceptions/task_exception', 'metadata': {'1': 2}}}
        )

        self._get('/foo/wait_task_exception')
        while self.jsonBody():
            time.sleep(0.5)
            self._get('/foo/wait_task_exception')

    def test_internal_server_error(self):
        self._get('/foo/internal_server_error')
        self.assertStatus(500)
        self.assertIn('unexpected condition', self.jsonBody()['detail'])

    def test_404(self):
        self._get('/foonot_found')
        self.assertStatus(404)
        self.assertIn('detail', self.jsonBody())
