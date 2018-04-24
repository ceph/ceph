# -*- coding: utf-8 -*-
from __future__ import absolute_import

import cherrypy

import rados
from ..services.ceph_service import SendCommandError
from ..controllers import RESTController, ApiController
from .helper import ControllerTestCase
from ..services.exception import handle_rados_error, handle_send_command_error, \
    serialize_dashboard_exception
from ..tools import ViewCache, TaskManager


# pylint: disable=W0613
@ApiController('foo')
class FooResource(RESTController):
    @cherrypy.expose
    @cherrypy.tools.json_out()
    @handle_rados_error('foo')
    def no_exception(self, param1, param2):
        return [param1, param2]

    @cherrypy.expose
    @cherrypy.tools.json_out()
    @handle_rados_error('foo')
    def error_foo_controller(self):
        raise rados.OSError('hi', errno=-42)

    @cherrypy.expose
    @cherrypy.tools.json_out()
    @handle_send_command_error('foo')
    def error_send_command(self):
        raise SendCommandError('hi', 'prefix', {}, -42)

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def error_generic(self):
        raise rados.Error('hi')

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def vc_no_data(self):
        @ViewCache(timeout=0)
        def _no_data():
            import time
            time.sleep(0.2)

        _no_data()
        assert False

    @handle_rados_error('foo')
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def vc_exception(self):
        @ViewCache(timeout=10)
        def _raise():
            raise rados.OSError('hi', errno=-42)

        _raise()
        assert False

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def internal_server_error(self):
        return 1/0

    @handle_send_command_error('foo')
    def list(self):
        raise SendCommandError('list', 'prefix', {}, -42)

    @cherrypy.expose
    @cherrypy.tools.json_out()
    @Task('task_exceptions/task_exception', {}, 1.0,
          exception_handler=serialize_dashboard_exception)
    @handle_rados_error('foo')
    def task_exception(self):
        raise rados.OSError('hi', errno=-42)

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def wait_task_exception(self):
        ex, _ = TaskManager.list('task_exceptions/task_exception')
        return bool(len(ex))


# pylint: disable=C0102
class Root(object):
    foo = FooResource()


class RESTControllerTest(ControllerTestCase):

    @classmethod
    def setup_server(cls):
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
            {'detail': 'hi', 'code': "42", 'component': 'foo'}
        )

    def test_error_send_command_list(self):
        self._get('/foo/')
        self.assertStatus(400)
        self.assertJsonBody(
            {'detail': 'list', 'code': "42", 'component': 'foo'}
        )

    def test_error_send_command_bowsable_api(self):
        self.getPage('/foo/error_send_command', headers=[('Accept', 'text/html')])
        for err in ["'detail': 'hi'", "'component': 'foo'"]:
            self.assertIn(err.replace("'", "\'").encode('utf-8'), self.body)

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

    def test_internal_server_error(self):
        self._get('/foo/internal_server_error')
        self.assertStatus(500)
        self.assertIn('unexpected condition', self.jsonBody()['detail'])

    def test_404(self):
        self._get('/foonot_found')
        self.assertStatus(404)
        self.assertIn('detail', self.jsonBody())
