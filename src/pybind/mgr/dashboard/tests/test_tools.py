# -*- coding: utf-8 -*-
from __future__ import absolute_import

import unittest

import cherrypy
from cherrypy.lib.sessions import RamSession
from mock import patch

from . import ControllerTestCase
from ..services.exception import handle_rados_error
from ..controllers import RESTController, ApiController, Controller, \
                          BaseController, Proxy
from ..tools import is_valid_ipv6_address, dict_contains_path, \
                    RequestLoggingTool


# pylint: disable=W0613
@Controller('/foo', secure=False)
class FooResource(RESTController):
    elems = []

    def list(self):
        return FooResource.elems

    def create(self, a):
        FooResource.elems.append({'a': a})
        return {'a': a}

    def get(self, key):
        return {'detail': (key, [])}

    def delete(self, key):
        del FooResource.elems[int(key)]

    def bulk_delete(self):
        FooResource.elems = []

    def set(self, key, newdata):
        FooResource.elems[int(key)] = {'newdata': newdata}
        return dict(key=key, newdata=newdata)


@Controller('/foo/:key/:method', secure=False)
class FooResourceDetail(RESTController):
    def list(self, key, method):
        return {'detail': (key, [method])}


@ApiController('/rgw/proxy', secure=False)
class GenerateControllerRoutesController(BaseController):
    @Proxy()
    def __call__(self, path, **params):
        pass


@ApiController('/fooargs', secure=False)
class FooArgs(RESTController):
    def set(self, code, name=None, opt1=None, opt2=None):
        return {'code': code, 'name': name, 'opt1': opt1, 'opt2': opt2}

    @handle_rados_error('foo')
    def create(self, my_arg_name):
        return my_arg_name

    def list(self):
        raise cherrypy.NotFound()


# pylint: disable=blacklisted-name
class Root(object):
    foo = FooResource()
    fooargs = FooArgs()


class RESTControllerTest(ControllerTestCase):

    @classmethod
    def setup_server(cls):
        cls.setup_controllers(
            [FooResource, FooResourceDetail, FooArgs, GenerateControllerRoutesController])

    def test_empty(self):
        self._delete("/foo")
        self.assertStatus(204)
        self._get("/foo")
        self.assertStatus('200 OK')
        self.assertHeader('Content-Type', 'application/json')
        self.assertBody('[]')

    def test_fill(self):
        sess_mock = RamSession()
        with patch('cherrypy.session', sess_mock, create=True):
            data = {'a': 'b'}
            for _ in range(5):
                self._post("/foo", data)
                self.assertJsonBody(data)
                self.assertStatus(201)
                self.assertHeader('Content-Type', 'application/json')

            self._get("/foo")
            self.assertStatus('200 OK')
            self.assertHeader('Content-Type', 'application/json')
            self.assertJsonBody([data] * 5)

            self._put('/foo/0', {'newdata': 'newdata'})
            self.assertStatus('200 OK')
            self.assertHeader('Content-Type', 'application/json')
            self.assertJsonBody({'newdata': 'newdata', 'key': '0'})

    def test_not_implemented(self):
        self._put("/foo")
        self.assertStatus(404)
        body = self.jsonBody()
        self.assertIsInstance(body, dict)
        assert body['detail'] == "The path '/foo' was not found."
        assert '404' in body['status']
        assert 'traceback' in body

    def test_args_from_json(self):
        self._put("/api/fooargs/hello", {'name': 'world'})
        self.assertJsonBody({'code': 'hello', 'name': 'world', 'opt1': None, 'opt2': None})

        self._put("/api/fooargs/hello", {'name': 'world', 'opt1': 'opt1'})
        self.assertJsonBody({'code': 'hello', 'name': 'world', 'opt1': 'opt1', 'opt2': None})

        self._put("/api/fooargs/hello", {'name': 'world', 'opt2': 'opt2'})
        self.assertJsonBody({'code': 'hello', 'name': 'world', 'opt1': None, 'opt2': 'opt2'})

    def test_detail_route(self):
        self._get('/foo/default')
        self.assertJsonBody({'detail': ['default', []]})

        self._get('/foo/default/default')
        self.assertJsonBody({'detail': ['default', ['default']]})

        self._get('/foo/1/detail')
        self.assertJsonBody({'detail': ['1', ['detail']]})

        self._post('/foo/1/detail', 'post-data')
        self.assertStatus(404)

    def test_generate_controller_routes(self):
        # We just need to add this controller in setup_server():
        # noinspection PyStatementEffect
        # pylint: disable=pointless-statement
        GenerateControllerRoutesController


class RequestLoggingToolTest(ControllerTestCase):

    def __init__(self, *args, **kwargs):
        cherrypy.tools.request_logging = RequestLoggingTool()
        cherrypy.config.update({'tools.request_logging.on': True})
        super(RequestLoggingToolTest, self).__init__(*args, **kwargs)

    @classmethod
    def setup_server(cls):
        cls.setup_controllers([FooResource])

    def test_is_logged(self):
        with patch('logging.Logger.debug') as mock_logger_debug:
            self._put('/foo/0', {'newdata': 'xyz'})
            self.assertStatus(200)
            call_args_list = mock_logger_debug.call_args_list
            _, host, _, method, user, path = call_args_list[0][0]
            self.assertEqual(host, '127.0.0.1')
            self.assertEqual(method, 'PUT')
            self.assertIsNone(user)
            self.assertEqual(path, '/foo/0')


class TestFunctions(unittest.TestCase):

    def test_is_valid_ipv6_address(self):
        self.assertTrue(is_valid_ipv6_address('::'))
        self.assertTrue(is_valid_ipv6_address('::1'))
        self.assertFalse(is_valid_ipv6_address('127.0.0.1'))
        self.assertFalse(is_valid_ipv6_address('localhost'))
        self.assertTrue(is_valid_ipv6_address('1200:0000:AB00:1234:0000:2552:7777:1313'))
        self.assertFalse(is_valid_ipv6_address('1200::AB00:1234::2552:7777:1313'))

    def test_dict_contains_path(self):
        x = {'a': {'b': {'c': 'foo'}}}
        self.assertTrue(dict_contains_path(x, ['a', 'b', 'c']))
        self.assertTrue(dict_contains_path(x, ['a', 'b', 'c']))
        self.assertTrue(dict_contains_path(x, ['a']))
        self.assertFalse(dict_contains_path(x, ['a', 'c']))
        self.assertTrue(dict_contains_path(x, []))
