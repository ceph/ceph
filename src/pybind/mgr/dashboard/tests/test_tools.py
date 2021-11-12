# -*- coding: utf-8 -*-

import unittest

import cherrypy
from cherrypy.lib.sessions import RamSession

try:
    from mock import patch
except ImportError:
    from unittest.mock import patch

from ..controllers import APIRouter, BaseController, Proxy, RESTController, Router
from ..controllers._version import APIVersion
from ..services.exception import handle_rados_error
from ..tests import ControllerTestCase
from ..tools import dict_contains_path, dict_get, json_str_to_object, \
    merge_list_of_dicts_by_key, partial_dict


# pylint: disable=W0613
@Router('/foo', secure=False)
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


@Router('/foo/:key/:method', secure=False)
class FooResourceDetail(RESTController):
    def list(self, key, method):
        return {'detail': (key, [method])}


@APIRouter('/rgw/proxy', secure=False)
class GenerateControllerRoutesController(BaseController):
    @Proxy()
    def __call__(self, path, **params):
        pass


@APIRouter('/fooargs', secure=False)
class FooArgs(RESTController):
    def set(self, code, name=None, opt1=None, opt2=None):
        return {'code': code, 'name': name, 'opt1': opt1, 'opt2': opt2}

    @handle_rados_error('foo')
    def create(self, my_arg_name):
        return my_arg_name

    def list(self):
        raise cherrypy.NotFound()


class Root(object):
    foo_resource = FooResource()
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
        self.assertHeader('Content-Type', APIVersion.DEFAULT.to_mime_type())
        self.assertBody('[]')

    def test_fill(self):
        sess_mock = RamSession()
        with patch('cherrypy.session', sess_mock, create=True):
            data = {'a': 'b'}
            for _ in range(5):
                self._post("/foo", data)
                self.assertJsonBody(data)
                self.assertStatus(201)
                self.assertHeader('Content-Type', APIVersion.DEFAULT.to_mime_type())

            self._get("/foo")
            self.assertStatus('200 OK')
            self.assertHeader('Content-Type', APIVersion.DEFAULT.to_mime_type())
            self.assertJsonBody([data] * 5)

            self._put('/foo/0', {'newdata': 'newdata'})
            self.assertStatus('200 OK')
            self.assertHeader('Content-Type', APIVersion.DEFAULT.to_mime_type())
            self.assertJsonBody({'newdata': 'newdata', 'key': '0'})

    def test_not_implemented(self):
        self._put("/foo")
        self.assertStatus(404)
        body = self.json_body()
        self.assertIsInstance(body, dict)
        assert body['detail'] == "The path '/foo' was not found."
        assert '404' in body['status']

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

    _request_logging = True

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

    def test_dict_contains_path(self):
        x = {'a': {'b': {'c': 'foo'}}}
        self.assertTrue(dict_contains_path(x, ['a', 'b', 'c']))
        self.assertTrue(dict_contains_path(x, ['a', 'b', 'c']))
        self.assertTrue(dict_contains_path(x, ['a']))
        self.assertFalse(dict_contains_path(x, ['a', 'c']))
        self.assertTrue(dict_contains_path(x, []))

    def test_json_str_to_object(self):
        expected_result = {'a': 1, 'b': 'bbb'}
        self.assertEqual(expected_result, json_str_to_object('{"a": 1, "b": "bbb"}'))
        self.assertEqual(expected_result, json_str_to_object(b'{"a": 1, "b": "bbb"}'))
        self.assertEqual('', json_str_to_object(''))
        self.assertRaises(TypeError, json_str_to_object, None)

    def test_partial_dict(self):
        expected_result = {'a': 1, 'c': 3}
        self.assertEqual(expected_result, partial_dict({'a': 1, 'b': 2, 'c': 3}, ['a', 'c']))
        self.assertEqual({}, partial_dict({'a': 1, 'b': 2, 'c': 3}, []))
        self.assertEqual({}, partial_dict({}, []))
        self.assertRaises(KeyError, partial_dict, {'a': 1, 'b': 2, 'c': 3}, ['d'])
        self.assertRaises(TypeError, partial_dict, None, ['a'])
        self.assertRaises(TypeError, partial_dict, {'a': 1, 'b': 2, 'c': 3}, None)

    def test_dict_get(self):
        self.assertFalse(dict_get({'foo': {'bar': False}}, 'foo.bar'))
        self.assertIsNone(dict_get({'foo': {'bar': False}}, 'foo.bar.baz'))
        self.assertEqual(dict_get({'foo': {'bar': False}, 'baz': 'xyz'}, 'baz'), 'xyz')

    def test_merge_list_of_dicts_by_key(self):
        expected_result = [{'a': 1, 'b': 2, 'c': 3}, {'a': 4, 'b': 5, 'c': 6}]
        self.assertEqual(expected_result, merge_list_of_dicts_by_key(
            [{'a': 1, 'b': 2}, {'a': 4, 'b': 5}], [{'a': 1, 'c': 3}, {'a': 4, 'c': 6}], 'a'))

        expected_result = [{'a': 1, 'b': 2}, {'a': 4, 'b': 5, 'c': 6}]
        self.assertEqual(expected_result, merge_list_of_dicts_by_key(
            [{'a': 1, 'b': 2}, {'a': 4, 'b': 5}], [{}, {'a': 4, 'c': 6}], 'a'))
        self.assertRaises(TypeError, merge_list_of_dicts_by_key, None)
