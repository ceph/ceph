# -*- coding: utf-8 -*-
from __future__ import absolute_import

import unittest

from cherrypy.lib.sessions import RamSession
from mock import patch

from .helper import ControllerTestCase
from ..controllers import RESTController, ApiController
from ..tools import is_valid_ipv6_address, dict_contains_path


@ApiController('foo')
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


@ApiController('foo/:key/:method')
class FooResourceDetail(RESTController):
    def list(self, key, method):
        return {'detail': (key, [method])}


@ApiController('fooargs')
class FooArgs(RESTController):
    def set(self, code, name=None, opt1=None, opt2=None):
        return {'code': code, 'name': name, 'opt1': opt1, 'opt2': opt2}


# pylint: disable=blacklisted-name
class Root(object):
    foo = FooResource()
    fooargs = FooArgs()


class RESTControllerTest(ControllerTestCase):

    @classmethod
    def setup_server(cls):
        cls.setup_controllers([FooResource, FooResourceDetail, FooArgs])

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
        self._put("/fooargs/hello", {'name': 'world'})
        self.assertJsonBody({'code': 'hello', 'name': 'world', 'opt1': None, 'opt2': None})

        self._put("/fooargs/hello", {'name': 'world', 'opt1': 'opt1'})
        self.assertJsonBody({'code': 'hello', 'name': 'world', 'opt1': 'opt1', 'opt2': None})

        self._put("/fooargs/hello", {'name': 'world', 'opt2': 'opt2'})
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

    def test_developer_page(self):
        self.getPage('/foo', headers=[('Accept', 'text/html')])
        self.assertIn('<p>GET', self.body.decode('utf-8'))
        self.assertIn('Content-Type: text/html', self.body.decode('utf-8'))
        self.assertIn('<form action="/api/foo/" method="post">', self.body.decode('utf-8'))
        self.assertIn('<input type="hidden" name="_method" value="post" />',
                      self.body.decode('utf-8'))

    def test_developer_exception_page(self):
        self.getPage('/foo',
                     headers=[('Accept', 'text/html'), ('Content-Length', '0')],
                     method='put')
        self.assertStatus(404)


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
