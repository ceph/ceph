# -*- coding: utf-8 -*-
from __future__ import absolute_import

import cherrypy
from cherrypy.lib.sessions import RamSession
from mock import patch

from .helper import ControllerTestCase
from ..tools import RESTController, ApiController


# pylint: disable=W0613
@ApiController('foo')
class FooResource(RESTController):
    elems = []

    def list(self, *vpath, **params):
        return FooResource.elems

    def create(self, data, *args, **kwargs):
        FooResource.elems.append(data)
        return data

    def get(self, key, *args, **kwargs):
        return {'detail': (key, args)}

    def delete(self, key):
        del FooResource.elems[int(key)]

    def bulk_delete(self):
        FooResource.elems = []

    def set(self, data, key):
        FooResource.elems[int(key)] = data
        return dict(key=key, **data)


@ApiController('fooargs')
class FooArgs(RESTController):
    @RESTController.args_from_json
    def set(self, code, name, opt1=None, opt2=None):
        return {'code': code, 'name': name, 'opt1': opt1, 'opt2': opt2}


# pylint: disable=C0102
class Root(object):
    foo = FooResource()
    fooargs = FooArgs()


class RESTControllerTest(ControllerTestCase):

    @classmethod
    def setup_server(cls):
        cherrypy.tree.mount(Root())

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
        self.assertStatus(405)
        body = self.jsonBody()
        self.assertIsInstance(body, dict)
        assert body['detail'] == 'Method not implemented.'
        assert '405' in body['status']
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
        self.assertStatus(405)

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
        assert '<p>PUT' in self.body.decode('utf-8')
        assert 'Exception' in self.body.decode('utf-8')
        assert 'Content-Type: text/html' in self.body.decode('utf-8')
        assert '<form action="/api/foo/" method="post">' in self.body.decode('utf-8')
        assert '<input type="hidden" name="_method" value="post" />' in self.body.decode('utf-8')
