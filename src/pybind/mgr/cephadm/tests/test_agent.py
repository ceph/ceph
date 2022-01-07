import json
from unittest.mock import Mock

import cherrypy
import pytest
from cherrypy.test.helper import CPWebCase
from cephadm.agent import Root
from cephadm.tests.fixtures import with_cephadm_module


class TestAgent(CPWebCase):

    @pytest.fixture(autouse=True)
    def _cephadm_module(self):
        with with_cephadm_module({}) as m:
            self.cephadm_module = m
            cherrypy.tree.apps['']._mock_wraps = self.cephadm_module
            yield m

    def _request(self, url, method='GET', data=None, headers=None):
        b = None
        h = None
        if data:
            b = json.dumps(data)
            h = [('Content-Type', 'application/json'),
                 ('Content-Length', str(len(b)))]
        if headers:
            h = headers
        self.getPage(url, method=method, body=b, headers=h)

    def assertJsonBody(self, data, msg=None):
        """Fail if value != self.body."""
        json_body = self.json_body()
        if data != json_body:
            if msg is None:
                msg = 'expected body:\n%r\n\nactual body:\n%r' % (
                    data, json_body)
            self._handlewebError(msg)

    def json_body(self):
        body_str = self.body.decode('utf-8') if isinstance(self.body, bytes) else self.body
        return json.loads(body_str)

    @classmethod
    def setup_server(cls):
        root_conf = {'/': {'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
                           'tools.response_headers.on': True}}
        cherrypy.tree.mount(Root(Mock(wraps=None)), '/', root_conf)

    def test_slash(self):
        self._request('/')
        assert b'Cephadm HTTP Endpoint is up and running' in self.body

    def test_post_empty(self):
        self._request('/data/', 'POST', data='{}')
        self.assertJsonBody({"result": "Bad metadata: 'str' object has no attribute 'keys'"})
