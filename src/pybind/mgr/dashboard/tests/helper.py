# -*- coding: utf-8 -*-
# pylint: disable=W0212
from __future__ import absolute_import

import json

import cherrypy
from cherrypy.test import helper

from ..controllers.auth import Auth
from ..tools import json_error_page, SessionExpireAtBrowserCloseTool


class ControllerTestCase(helper.CPWebCase):
    def __init__(self, *args, **kwargs):
        cherrypy.tools.authenticate = cherrypy.Tool('before_handler', Auth.check_auth)
        cherrypy.tools.session_expire_at_browser_close = SessionExpireAtBrowserCloseTool()
        cherrypy.config.update({'error_page.default': json_error_page})
        super(ControllerTestCase, self).__init__(*args, **kwargs)

    def _request(self, url, method, data=None):
        if not data:
            b = None
            h = None
        else:
            b = json.dumps(data)
            h = [('Content-Type', 'application/json'),
                 ('Content-Length', str(len(b)))]
        self.getPage(url, method=method, body=b, headers=h)

    def _get(self, url):
        self._request(url, 'GET')

    def _post(self, url, data=None):
        self._request(url, 'POST', data)

    def _delete(self, url, data=None):
        self._request(url, 'DELETE', data)

    def _put(self, url, data=None):
        self._request(url, 'PUT', data)

    def jsonBody(self):
        body_str = self.body.decode('utf-8') if isinstance(self.body, bytes) else self.body
        return json.loads(body_str)

    def assertJsonBody(self, data, msg=None):
        """Fail if value != self.body."""
        json_body = self.jsonBody()
        if data != json_body:
            if msg is None:
                msg = 'expected body:\n%r\n\nactual body:\n%r' % (
                    data, json_body)
            self._handlewebError(msg)
