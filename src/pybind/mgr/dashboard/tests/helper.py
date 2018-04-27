# -*- coding: utf-8 -*-
# pylint: disable=too-many-arguments
from __future__ import absolute_import

import json
import threading
import time

import cherrypy
from cherrypy.test import helper

from .. import logger
from ..controllers.auth import Auth
from ..controllers import json_error_page, generate_controller_routes
from ..tools import SessionExpireAtBrowserCloseTool


class ControllerTestCase(helper.CPWebCase):
    @classmethod
    def setup_controllers(cls, ctrl_classes, base_url=''):
        if not isinstance(ctrl_classes, list):
            ctrl_classes = [ctrl_classes]
        mapper = cherrypy.dispatch.RoutesDispatcher()
        for ctrl in ctrl_classes:
            generate_controller_routes(ctrl, mapper, base_url)
        if base_url == '':
            base_url = '/'
        cherrypy.tree.mount(None, config={
            base_url: {'request.dispatch': mapper}})

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

    def _task_request(self, method, url, data, timeout):
        self._request(url, method, data)
        if self.status != '202 Accepted':
            logger.info("task finished immediately")
            return

        res = self.jsonBody()
        self.assertIsInstance(res, dict)
        self.assertIn('name', res)
        self.assertIn('metadata', res)

        task_name = res['name']
        task_metadata = res['metadata']

        # pylint: disable=protected-access
        class Waiter(threading.Thread):
            def __init__(self, task_name, task_metadata, tc):
                super(Waiter, self).__init__()
                self.task_name = task_name
                self.task_metadata = task_metadata
                self.ev = threading.Event()
                self.abort = False
                self.res_task = None
                self.tc = tc

            def run(self):
                running = True
                while running and not self.abort:
                    logger.info("task (%s, %s) is still executing", self.task_name,
                                self.task_metadata)
                    time.sleep(1)
                    self.tc._get('/task?name={}'.format(self.task_name))
                    res = self.tc.jsonBody()
                    for task in res['finished_tasks']:
                        if task['metadata'] == self.task_metadata:
                            # task finished
                            running = False
                            self.res_task = task
                            self.ev.set()

        thread = Waiter(task_name, task_metadata, self)
        thread.start()
        status = thread.ev.wait(timeout)
        if not status:
            # timeout expired
            thread.abort = True
            thread.join()
            raise Exception("Waiting for task ({}, {}) to finish timed out"
                            .format(task_name, task_metadata))
        logger.info("task (%s, %s) finished", task_name, task_metadata)
        if thread.res_task['success']:
            self.body = json.dumps(thread.res_task['ret_value'])
            if method == 'POST':
                self.status = '201 Created'
            elif method == 'PUT':
                self.status = '200 OK'
            elif method == 'DELETE':
                self.status = '204 No Content'
            return
        else:
            if 'status' in thread.res_task['exception']:
                self.status = thread.res_task['exception']['status']
            else:
                self.status = 500
            self.body = json.dumps(thread.res_task['exception'])
            return

    def _task_post(self, url, data=None, timeout=60):
        self._task_request('POST', url, data, timeout)

    def _task_delete(self, url, timeout=60):
        self._task_request('DELETE', url, None, timeout)

    def _task_put(self, url, data=None, timeout=60):
        self._task_request('PUT', url, data, timeout)

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
