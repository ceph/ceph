# -*- coding: utf-8 -*-
# pylint: disable=too-many-arguments
from __future__ import absolute_import

import json
import logging
import threading
import time

import cherrypy
from cherrypy._cptools import HandlerWrapperTool
from cherrypy.test import helper
from pyfakefs import fake_filesystem

from mgr_module import CLICommand

from .. import mgr
from ..controllers import json_error_page, generate_controller_routes
from ..services.auth import AuthManagerTool
from ..services.exception import dashboard_exception_handler

from ..plugins import PLUGIN_MANAGER
from ..plugins import feature_toggles, debug  # noqa # pylint: disable=unused-import


PLUGIN_MANAGER.hook.init()
PLUGIN_MANAGER.hook.register_commands()


logger = logging.getLogger('tests')


class CmdException(Exception):
    def __init__(self, retcode, message):
        super(CmdException, self).__init__(message)
        self.retcode = retcode


def exec_dashboard_cmd(command_handler, cmd, **kwargs):
    cmd_dict = {'prefix': 'dashboard {}'.format(cmd)}
    cmd_dict.update(kwargs)
    if cmd_dict['prefix'] not in CLICommand.COMMANDS:
        ret, out, err = command_handler(cmd_dict)
        if ret < 0:
            raise CmdException(ret, err)
        try:
            return json.loads(out)
        except ValueError:
            return out

    ret, out, err = CLICommand.COMMANDS[cmd_dict['prefix']].call(mgr, cmd_dict,
                                                                 None)
    if ret < 0:
        raise CmdException(ret, err)
    try:
        return json.loads(out)
    except ValueError:
        return out


class KVStoreMockMixin(object):
    CONFIG_KEY_DICT = {}

    @classmethod
    def mock_set_module_option(cls, attr, val):
        cls.CONFIG_KEY_DICT[attr] = val

    @classmethod
    def mock_get_module_option(cls, attr, default=None):
        return cls.CONFIG_KEY_DICT.get(attr, default)

    @classmethod
    def mock_kv_store(cls):
        cls.CONFIG_KEY_DICT.clear()
        mgr.set_module_option.side_effect = cls.mock_set_module_option
        mgr.get_module_option.side_effect = cls.mock_get_module_option
        # kludge below
        mgr.set_store.side_effect = cls.mock_set_module_option
        mgr.get_store.side_effect = cls.mock_get_module_option

    @classmethod
    def get_key(cls, key):
        return cls.CONFIG_KEY_DICT.get(key, None)


class CLICommandTestMixin(KVStoreMockMixin):
    @classmethod
    def exec_cmd(cls, cmd, **kwargs):
        return exec_dashboard_cmd(None, cmd, **kwargs)


class FakeFsMixin(object):
    fs = fake_filesystem.FakeFilesystem()
    f_open = fake_filesystem.FakeFileOpen(fs)
    f_os = fake_filesystem.FakeOsModule(fs)
    builtins_open = 'builtins.open'


class ControllerTestCase(helper.CPWebCase):
    _endpoints_cache = {}

    @classmethod
    def setup_controllers(cls, ctrl_classes, base_url=''):
        if not isinstance(ctrl_classes, list):
            ctrl_classes = [ctrl_classes]
        mapper = cherrypy.dispatch.RoutesDispatcher()
        endpoint_list = []
        for ctrl in ctrl_classes:
            inst = ctrl()

            # We need to cache the controller endpoints because
            # BaseController#endpoints method is not idempontent
            # and a controller might be needed by more than one
            # unit test.
            if ctrl not in cls._endpoints_cache:
                ctrl_endpoints = ctrl.endpoints()
                cls._endpoints_cache[ctrl] = ctrl_endpoints

            ctrl_endpoints = cls._endpoints_cache[ctrl]
            for endpoint in ctrl_endpoints:
                endpoint.inst = inst
                endpoint_list.append(endpoint)
        endpoint_list = sorted(endpoint_list, key=lambda e: e.url)
        for endpoint in endpoint_list:
            generate_controller_routes(endpoint, mapper, base_url)
        if base_url == '':
            base_url = '/'
        cherrypy.tree.mount(None, config={
            base_url: {'request.dispatch': mapper}})

    def __init__(self, *args, **kwargs):
        cherrypy.tools.authenticate = AuthManagerTool()
        cherrypy.tools.dashboard_exception_handler = HandlerWrapperTool(dashboard_exception_handler,
                                                                        priority=31)
        cherrypy.config.update({
            'error_page.default': json_error_page,
            'tools.json_in.on': True,
            'tools.json_in.force': False
        })
        PLUGIN_MANAGER.hook.configure_cherrypy(config=cherrypy.config)
        super(ControllerTestCase, self).__init__(*args, **kwargs)

    def _request(self, url, method, data=None, headers=None):
        if not data:
            b = None
            h = None
        else:
            b = json.dumps(data)
            h = [('Content-Type', 'application/json'),
                 ('Content-Length', str(len(b)))]
        if headers:
            h = headers
        self.getPage(url, method=method, body=b, headers=h)

    def _get(self, url, headers=None):
        self._request(url, 'GET', headers=headers)

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

        res = self.json_body()
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
                    self.tc._get('/api/task?name={}'.format(self.task_name))
                    res = self.tc.json_body()
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

        if 'status' in thread.res_task['exception']:
            self.status = thread.res_task['exception']['status']
        else:
            self.status = 500
        self.body = json.dumps(thread.res_task['exception'])

    def _task_post(self, url, data=None, timeout=60):
        self._task_request('POST', url, data, timeout)

    def _task_delete(self, url, timeout=60):
        self._task_request('DELETE', url, None, timeout)

    def _task_put(self, url, data=None, timeout=60):
        self._task_request('PUT', url, data, timeout)

    def json_body(self):
        body_str = self.body.decode('utf-8') if isinstance(self.body, bytes) else self.body
        return json.loads(body_str)

    def assertJsonBody(self, data, msg=None):  # noqa: N802
        """Fail if value != self.body."""
        json_body = self.json_body()
        if data != json_body:
            if msg is None:
                msg = 'expected body:\n%r\n\nactual body:\n%r' % (
                    data, json_body)
            self._handlewebError(msg)

    def assertInJsonBody(self, data, msg=None):  # noqa: N802
        json_body = self.json_body()
        if data not in json_body:
            if msg is None:
                msg = 'expected %r to be in %r' % (data, json_body)
            self._handlewebError(msg)
