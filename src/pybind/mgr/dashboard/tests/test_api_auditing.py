# -*- coding: utf-8 -*-

import json
import re

try:
    import mock
except ImportError:
    import unittest.mock as mock

from .. import mgr
from ..controllers import RESTController, Router
from ..tests import ControllerTestCase, KVStoreMockMixin


# pylint: disable=W0613
@Router('/foo', secure=False)
class FooResource(RESTController):
    def create(self, password):
        pass

    def get(self, key):
        pass

    def delete(self, key):
        pass

    def set(self, key, password, secret_key=None):
        pass


class ApiAuditingTest(ControllerTestCase, KVStoreMockMixin):

    _request_logging = True

    @classmethod
    def setup_server(cls):
        cls.setup_controllers([FooResource])

    def setUp(self):
        self.mock_kv_store()
        mgr.cluster_log = mock.Mock()
        mgr.set_module_option('AUDIT_API_ENABLED', True)
        mgr.set_module_option('AUDIT_API_LOG_PAYLOAD', True)

    def _validate_cluster_log_msg(self, path, method, user, params):
        channel, _, msg = mgr.cluster_log.call_args_list[0][0]
        self.assertEqual(channel, 'audit')
        pattern = r'^\[DASHBOARD\] from=\'(.+)\' path=\'(.+)\' ' \
                  'method=\'(.+)\' user=\'(.+)\' params=\'(.+)\'$'
        m = re.match(pattern, msg)
        self.assertEqual(m.group(2), path)
        self.assertEqual(m.group(3), method)
        self.assertEqual(m.group(4), user)
        self.assertDictEqual(json.loads(m.group(5)), params)

    def test_no_audit(self):
        mgr.set_module_option('AUDIT_API_ENABLED', False)
        self._delete('/foo/test1')
        mgr.cluster_log.assert_not_called()

    def test_no_payload(self):
        mgr.set_module_option('AUDIT_API_LOG_PAYLOAD', False)
        self._delete('/foo/test1')
        _, _, msg = mgr.cluster_log.call_args_list[0][0]
        self.assertNotIn('params=', msg)

    def test_no_audit_get(self):
        self._get('/foo/test1')
        mgr.cluster_log.assert_not_called()

    def test_audit_put(self):
        self._put('/foo/test1', {'password': 'y', 'secret_key': 1234})
        mgr.cluster_log.assert_called_once()
        self._validate_cluster_log_msg('/foo/test1', 'PUT', 'None',
                                       {'key': 'test1',
                                        'password': '***',
                                        'secret_key': '***'})

    def test_audit_post(self):
        with mock.patch('dashboard.services.auth.JwtManager.get_username',
                        return_value='hugo'):
            self._post('/foo?password=1234')
            mgr.cluster_log.assert_called_once()
            self._validate_cluster_log_msg('/foo', 'POST', 'hugo',
                                           {'password': '***'})

    def test_audit_delete(self):
        self._delete('/foo/test1')
        mgr.cluster_log.assert_called_once()
        self._validate_cluster_log_msg('/foo/test1', 'DELETE',
                                       'None', {'key': 'test1'})
