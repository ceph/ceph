import json
import unittest

try:
    from mock import patch
except ImportError:
    from unittest.mock import patch

from . import ControllerTestCase, KVStoreMockMixin
from ..controllers.grafana import Grafana
from ..grafana import GrafanaRestClient
from ..settings import Settings


class GrafanaTest(ControllerTestCase, KVStoreMockMixin):
    @classmethod
    def setup_server(cls):
        # pylint: disable=protected-access
        Grafana._cp_config['tools.authenticate.on'] = False
        cls.setup_controllers([Grafana])

    def setUp(self):
        self.mock_kv_store()

    @staticmethod
    def server_settings(
            url='http://localhost:3000',
            user='admin',
            password='admin',
    ):
        if url is not None:
            Settings.GRAFANA_API_URL = url
        if user is not None:
            Settings.GRAFANA_API_USERNAME = user
        if password is not None:
            Settings.GRAFANA_API_PASSWORD = password

    def test_url(self):
        self.server_settings()
        self._get('/api/grafana/url')
        self.assertStatus(200)
        self.assertJsonBody({'instance': 'http://localhost:3000'})

    def test_validation(self):
        self.server_settings()
        self._get('/api/grafana/validation/foo')
        self.assertStatus(500)

    def test_dashboards_unavailable_no_url(self):
        self.server_settings(url=None)
        self._post('/api/grafana/dashboards')
        self.assertStatus(500)

    def test_dashboards_unavailable_no_user(self):
        self.server_settings(user=None)
        self._post('/api/grafana/dashboards')
        self.assertStatus(500)

    def test_dashboards_unavailable_no_password(self):
        self.server_settings(password=None)
        self._post('/api/grafana/dashboards')
        self.assertStatus(500)


class GrafanaRestClientTest(unittest.TestCase, KVStoreMockMixin):
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
    }
    payload = json.dumps({
        'dashboard': 'foo',
        'overwrite': True
    })

    def setUp(self):
        self.mock_kv_store()
        Settings.GRAFANA_API_URL = 'https://foo/bar'
        Settings.GRAFANA_API_USERNAME = 'xyz'
        Settings.GRAFANA_API_PASSWORD = 'abc'
        Settings.GRAFANA_API_SSL_VERIFY = True

    def test_ssl_verify_url_validation(self):
        with patch('requests.request') as mock_request:
            rest_client = GrafanaRestClient()
            rest_client.url_validation('FOO', Settings.GRAFANA_API_URL)
            mock_request.assert_called_with('FOO', Settings.GRAFANA_API_URL,
                                            verify=True)

    def test_no_ssl_verify_url_validation(self):
        Settings.GRAFANA_API_SSL_VERIFY = False
        with patch('requests.request') as mock_request:
            rest_client = GrafanaRestClient()
            rest_client.url_validation('BAR', Settings.GRAFANA_API_URL)
            mock_request.assert_called_with('BAR', Settings.GRAFANA_API_URL,
                                            verify=False)

    def test_ssl_verify_push_dashboard(self):
        with patch('requests.post') as mock_request:
            rest_client = GrafanaRestClient()
            rest_client.push_dashboard('foo')
            mock_request.assert_called_with(
                Settings.GRAFANA_API_URL + '/api/dashboards/db',
                auth=(Settings.GRAFANA_API_USERNAME,
                      Settings.GRAFANA_API_PASSWORD),
                data=self.payload, headers=self.headers, verify=True)

    def test_no_ssl_verify_push_dashboard(self):
        Settings.GRAFANA_API_SSL_VERIFY = False
        with patch('requests.post') as mock_request:
            rest_client = GrafanaRestClient()
            rest_client.push_dashboard('foo')
            mock_request.assert_called_with(
                Settings.GRAFANA_API_URL + '/api/dashboards/db',
                auth=(Settings.GRAFANA_API_USERNAME,
                      Settings.GRAFANA_API_PASSWORD),
                data=self.payload, headers=self.headers, verify=False)
