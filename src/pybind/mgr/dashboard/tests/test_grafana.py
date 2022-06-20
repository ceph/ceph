import json
import unittest

try:
    from mock import patch
except ImportError:
    from unittest.mock import patch

from requests import RequestException

from ..controllers.grafana import Grafana
from ..grafana import GrafanaRestClient
from ..settings import Settings
from ..tests import ControllerTestCase, KVStoreMockMixin


class GrafanaTest(ControllerTestCase, KVStoreMockMixin):
    @classmethod
    def setup_server(cls):
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

    @patch('dashboard.controllers.grafana.GrafanaRestClient.url_validation')
    def test_validation_endpoint_returns(self, url_validation):
        """
        The point of this test is to see that `validation` is an active endpoint that returns a 200
        status code.
        """
        url_validation.return_value = b'404'
        self.server_settings()
        self._get('/api/grafana/validation/foo')
        self.assertStatus(200)
        self.assertBody(b'"404"')

    @patch('dashboard.controllers.grafana.GrafanaRestClient.url_validation')
    def test_validation_endpoint_fails(self, url_validation):
        url_validation.side_effect = RequestException
        self.server_settings()
        self._get('/api/grafana/validation/bar')
        self.assertStatus(400)
        self.assertJsonBody({'detail': '', 'code': 'Error', 'component': 'grafana'})

    def test_dashboards_unavailable_no_url(self):
        self.server_settings(url="")
        self._post('/api/grafana/dashboards')
        self.assertStatus(500)

    @patch('dashboard.controllers.grafana.GrafanaRestClient.push_dashboard')
    def test_dashboards_unavailable_no_user(self, pd):
        pd.side_effect = RequestException
        self.server_settings(user="")
        self._post('/api/grafana/dashboards')
        self.assertStatus(500)

    def test_dashboards_unavailable_no_password(self):
        self.server_settings(password="")
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
