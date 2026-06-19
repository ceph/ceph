# -*- coding: utf-8 -*-
try:
    from mock import patch
except ImportError:
    from unittest.mock import patch

from requests import Response

from .. import mgr
from ..controllers.loki import Loki, LokiSettings
from ..tests import ControllerTestCase


class LokiControllerTest(ControllerTestCase):
    loki_host = 'http://loki:3100'
    loki_host_api = loki_host + '/loki/api/v1'

    @classmethod
    def setup_server(cls):
        settings = {
            'LOKI_API_HOST': cls.loki_host,
        }
        mgr.get_module_option.side_effect = settings.get
        cls.setup_controllers([Loki, LokiSettings])

    @patch('requests.request')
    def test_query(self, mock_request):
        r = Response()
        r.status_code = 200
        r._content = b'{"status":"success","data":{"resultType":"vector","result":[]}}'
        mock_request.return_value = r

        self._get('/api/loki/query', params={'params': 'sum(rate({job="varlogs"}[10m]))'})
        mock_request.assert_called_with(
            'GET',
            self.loki_host_api + '/query',
            json=None,
            params={'query': 'sum(rate({job="varlogs"}[10m]))'},
            verify=True,
            cert=None,
            auth=None,
            headers=None)
        self.assertStatus(200)

    @patch('requests.request')
    def test_query_range(self, mock_request):
        r = Response()
        r.status_code = 200
        r._content = b'{"status":"success","data":{"resultType":"streams","result":[]}}'
        mock_request.return_value = r

        self._get('/api/loki/query_range',
                  params={'params': '{job="varlogs"}', 'start': '1609459200000000000',
                          'end': '1609462800000000000'})
        mock_request.assert_called_with(
            'GET',
            self.loki_host_api + '/query_range',
            json=None,
            params={'query': '{job="varlogs"}',
                    'start': '1609459200000000000',
                    'end': '1609462800000000000'},
            verify=True,
            cert=None,
            auth=None,
            headers=None)
        self.assertStatus(200)

    @patch('requests.request')
    def test_labels(self, mock_request):
        r = Response()
        r.status_code = 200
        r._content = b'{"status":"success","data":["filename","job"]}'
        mock_request.return_value = r

        self._get('/api/loki/labels', params={'since': '6h'})
        mock_request.assert_called_with(
            'GET',
            self.loki_host_api + '/labels',
            json=None,
            params={'since': '6h'},
            verify=True,
            cert=None,
            auth=None,
            headers=None)
        self.assertStatus(200)
        self.assertJsonBody(['filename', 'job'])

    @patch('requests.request')
    def test_label_values(self, mock_request):
        r = Response()
        r.status_code = 200
        r._content = b'{"status":"success","data":["Cluster Logs"]}'
        mock_request.return_value = r

        self._get('/api/loki/label/job/values',
                  params={'params': '{job="Cluster Logs"}', 'since': '24h'})
        mock_request.assert_called_with(
            'GET',
            self.loki_host_api + '/label/job/values',
            json=None,
            params={'query': '{job="Cluster Logs"}', 'since': '24h'},
            verify=True,
            cert=None,
            auth=None,
            headers=None)
        self.assertStatus(200)
        self.assertJsonBody(['Cluster Logs'])

    @patch('requests.request')
    def test_query_error(self, mock_request):
        r = Response()
        r.status_code = 200
        r._content = b'{"status":"error","error":"parse error"}'
        mock_request.return_value = r

        self._get('/api/loki/query', params={'params': 'invalid'})
        self.assertStatus(400)

    @patch('dashboard.controllers.loki.LokiRESTController._discover_loki_host')
    @patch('requests.request')
    def test_query_discovers_loki_host_when_unconfigured(self, mock_request, mock_discover):
        mgr.get_module_option.side_effect = lambda name, default=None: ''
        mock_discover.return_value = self.loki_host

        r = Response()
        r.status_code = 200
        r._content = b'{"status":"success","data":{"resultType":"vector","result":[]}}'
        mock_request.return_value = r

        self._get('/api/loki/query', params={'params': '{job="Cluster Logs"}'})
        mock_discover.assert_called_once()
        mock_request.assert_called_with(
            'GET',
            self.loki_host_api + '/query',
            json=None,
            params={'query': '{job="Cluster Logs"}'},
            verify=True,
            cert=None,
            auth=None,
            headers=None)
        self.assertStatus(200)

    @patch('dashboard.controllers.loki.LokiRESTController._discover_loki_host', return_value=None)
    def test_query_unconfigured_host(self, mock_discover):
        mgr.get_module_option.side_effect = lambda name, default=None: ''

        self._get('/api/loki/query', params={'params': '{job="Cluster Logs"}'})
        self.assertStatus(503)
        self.assertIn(b'Loki API host is not configured', self.body)

    @patch("dashboard.controllers.loki.mgr.get_module_option_ex", return_value='cephadm')
    @patch('dashboard.controllers.loki.mgr.get_localized_store', return_value=None)
    @patch('dashboard.services.orchestrator.OrchClient.instance')
    @patch('dashboard.services.orchestrator.OrchClient.status', return_value={'available': True})
    @patch('dashboard.services.orchestrator.OrchClient.available', return_value=True)
    @patch('requests.request')
    def test_label_values_with_secure_stack(self, mock_request, _mock_available,
                                            _mock_status, mock_instance,
                                            _mock_get_localized_store,
                                            _mock_get_module_option_ex):
        fake_orch = mock_instance.return_value
        fake_orch.monitoring.get_security_config.return_value = {'security_enabled': True}
        fake_orch.monitoring.get_prometheus_access_info.return_value = {
            'user': 'loki-user',
            'password': 'loki-password',
            'certificate': 'ca-cert',
        }

        r = Response()
        r.status_code = 200
        r._content = b'{"status":"success","data":["/var/log/ceph/cephadm.log"]}'
        mock_request.return_value = r

        self._get('/api/loki/label/filename/values',
                  params={'params': '{job="Cluster Logs"}', 'since': '24h'})
        self.assertIsNotNone(mock_request.call_args.kwargs['auth'])
        self.assertStatus(200)

    def test_loki_settings(self):
        self._get('/ui-api/loki/loki-api-host')
        self.assertStatus(200)
        self.assertJsonBody({
            'name': 'LOKI_API_HOST',
            'default': '',
            'type': 'str',
            'value': self.loki_host
        })
