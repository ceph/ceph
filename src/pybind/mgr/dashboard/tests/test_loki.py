# -*- coding: utf-8 -*-
try:
    from mock import patch
except ImportError:
    from unittest.mock import patch

from requests import Response

from .. import mgr
from ..controllers.loki import Loki
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
        cls.setup_controllers([Loki])

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
            verify=True)
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
            verify=True)
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
            verify=True)
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
            verify=True)
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

    def test_query_unconfigured_host(self):
        mgr.get_module_option.side_effect = lambda name, default=None: ''

        self._get('/api/loki/query', params={'params': '{job="Cluster Logs"}'})
        self.assertStatus(503)
        self.assertIn(b'Loki API host is not configured', self.body)
