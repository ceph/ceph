# -*- coding: utf-8 -*-
import unittest

from mock import patch
from .. import mgr
from ..rest_client import RestClient


class RestClientTest(unittest.TestCase):
    def setUp(self):
        settings = {'REST_REQUESTS_TIMEOUT': 45}
        mgr.get_config.side_effect = settings.get

    def test_timeout_auto_set(self):
        with patch('requests.Session.request') as mock_request:
            rest_client = RestClient('localhost', 8000)
            rest_client.session.request('GET', '/test')
            mock_request.assert_called_with('GET', '/test', timeout=45)

    def test_timeout_auto_set_arg(self):
        with patch('requests.Session.request') as mock_request:
            rest_client = RestClient('localhost', 8000)
            rest_client.session.request(
                'GET', '/test', None, None, None, None,
                None, None, None)
            mock_request.assert_called_with(
                'GET', '/test', None, None, None, None,
                None, None, None, timeout=45)

    def test_timeout_no_auto_set_kwarg(self):
        with patch('requests.Session.request') as mock_request:
            rest_client = RestClient('localhost', 8000)
            rest_client.session.request('GET', '/test', timeout=20)
            mock_request.assert_called_with('GET', '/test', timeout=20)

    def test_timeout_no_auto_set_arg(self):
        with patch('requests.Session.request') as mock_request:
            rest_client = RestClient('localhost', 8000)
            rest_client.session.request(
                'GET', '/test', None, None, None, None,
                None, None, 40)
            mock_request.assert_called_with(
                'GET', '/test', None, None, None, None,
                None, None, 40)
