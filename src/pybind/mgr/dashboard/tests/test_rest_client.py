# -*- coding: utf-8 -*-
import unittest
import requests.exceptions

from mock import patch
from urllib3.exceptions import MaxRetryError, ProtocolError
from .. import mgr
from ..rest_client import RequestException, RestClient


class RestClientTest(unittest.TestCase):
    def setUp(self):
        settings = {'REST_REQUESTS_TIMEOUT': 45}
        mgr.get_module_option.side_effect = settings.get

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


class RestClientDoRequestTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mock_requests = patch('requests.Session').start()
        cls.rest_client = RestClient('localhost', 8000, 'UnitTest')

    def test_do_request_exception_no_args(self):
        self.mock_requests().get.side_effect = requests.exceptions.ConnectionError()
        with self.assertRaises(RequestException) as context:
            self.rest_client.do_request('GET', '/test')
            self.assertEqual('UnitTest REST API cannot be reached. Please '
                             'check your configuration and that the API '
                             'endpoint is accessible',
                             context.exception.message)

    def test_do_request_exception_args_1(self):
        self.mock_requests().post.side_effect = requests.exceptions.ConnectionError(
            MaxRetryError('Abc', 'http://xxx.yyy', 'too many redirects'))
        with self.assertRaises(RequestException) as context:
            self.rest_client.do_request('POST', '/test')
            self.assertEqual('UnitTest REST API cannot be reached. Please '
                             'check your configuration and that the API '
                             'endpoint is accessible',
                             context.exception.message)

    def test_do_request_exception_args_2(self):
        self.mock_requests().put.side_effect = requests.exceptions.ConnectionError(
            ProtocolError('Connection broken: xyz'))
        with self.assertRaises(RequestException) as context:
            self.rest_client.do_request('PUT', '/test')
            self.assertEqual('UnitTest REST API cannot be reached. Please '
                             'check your configuration and that the API '
                             'endpoint is accessible',
                             context.exception.message)

    def test_do_request_exception_nested_args(self):
        self.mock_requests().delete.side_effect = requests.exceptions.ConnectionError(
            MaxRetryError('Xyz', 'https://foo.bar',
                          Exception('Foo: [Errno -42] bla bla bla')))
        with self.assertRaises(RequestException) as context:
            self.rest_client.do_request('DELETE', '/test')
            self.assertEqual('UnitTest REST API cannot be reached: bla '
                             'bla bla [errno -42]. Please check your '
                             'configuration and that the API endpoint '
                             'is accessible',
                             context.exception.message)
