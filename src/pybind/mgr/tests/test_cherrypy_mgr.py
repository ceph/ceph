import unittest
from unittest import mock
import cherrypy
import cherrypy_mgr
import logging

from cherrypy_mgr import CherryPyMgr, CherryPyAccessFilter


class TestCherryPyAccessFilter(unittest.TestCase):
    def setUp(self):
        self.filter = CherryPyAccessFilter(interval=300.0)
        self.monotonic_patcher = mock.patch('cherrypy_mgr.time.monotonic', return_value=100.0)
        self.mock_monotonic = self.monotonic_patcher.start()

    def tearDown(self):
        self.monotonic_patcher.stop()

    def _record(self, name: str, message: str) -> logging.LogRecord:
        return logging.LogRecord(name, logging.INFO, __file__, 0, message, (), None)

    def test_rate_limits_200_within_interval(self):
        record = self._record(
            'cherrypy.access.123',
            '127.0.0.1 - - [time] "GET /metrics HTTP/1.1" 200 1 "" "Prometheus/3.6.0"'
        )

        self.assertTrue(self.filter.filter(record))
        self.assertFalse(self.filter.filter(record))

    def test_non_200_always_passes(self):
        record = self._record(
            'cherrypy.access.123',
            '127.0.0.1 - - [time] "GET /metrics HTTP/1.1" 500 1 "" "Prometheus/3.6.0"'
        )

        self.assertTrue(self.filter.filter(record))
        self.assertTrue(self.filter.filter(record))
        self.assertEqual(self.filter._last, {})

    def test_regex_key_extraction_without_query_params(self):
        record = self._record(
            'cherrypy.access.123',
            '127.0.0.1 - - [time] "GET /metrics HTTP/1.1" 200 1 "" "Prometheus/3.6.0"'
        )

        self.assertTrue(self.filter.filter(record))
        self.assertEqual(self.filter._last, {'/metrics': 100.0})

    def test_regex_key_extraction_with_query_params(self):
        record = self._record(
            'cherrypy.access.123',
            '127.0.0.1 - - [time] "GET /sd/prometheus/sd-config?service=ceph HTTP/1.1" 200 1 "" "Prometheus/3.6.0"'
        )

        self.assertTrue(self.filter.filter(record))
        self.assertEqual(
            self.filter._last,
            {'/sd/prometheus/sd-config?service=ceph': 100.0}
        )

    def test_fallback_when_regex_does_not_match(self):
        message = 'raw" 200 message without an HTTP request pattern'
        record = self._record('cherrypy.access.123', message)

        self.assertTrue(self.filter.filter(record))
        self.assertEqual(
            self.filter._last,
            {'raw" 200 message without an HTTP request pattern': 100.0}
        )


class TestCherryPyMgr(unittest.TestCase):
    def setUp(self):
        CherryPyMgr._trees = {}
        self.patcher_engine = mock.patch('cherrypy_mgr.cherrypy.engine')
        self.mock_engine = self.patcher_engine.start()
        self.mock_engine.state = cherrypy.engine.states.STOPPED

        self.patcher_config = mock.patch('cherrypy_mgr.cherrypy.config')
        self.mock_config = self.patcher_config.start()

        self.patcher_server = mock.patch('cherrypy_mgr.cherrypy.server')
        self.mock_server = self.patcher_server.start()

    def tearDown(self):
        self.patcher_engine.stop()
        self.patcher_config.stop()
        self.patcher_server.stop()
        self.patcher_engine.stop()
    
    @mock.patch('cherrypy_mgr.ServerAdapter')
    @mock.patch('cherrypy_mgr.WSGIServer')
    def test_mount(self, mock_wsgi_server, mock_server_adapter):
        tree = mock.MagicMock(spec=cherrypy._cptree.Tree)
        name = 'test_app'
        bind_addr = ('127.0.0.0', 8080)
        ssl_info = None

        adapter, _ = CherryPyMgr.mount(tree, name, bind_addr, ssl_info)

        self.assertIn(name, CherryPyMgr._trees)
        self.assertEqual(CherryPyMgr._trees[name], tree)
        self.mock_server.unsubscribe.assert_called_once()
        self.mock_engine.autoreload.unsubscribe.assert_called_once()
        self.mock_engine.start.assert_called_once()
        mock_wsgi_server.assert_called_with(
            bind_addr=bind_addr,
            wsgi_app=tree,
            numthreads=30,
            server_name='Ceph-Mgr'
        )
        mock_server_adapter.return_value.start.assert_called_once()

    @mock.patch('cherrypy_mgr.ServerAdapter')
    @mock.patch('cherrypy_mgr.WSGIServer')
    def test_mount_engine_already_started(self, mock_wsgi_server, mock_server_adapter):
        self.mock_engine.state = cherrypy.engine.states.STARTED

        tree = mock.MagicMock(spec=cherrypy._cptree.Tree)
        name = 'another_app'
        bind_addr = ('127.0.0.1', 8082)

        adapter, _ = CherryPyMgr.mount(tree, name, bind_addr)

        self.mock_engine.start.assert_not_called()
        mock_server_adapter.return_value.start.assert_called_once()

    @mock.patch('cherrypy_mgr.BuiltinSSLAdapter')
    @mock.patch('cherrypy_mgr.ServerAdapter')
    @mock.patch('cherrypy_mgr.WSGIServer')
    def test_mount_with_ssl(self, mock_wsgi_server, mock_server_adapter, mock_builtin_ssl_adapter):
        tree = mock.MagicMock(spec=cherrypy._cptree.Tree)
        name = 'ssl_app'
        bind_addr = ('127.0.0.1', 8080)
        ssl_info = {
            'cert': '/path/to/cert.pem',
            'key': '/path/to/key.pem',
            'context': 'fake_context'
        }

        CherryPyMgr.mount(tree, name, bind_addr, ssl_info)

        mock_wsgi_server.assert_called_once()
        server_instance = mock_wsgi_server.return_value

        mock_builtin_ssl_adapter.assert_called_once_with(ssl_info['cert'], ssl_info['key'])
        self.assertEqual(mock_builtin_ssl_adapter.return_value.context, 'fake_context')
        self.assertEqual(server_instance.ssl_adapter, mock_builtin_ssl_adapter.return_value)
    
    def test_get_server_config(self):
        tree = cherrypy._cptree.Tree()
        app_one = mock.Mock()
        app_one.config = {'id': 'app_one'}
        
        app_two = mock.Mock()
        app_two.config = {'id': 'app_two'}

        tree.apps['/app_one'] = app_one
        tree.apps['/app_two'] = app_two
        CherryPyMgr._trees['test_app'] = tree

        # get the config of app_two using different mount point formats
        result = CherryPyMgr.get_server_config('test_app', '/app_two')
        self.assertEqual(result, {'id': 'app_two'})
        result = CherryPyMgr.get_server_config('test_app', '/app_two/')
        self.assertEqual(result, {'id': 'app_two'})

        # for app_one, test with mount point '/' and '/app_one'
        result = CherryPyMgr.get_server_config('test_app', '/app_one')
        self.assertEqual(result, {'id': 'app_one'})
        result = CherryPyMgr.get_server_config('test_app', '/')
        self.assertIsNone(result, {'id': 'app_one'})

        # test non-existent app and mount point
        self.assertIsNone(CherryPyMgr.get_server_config('ghost_app'))
        self.assertIsNone(CherryPyMgr.get_server_config('test_app', '/missing'))
