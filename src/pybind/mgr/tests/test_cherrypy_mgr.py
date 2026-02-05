import unittest
from unittest import mock
import cherrypy
import cherrypy_mgr

from cherrypy_mgr import CherryPyMgr

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
