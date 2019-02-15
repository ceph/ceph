import mock

from .helper import ControllerTestCase
from ..controllers.rgw import RgwUser


class RgwUserControllerTestCase(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        RgwUser._cp_config['tools.authenticate.on'] = False  # pylint: disable=protected-access
        cls.setup_controllers([RgwUser], '/test')

    @mock.patch('dashboard.controllers.rgw.RgwRESTController.proxy')
    def test_user_list(self, mock_proxy):
        mock_proxy.side_effect = [{
            'count': 3,
            'keys': ['test1', 'test2', 'test3'],
            'truncated': False
        }]
        self._get('/test/api/rgw/user')
        self.assertStatus(200)
        mock_proxy.assert_has_calls([
            mock.call('GET', 'user?list', {})
        ])
        self.assertJsonBody(['test1', 'test2', 'test3'])

    @mock.patch('dashboard.controllers.rgw.RgwRESTController.proxy')
    def test_user_list_marker(self, mock_proxy):
        mock_proxy.side_effect = [{
            'count': 3,
            'keys': ['test1', 'test2', 'test3'],
            'marker': 'foo:bar',
            'truncated': True
        }, {
            'count': 1,
            'keys': ['admin'],
            'truncated': False
        }]
        self._get('/test/api/rgw/user')
        self.assertStatus(200)
        mock_proxy.assert_has_calls([
            mock.call('GET', 'user?list', {}),
            mock.call('GET', 'user?list', {'marker': 'foo:bar'})
        ])
        self.assertJsonBody(['test1', 'test2', 'test3', 'admin'])

    @mock.patch('dashboard.controllers.rgw.RgwRESTController.proxy')
    def test_user_list_duplicate_marker(self, mock_proxy):
        mock_proxy.side_effect = [{
            'count': 3,
            'keys': ['test1', 'test2', 'test3'],
            'marker': 'foo:bar',
            'truncated': True
        }, {
            'count': 3,
            'keys': ['test4', 'test5', 'test6'],
            'marker': 'foo:bar',
            'truncated': True
        }, {
            'count': 1,
            'keys': ['admin'],
            'truncated': False
        }]
        self._get('/test/api/rgw/user')
        self.assertStatus(500)

    @mock.patch('dashboard.controllers.rgw.RgwRESTController.proxy')
    def test_user_list_invalid_marker(self, mock_proxy):
        mock_proxy.side_effect = [{
            'count': 3,
            'keys': ['test1', 'test2', 'test3'],
            'marker': 'foo:bar',
            'truncated': True
        }, {
            'count': 3,
            'keys': ['test4', 'test5', 'test6'],
            'marker': '',
            'truncated': True
        }, {
            'count': 1,
            'keys': ['admin'],
            'truncated': False
        }]
        self._get('/test/api/rgw/user')
        self.assertStatus(500)
