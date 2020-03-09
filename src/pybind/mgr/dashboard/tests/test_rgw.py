import mock

from . import ControllerTestCase
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

    @mock.patch('dashboard.controllers.rgw.RgwRESTController.proxy')
    @mock.patch.object(RgwUser, '_keys_allowed')
    def test_user_get_with_keys(self, keys_allowed, mock_proxy):
        keys_allowed.return_value = True
        mock_proxy.return_value = {
            'tenant': '',
            'user_id': 'my_user_id',
            'keys': [],
            'swift_keys': []
        }
        self._get('/test/api/rgw/user/testuser')
        self.assertStatus(200)
        self.assertInJsonBody('keys')
        self.assertInJsonBody('swift_keys')

    @mock.patch('dashboard.controllers.rgw.RgwRESTController.proxy')
    @mock.patch.object(RgwUser, '_keys_allowed')
    def test_user_get_without_keys(self, keys_allowed, mock_proxy):
        keys_allowed.return_value = False
        mock_proxy.return_value = {
            'tenant': '',
            'user_id': 'my_user_id',
            'keys': [],
            'swift_keys': []
        }
        self._get('/test/api/rgw/user/testuser')
        self.assertStatus(200)
        self.assertNotIn('keys', self.jsonBody())
        self.assertNotIn('swift_keys', self.jsonBody())
