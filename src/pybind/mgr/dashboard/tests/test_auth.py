import unittest
from unittest.mock import Mock, patch

from .. import mgr
from ..controllers.auth import Auth
from ..services.auth import JwtManager
from ..tests import ControllerTestCase

mgr.get_module_option.return_value = JwtManager.JWT_TOKEN_TTL
mgr.get_store.return_value = 'jwt_secret'
mgr.ACCESS_CTRL_DB = Mock()
mgr.ACCESS_CTRL_DB.get_attempt.return_value = 1


class JwtManagerTest(unittest.TestCase):

    def test_generate_token_and_decode(self):
        mgr.get_module_option.return_value = JwtManager.JWT_TOKEN_TTL
        mgr.get_store.return_value = 'jwt_secret'

        token = JwtManager.gen_token('my-username')
        self.assertIsInstance(token, str)
        self.assertTrue(token)

        decoded_token = JwtManager.decode_token(token)
        self.assertIsInstance(decoded_token, dict)
        self.assertEqual(decoded_token['iss'], 'ceph-dashboard')
        self.assertEqual(decoded_token['username'], 'my-username')


class AuthTest(ControllerTestCase):

    @classmethod
    def setup_server(cls):
        cls.setup_controllers([Auth])

    def test_request_not_authorized(self):
        self.setup_controllers([Auth], cp_config={'tools.authenticate.on': True})
        self._post('/api/auth/logout')
        self.assertStatus(401)

    @patch('dashboard.controllers.auth.JwtManager.gen_token', Mock(return_value='my-token'))
    @patch('dashboard.mgr.get', Mock(return_value={
        'config': {
            'fsid': '943949f0-ce37-47ca-a33c-3413d46ee9ec'
        }
    }))
    @patch('dashboard.controllers.auth.AuthManager.authenticate', Mock(return_value={
        'permissions': {'rgw': ['read']},
        'pwdExpirationDate': 1000000,
        'pwdUpdateRequired': False
    }))
    def test_login(self):
        self._post('/api/auth', {'username': 'my-user', 'password': 'my-pass'})
        self.assertStatus(201)
        self.assertJsonBody({
            'token': 'my-token',
            'username': 'my-user',
            'permissions': {'rgw': ['read']},
            'pwdExpirationDate': 1000000,
            'sso': False,
            'pwdUpdateRequired': False
        })

    @patch('dashboard.controllers.auth.JwtManager', Mock())
    def test_logout(self):
        self._post('/api/auth/logout')
        self.assertStatus(200)
        self.assertJsonBody({
            'redirect_url': '#/login'
        })
