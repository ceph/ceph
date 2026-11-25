import time
import unittest
from unittest.mock import Mock, patch

from dashboard.services.auth import AuthType

from .. import mgr
from ..controllers.auth import Auth
from ..services.access_control import ADMIN_ROLE, READ_ONLY_ROLE
from ..services.auth import JwtManager
from ..tests import ControllerTestCase

mgr.get_module_option.return_value = JwtManager.JWT_TOKEN_TTL
mgr.get_store.return_value = 'jwt_secret'
mgr.ACCESS_CTRL_DB = Mock()
mgr.ACCESS_CTRL_DB.get_attempt.return_value = 1
mgr.SSO_DB.protocol = AuthType.LOCAL


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


class JwtManagerTtlPolicyTest(unittest.TestCase):
    """Users must not be able to pick the expiration of their own tokens."""
    DEFAULT_TTL = JwtManager.JWT_TOKEN_TTL  # 8h
    MAX_TTL = JwtManager.JWT_TOKEN_MAX_TTL  # 30d

    def setUp(self):
        self.options = {'jwt_token_ttl': self.DEFAULT_TTL,
                        'jwt_token_max_ttl': self.MAX_TTL}
        self.users = {}
        JwtManager.logger = Mock()
        mgr.SSO_DB.protocol = AuthType.LOCAL
        # pin the signing secret: other tests may leave side effects on the shared mgr mock
        previous_secret = JwtManager._secret  # pylint: disable=protected-access
        JwtManager._secret = 'jwt_secret'  # pylint: disable=protected-access
        self.addCleanup(setattr, JwtManager, '_secret', previous_secret)
        # restore whatever state the shared mgr mock had, to not affect other tests
        self.addCleanup(setattr, mgr.get_module_option, 'side_effect',
                        mgr.get_module_option.side_effect)
        self.addCleanup(setattr, mgr.get_module_option, 'return_value',
                        mgr.get_module_option.return_value)
        mgr.get_module_option.side_effect = lambda k, d=None: self.options.get(k, d)
        patcher = patch('dashboard.services.auth.auth.AuthManager.get_user',
                        side_effect=self._get_user)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _get_user(self, username):
        from ..exceptions import UserDoesNotExist
        if username not in self.users:
            raise UserDoesNotExist(username)
        return self.users[username]

    def _add_user(self, name, role):
        user = Mock()
        user.roles = {role}
        user.last_update = 0
        self.users[name] = user

    def _lifetime(self, username, ttl):
        decoded = JwtManager.decode_token(JwtManager.gen_token(username, ttl=ttl))
        return decoded['exp'] - decoded['iat']

    def test_default_ttl_without_request(self):
        self._add_user('admin', ADMIN_ROLE)
        self.assertEqual(self._lifetime('admin', None), self.DEFAULT_TTL)

    def test_non_admin_cannot_extend_ttl(self):
        self._add_user('reader', READ_ONLY_ROLE)
        for hours in (24, 24 * 365, 10 ** 9):
            self.assertEqual(self._lifetime('reader', hours), self.DEFAULT_TTL)

    def test_unknown_user_cannot_extend_ttl(self):
        self.assertEqual(self._lifetime('ghost', 24 * 15), self.DEFAULT_TTL)

    def test_anyone_can_shorten_ttl(self):
        self._add_user('reader', READ_ONLY_ROLE)
        self.assertEqual(self._lifetime('reader', 1), 3600)

    def test_invalid_requests_fall_back_to_default(self):
        self._add_user('admin', ADMIN_ROLE)
        for value in (0, -5, 'abc'):
            self.assertEqual(self._lifetime('admin', value), self.DEFAULT_TTL)

    def test_admin_can_extend_ttl_for_multicluster(self):
        # what the multi-cluster form sends: 15 days, in hours
        self._add_user('admin', ADMIN_ROLE)
        self.assertEqual(self._lifetime('admin', 15 * 24), 15 * 24 * 3600)

    def test_admin_ttl_is_capped_by_the_administrator_defined_maximum(self):
        self._add_user('admin', ADMIN_ROLE)
        self.assertEqual(self._lifetime('admin', 24 * 365 * 100), self.MAX_TTL)
        self.options['jwt_token_max_ttl'] = 2 * 24 * 3600
        self.assertEqual(self._lifetime('admin', 24 * 30), 2 * 24 * 3600)

    def test_admin_ttl_can_be_disabled(self):
        self._add_user('admin', ADMIN_ROLE)
        self.options['jwt_token_max_ttl'] = 0
        self.assertEqual(self._lifetime('admin', 24 * 15), self.DEFAULT_TTL)

    def _forge(self, username, lifetime):
        now = int(time.time())
        return JwtManager.encode({'iss': 'ceph-dashboard', 'jti': 'forged-jti',
                                  'exp': now + lifetime, 'iat': now,
                                  'username': username}, 'jwt_secret')

    def test_legacy_long_lived_token_of_non_admin_is_rejected(self):
        # issued by a previous release with ttl=<huge>
        self._add_user('reader', READ_ONLY_ROLE)
        with patch.object(JwtManager, 'is_blocklisted', return_value=False):
            self.assertIsNone(JwtManager.get_user(self._forge('reader', 10 ** 9)))
            self.assertIsNotNone(JwtManager.get_user(self._forge('reader', 3600)))

    def test_long_lived_token_of_admin_is_still_accepted_within_maximum(self):
        # existing multi-cluster tokens keep working
        self._add_user('admin', ADMIN_ROLE)
        with patch.object(JwtManager, 'is_blocklisted', return_value=False):
            self.assertIsNotNone(JwtManager.get_user(self._forge('admin', 15 * 24 * 3600)))
            self.assertIsNone(JwtManager.get_user(self._forge('admin', 10 ** 9)))

    def test_long_lived_token_of_a_deleted_user_is_rejected(self):
        with patch.object(JwtManager, 'is_blocklisted', return_value=False):
            self.assertIsNone(JwtManager.get_user(self._forge('ghost', 3600)))

    def test_lowering_the_default_ttl_does_not_invalidate_current_tokens(self):
        self._add_user('reader', READ_ONLY_ROLE)
        self.options['jwt_token_ttl'] = 600
        with patch.object(JwtManager, 'is_blocklisted', return_value=False):
            self.assertIsNotNone(JwtManager.get_user(self._forge('reader', 8 * 3600)))


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
            'redirect_url': '#/login',
            'protocol': 'local'
        })
