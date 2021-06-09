import unittest

from .. import mgr
from ..services.auth import JwtManager


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
