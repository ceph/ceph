import base64
import json
import unittest
from unittest.mock import Mock, MagicMock, patch

from dashboard.services.access_control import ADMIN_ROLE
from dashboard.services.auth.oauth2 import OAuth2
from dashboard.services.sso import load_sso_db
from dashboard.tests import CLICommandTestMixin

class OAuth2TestMixin:
    _JWT_PAYLOAD = \
    {
       "exp":1723561265,
       "iat":1723560965,
       "auth_time":1723560960,
       "jti":"67d77213-b8ec-4193-9d81-af51a5f42bf5",
       "iss":"http://127.0.0.1:8080/realms/ceph",
       "aud":[
          "oauth2-proxy",
          "account"
       ],
       "sub":"75bd4c99-c7cc-43ec-a685-d787f52cf16f",
       "typ":"Bearer",
       "azp":"oauth2-proxy",
       "sid":"8ebde838-957a-4158-ab9b-028050c9a61c",
       "acr":"1",
       "allowed-origins":[
          "http://127.0.0.1",
          "https://127.0.0.1"
       ],
       "realm_access":{
          "roles":[
             "default-roles-ceph",
             "offline_access",
             "uma_authorization"
          ]
       },
       "resource_access":{
          "oauth2-proxy":{
            "roles":[
                "admin"
            ]
          },
          "clienttest":{
            "roles":[
                "clienttestrole"
            ]
          },
          "account":{
             "roles":[
                "manage-account",
                "manage-account",
                "manage-account-links",
                "view-profile"
             ]
          }
       },
       "scope":"openid email profile",
       "email_verified":True,
       "name":"admin test",
       "preferred_username":"admin123",
       "given_name":"test",
       "family_name":"test",
       "email":"admin@127.0.0.1"
    }
    _ENCODED_HEADER = base64.b64encode(bytes("header", "utf-8")).decode('utf-8')
    _ENCODED_PAYLOAD = base64.b64encode(bytes(str(_JWT_PAYLOAD), "utf-8")).decode('utf-8')
    _ENCODED_SIGNATURE = base64.b64encode(bytes("signature", "utf-8")).decode('utf-8')
    _JWT = _ENCODED_HEADER + "." + _ENCODED_PAYLOAD + "." + _ENCODED_SIGNATURE


class OAuth2Test(unittest.TestCase, CLICommandTestMixin, OAuth2TestMixin):

    @classmethod
    def setUpClass(cls) -> None:
        cls.mock_kv_store()
        load_sso_db()
        # load_access_control_db()

    def test_get_token_from_cookie(self):
        request = MagicMock()
        request.cookie = {"token": Mock(value="token_value")}
        token = OAuth2._get_token_from_cookie(request)
        self.assertEqual(token, "token_value")

    def test_get_token_from_headers(self):
        request = MagicMock()
        request.headers.get = MagicMock(return_value="token_value")
        token = OAuth2._get_token_from_headers(request)
        self.assertEqual(token, "token_value")

    @patch("cherrypy.request")
    def test_get_token_payload(self, mock_request):
        mock_request.jwt_payload = self._JWT_PAYLOAD
        token_payload = OAuth2.token_payload()
        self.assertEqual(token_payload, self._JWT_PAYLOAD)

    @patch("cherrypy.request")
    def test_set_token_payload(self, mock_request):
        with patch("dashboard.services.auth.oauth2.decode_jwt_segment", return_value=self._JWT_PAYLOAD) as mock_decode:
            OAuth2.set_token_payload(self._JWT)
            mock_decode.assert_called_once_with(self._JWT.split(".")[1])

            self.assertEqual(mock_request.jwt_payload, self._JWT_PAYLOAD)

    def test_get_client_roles(self):
        self.assertEqual(OAuth2._client_roles(self._JWT_PAYLOAD), ["admin"])

    def test_get_realm_roles(self):
        self.assertEqual(OAuth2._realm_roles(self._JWT_PAYLOAD), [
             "default-roles-ceph",
             "offline_access",
             "uma_authorization"
          ])

    def test_get_user_roles(self):
        with patch(
            "dashboard.services.auth.oauth2.OAuth2.token_payload",
             return_value = self._JWT_PAYLOAD):
            self.assertEqual(OAuth2.user_roles(), [ADMIN_ROLE])

    @patch("cherrypy.request")
    @patch("dashboard.mgr.ACCESS_CTRL_DB")
    def test_create_user(self, mock_ACCESS_CTRL_DB, mock_request):
        with patch(
            "dashboard.services.auth.oauth2.OAuth2.token_payload",
             return_value = self._JWT_PAYLOAD):
            user = MagicMock(username = self._JWT_PAYLOAD['sub'], password = 'password')
            mock_ACCESS_CTRL_DB.create_user = MagicMock(return_value = user)
            OAuth2._create_user()
            mock_ACCESS_CTRL_DB.create_user.assert_called_once_with(
                self._JWT_PAYLOAD['sub'],
                None,
                self._JWT_PAYLOAD['name'],
                self._JWT_PAYLOAD['email'])
            self.assertEqual(mock_request.user, user)

    @patch("cherrypy.request")
    @patch("dashboard.mgr.ACCESS_CTRL_DB")
    def test_reset_user(self, mock_ACCESS_CTRL_DB, mock_request):
        mock_request.user = MagicMock(username = self._JWT_PAYLOAD['sub'], password = 'password')
        OAuth2.reset_user()
        mock_ACCESS_CTRL_DB.delete_user.assert_called_once_with(self._JWT_PAYLOAD['sub'])
        self.assertIsNone(mock_request.user)

    def get_token_iss(self):
        self.assertEqual(OAuth2.token_iss(self._JWT), self._JWT_PAYLOAD['iss'])

    @patch("requests.get")
    def test_get_openid_config(self, mock_request):
        text = '{"foo": "bar"}'
        mock_request.return_value = MagicMock(status_code = 200, text = text)
        self.assertEqual(OAuth2.openid_config(self._JWT_PAYLOAD['iss']), json.loads(text))
        mock_request.assert_called_once_with(f"{self._JWT_PAYLOAD['iss']}/.well-known/openid-configuration")

    @patch("dashboard.services.auth.oauth2.prepare_url_prefix", return_value = "url_prefix")
    def test_get_login_url(self, mock_prepare_url_prefix):
        self.assertEqual(OAuth2.login_redirect_url(self._JWT), f'{mock_prepare_url_prefix.return_value}/#/login?access_token={self._JWT}')

    @patch("dashboard.services.auth.oauth2.prepare_url_prefix", return_value = "url_prefix")
    @patch.object(OAuth2, "openid_config", return_value = {"end_session_endpoint" : "sign_out_endpoint"})
    @patch("dashboard.services.auth.oauth2.quote", return_value = "sign_out_endpoint")
    def test_get_logout_url(self, mock_quote, mock_openid_config, mock_prepare_url_prefix):
        with patch("dashboard.services.auth.oauth2.decode_jwt_segment", return_value=self._JWT_PAYLOAD):
            end_session_endpoint = mock_openid_config.return_value["end_session_endpoint"]
            self.assertEqual(OAuth2.logout_redirect_url(self._JWT),
                            f'{mock_prepare_url_prefix.return_value}/oauth2/sign_out?rd={end_session_endpoint}')
            mock_quote.assert_called_once_with(end_session_endpoint, safe="")
