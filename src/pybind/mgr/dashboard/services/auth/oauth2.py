import json
from typing import Dict, List
from urllib.parse import quote

import cherrypy
import requests

from ... import mgr
from ...services.auth import BaseAuth, SSOAuth, decode_jwt_segment
from ...tools import prepare_url_prefix
from ..access_control import Role, User, UserAlreadyExists


class OAuth2(SSOAuth):
    LOGIN_URL = 'auth/oauth2/login'
    LOGOUT_URL = 'auth/oauth2/logout'
    sso = True

    class OAuth2Config(BaseAuth.Config):
        pass

    @staticmethod
    def enabled():
        return mgr.get_module_option('sso_oauth2')

    def to_dict(self) -> 'BaseAuth.Config':
        return self.OAuth2Config()

    @classmethod
    def from_dict(cls, s_dict: OAuth2Config) -> 'OAuth2':
        # pylint: disable=unused-argument
        return OAuth2()

    @classmethod
    def get_auth_name(cls):
        return cls.__name__.lower()

    @classmethod
    # pylint: disable=protected-access
    def get_token(cls, request: cherrypy._ThreadLocalProxy) -> str:
        try:
            return request.cookie['token'].value
        except KeyError:
            return request.headers.get('X-Access-Token')

    @classmethod
    def set_token(cls, token: str):
        cherrypy.request.jwt = token
        cherrypy.request.jwt_payload = cls.get_token_payload()
        cherrypy.request.user = cls.get_user(token)

    @classmethod
    def get_token_payload(cls) -> Dict:
        try:
            return cherrypy.request.jwt_payload
        except AttributeError:
            pass
        try:
            return decode_jwt_segment(cherrypy.request.jwt.split(".")[1])
        except AttributeError:
            return {}

    @classmethod
    def set_token_payload(cls, token):
        cherrypy.request.jwt_payload = decode_jwt_segment(token.split(".")[1])

    @classmethod
    def get_user_roles(cls):
        roles: List[Role] = []
        user_roles: List[Role] = []
        try:
            jwt_payload = cherrypy.request.jwt_payload
        except AttributeError:
            raise cherrypy.HTTPError()

        # check for client roes
        if 'resource_access' in jwt_payload:
            # Find the first value where the key is not 'account'
            roles = next((value['roles'] for key, value in jwt_payload['resource_access'].items()
                          if key != "account"), user_roles)
        # check for global roles
        elif 'realm_access' in jwt_payload:
            roles = next((value['roles'] for _, value in jwt_payload['realm_access'].items()),
                         user_roles)
        else:
            raise cherrypy.HTTPError()
        user_roles = Role.map_to_system_roles(roles)
        return user_roles

    @classmethod
    def get_user(cls, token: str) -> User:
        try:
            return cherrypy.request.user
        except AttributeError:
            cls.set_token_payload(token)
            cls._create_user()
        return cherrypy.request.user

    @classmethod
    def _create_user(cls):
        try:
            jwt_payload = cherrypy.request.jwt_payload
        except AttributeError:
            raise cherrypy.HTTPError()
        try:
            user = mgr.ACCESS_CTRL_DB.create_user(
                jwt_payload['sub'], None, jwt_payload['name'], jwt_payload['email'])
        except UserAlreadyExists:
            user = mgr.ACCESS_CTRL_DB.get_user(jwt_payload['sub'])
        user.set_roles(cls.get_user_roles())
        # set user last update to token time issued
        user.last_update = jwt_payload['iat']
        cherrypy.request.user = user

    @classmethod
    def reset_user(cls):
        try:
            mgr.ACCESS_CTRL_DB.delete_user(cherrypy.request.user.username)
            cherrypy.request.user = None
        except AttributeError:
            raise cherrypy.HTTPError()

    @classmethod
    def get_token_iss(cls, token=''):
        if token:
            cls.set_token_payload(token)
        return cls.get_token_payload()['iss']

    @classmethod
    def get_openid_config(cls, iss):
        msg = 'Failed to logout: could not contact IDP'
        try:
            response = requests.get(f'{iss}/.well-known/openid-configuration')
        except requests.exceptions.RequestException:
            raise cherrypy.HTTPError(500, message=msg)
        if response.status_code != 200:
            raise cherrypy.HTTPError(500, message=msg)
        return json.loads(response.text)

    @classmethod
    def get_login_redirect_url(cls, token) -> str:
        url_prefix = prepare_url_prefix(mgr.get_module_option('url_prefix', default=''))
        return f"{url_prefix}/#/login?access_token={token}"

    @classmethod
    def get_logout_redirect_url(cls, token) -> str:
        openid_config = OAuth2.get_openid_config(OAuth2.get_token_iss(token))
        end_session_url = openid_config.get('end_session_endpoint')
        encoded_end_session_url = quote(end_session_url, safe="")
        url_prefix = prepare_url_prefix(mgr.get_module_option('url_prefix', default=''))
        return f'{url_prefix}/oauth2/sign_out?rd={encoded_end_session_url}'
