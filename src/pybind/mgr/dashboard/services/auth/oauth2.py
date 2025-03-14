
import importlib
import json
import logging
from typing import Dict, List
from urllib.parse import quote

import cherrypy
import requests

from ... import mgr
from ...services.auth import BaseAuth, SSOAuth, decode_jwt_segment
from ...tools import prepare_url_prefix
from ..access_control import Role, User, UserAlreadyExists

try:
    jmespath = importlib.import_module("jmespath")
except ModuleNotFoundError:
    logging.error("Module 'jmespath' is not installed.")

logger = logging.getLogger('services.oauth2')


class OAuth2(SSOAuth):
    LOGIN_URL = 'auth/oauth2/login'
    LOGOUT_URL = 'auth/oauth2/logout'
    sso = True

    class OAuth2Config(BaseAuth.Config):
        roles_path: str

    def __init__(self, roles_path=None):
        self.roles_path = roles_path

    def get_roles_path(self):
        return self.roles_path

    @staticmethod
    def enabled():
        return mgr.get_module_option('sso_oauth2')

    def to_dict(self) -> 'OAuth2Config':
        return {
            'roles_path': self.roles_path
        }

    @classmethod
    def from_dict(cls, s_dict: OAuth2Config) -> 'OAuth2':
        try:
            return OAuth2(s_dict['roles_path'])
        except KeyError:
            return OAuth2({})

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
        roles: List[str] = []
        user_roles: List[Role] = []
        try:
            jwt_payload = cherrypy.request.jwt_payload
        except AttributeError:
            raise cherrypy.HTTPError(401)

        if jmespath and getattr(mgr.SSO_DB.config, 'roles_path', None):
            logger.debug("Using 'roles_path' to fetch roles")
            roles = jmespath.search(mgr.SSO_DB.config.roles_path, jwt_payload)
        # e.g Keycloak
        elif 'resource_access' in jwt_payload or 'realm_access' in jwt_payload:
            logger.debug("Using 'resource_access' or 'realm_access' to fetch roles")
            roles = jmespath.search(
                "resource_access.*[?@!='account'].roles[] || realm_access.roles[]",
                jwt_payload)
        elif 'roles' in jwt_payload:
            logger.debug("Using 'roles' to fetch roles")
            roles = jwt_payload['roles']
            if isinstance(roles, str):
                roles = [roles]
        else:
            raise cherrypy.HTTPError(403)
        user_roles = Role.map_to_system_roles(roles or [])
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
            logger.debug("User already exists")
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
