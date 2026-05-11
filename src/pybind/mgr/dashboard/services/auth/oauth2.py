
import json
import logging
import time
from typing import List, Optional
from urllib.parse import quote

import cherrypy
import requests

from ... import mgr
from ...services.auth import BaseAuth, SSOAuth
from ...tools import prepare_url_prefix
from ..access_control import Role, User, UserAlreadyExists

logger = logging.getLogger(__name__)


class OAuth2(SSOAuth):
    LOGIN_URL = 'auth/oauth2/login'
    LOGOUT_URL = 'auth/oauth2/logout'
    sso = True

    class OAuth2Config(BaseAuth.Config):
        roles_path: str
        issuer_url: str

    def __init__(self, roles_path=None, issuer_url=None):
        self.roles_path = roles_path
        self.issuer_url = issuer_url

    def get_roles_path(self):
        return self.roles_path

    @staticmethod
    def enabled():
        return mgr.get_module_option('sso_oauth2')

    def to_dict(self) -> 'OAuth2Config':
        return {
            'roles_path': self.roles_path,
            'issuer_url': self.issuer_url
        }

    @classmethod
    def from_dict(cls, s_dict: OAuth2Config) -> 'OAuth2':
        try:
            return OAuth2(s_dict.get('roles_path'), s_dict.get('issuer_url'))
        except (KeyError, AttributeError):
            return OAuth2({})

    @classmethod
    def get_auth_name(cls):
        return cls.__name__.lower()

    @classmethod
    def get_user_from_headers(cls, request) -> Optional[User]:
        username = request.headers.get('X-User', '')
        if not username:
            return None
        email = request.headers.get('X-Email', '')
        groups = request.headers.get('X-User-Groups', '')

        logger.debug("OAuth2 proxy headers — X-User: %s, X-Email: %s, X-User-Groups: '%s'",
                     username, email, groups)

        try:
            user = mgr.ACCESS_CTRL_DB.create_user(username, None, username, email)
        except UserAlreadyExists:
            user = mgr.ACCESS_CTRL_DB.get_user(username)

        roles = cls._map_groups_to_roles(groups)
        logger.debug("OAuth2 mapped roles: %s", [r.name for r in roles])
        user.set_roles(roles)
        user.last_update = int(time.time())
        return user

    @classmethod
    def _map_groups_to_roles(cls, groups_header: str) -> List[Role]:
        if not groups_header:
            return []
        idp_groups = [g.strip() for g in groups_header.split(',') if g.strip()]
        return Role.map_to_system_roles(idp_groups)

    @classmethod
    def get_login_redirect_url(cls) -> str:
        url_prefix = prepare_url_prefix(mgr.get_module_option('url_prefix', default=''))
        return f"{url_prefix}/#/login"

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
    def get_logout_redirect_url(cls) -> str:
        url_prefix = prepare_url_prefix(mgr.get_module_option('url_prefix', default=''))
        issuer_url = getattr(mgr.SSO_DB.config, 'issuer_url', None)
        if issuer_url:
            openid_config = cls.get_openid_config(issuer_url)
            end_session_url = openid_config.get('end_session_endpoint')
            if end_session_url:
                encoded = quote(end_session_url, safe="")
                return f'{url_prefix}/oauth2/sign_out?rd={encoded}'
        return f'{url_prefix}/oauth2/sign_out'
