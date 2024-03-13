# -*- coding: utf-8 -*-

import http.cookies
import json
import logging
import sys

import cherrypy

from .. import mgr
from ..exceptions import InvalidCredentialsError, UserDoesNotExist
from ..services.auth import AuthManager, JwtManager
from ..services.cluster import ClusterModel
from ..settings import Settings
from . import APIDoc, APIRouter, ControllerAuthMixin, EndpointDoc, RESTController, allow_empty_body

# Python 3.8 introduced `samesite` attribute:
# https://docs.python.org/3/library/http.cookies.html#morsel-objects
if sys.version_info < (3, 8):
    http.cookies.Morsel._reserved["samesite"] = "SameSite"  # type: ignore  # pylint: disable=W0212

logger = logging.getLogger('controllers.auth')

AUTH_CHECK_SCHEMA = {
    "username": (str, "Username"),
    "permissions": ({
        "cephfs": ([str], "")
    }, "List of permissions acquired"),
    "sso": (bool, "Uses single sign on?"),
    "pwdUpdateRequired": (bool, "Is password update required?")
}


@APIRouter('/auth', secure=False)
@APIDoc("Initiate a session with Ceph", "Auth")
class Auth(RESTController, ControllerAuthMixin):
    """
    Provide authenticates and returns JWT token.
    """
    # pylint: disable=R0912
    def create(self, username, password):
        user_data = AuthManager.authenticate(username, password)
        user_perms, pwd_expiration_date, pwd_update_required = None, None, None
        max_attempt = Settings.ACCOUNT_LOCKOUT_ATTEMPTS
        origin = cherrypy.request.headers.get('Origin', None)
        try:
            fsid = mgr.get('config')['fsid']
        except KeyError:
            fsid = ''
        if max_attempt == 0 or mgr.ACCESS_CTRL_DB.get_attempt(username) < max_attempt:
            if user_data:
                user_perms = user_data.get('permissions')
                pwd_expiration_date = user_data.get('pwdExpirationDate', None)
                pwd_update_required = user_data.get('pwdUpdateRequired', False)

            if isinstance(Settings.MULTICLUSTER_CONFIG, str):
                try:
                    item_to_dict = json.loads(Settings.MULTICLUSTER_CONFIG)
                except json.JSONDecodeError:
                    item_to_dict = {}
                multicluster_config = item_to_dict.copy()
            else:
                multicluster_config = Settings.MULTICLUSTER_CONFIG.copy()
            try:
                if fsid in multicluster_config['config']:
                    existing_entries = multicluster_config['config'][fsid]
                    if not any(entry['user'] == username for entry in existing_entries):
                        existing_entries.append({
                            "name": fsid,
                            "url": origin,
                            "cluster_alias": "local-cluster",
                            "user": username
                        })
                else:
                    multicluster_config['config'][fsid] = [{
                        "name": fsid,
                        "url": origin,
                        "cluster_alias": "local-cluster",
                        "user": username
                    }]

            except KeyError:
                multicluster_config = {
                    'current_url': origin,
                    'current_user': username,
                    'hub_url': origin,
                    'config': {
                        fsid: [
                            {
                                "name": fsid,
                                "url": origin,
                                "cluster_alias": "local-cluster",
                                "user": username
                            }
                        ]
                    }
                }
            Settings.MULTICLUSTER_CONFIG = multicluster_config

            if user_perms is not None:
                url_prefix = 'https' if mgr.get_localized_module_option('ssl') else 'http'

                logger.info('Login successful: %s', username)
                mgr.ACCESS_CTRL_DB.reset_attempt(username)
                mgr.ACCESS_CTRL_DB.save()
                token = JwtManager.gen_token(username)

                # For backward-compatibility: PyJWT versions < 2.0.0 return bytes.
                token = token.decode('utf-8') if isinstance(token, bytes) else token

                self._set_token_cookie(url_prefix, token)
                return {
                    'token': token,
                    'username': username,
                    'permissions': user_perms,
                    'pwdExpirationDate': pwd_expiration_date,
                    'sso': mgr.SSO_DB.protocol == 'saml2',
                    'pwdUpdateRequired': pwd_update_required
                }
            mgr.ACCESS_CTRL_DB.increment_attempt(username)
            mgr.ACCESS_CTRL_DB.save()
        else:
            try:
                user = mgr.ACCESS_CTRL_DB.get_user(username)
                user.enabled = False
                mgr.ACCESS_CTRL_DB.save()
                logging.warning('Maximum number of unsuccessful log-in attempts '
                                '(%d) reached for '
                                'username "%s" so the account was blocked. '
                                'An administrator will need to re-enable the account',
                                max_attempt, username)
                raise InvalidCredentialsError
            except UserDoesNotExist:
                raise InvalidCredentialsError
        logger.info('Login failed: %s', username)
        raise InvalidCredentialsError

    @RESTController.Collection('POST')
    @allow_empty_body
    def logout(self):
        logger.debug('Logout successful')
        token = JwtManager.get_token_from_header()
        JwtManager.blocklist_token(token)
        self._delete_token_cookie(token)
        redirect_url = '#/login'
        if mgr.SSO_DB.protocol == 'saml2':
            redirect_url = 'auth/saml2/slo'
        return {
            'redirect_url': redirect_url
        }

    def _get_login_url(self):
        if mgr.SSO_DB.protocol == 'saml2':
            return 'auth/saml2/login'
        return '#/login'

    @RESTController.Collection('POST', query_params=['token'])
    @EndpointDoc("Check token Authentication",
                 parameters={'token': (str, 'Authentication Token')},
                 responses={201: AUTH_CHECK_SCHEMA})
    def check(self, token):
        if token:
            user = JwtManager.get_user(token)
            if user:
                return {
                    'username': user.username,
                    'permissions': user.permissions_dict(),
                    'sso': mgr.SSO_DB.protocol == 'saml2',
                    'pwdUpdateRequired': user.pwd_update_required
                }
        return {
            'login_url': self._get_login_url(),
            'cluster_status': ClusterModel.from_db().dict()['status']
        }
