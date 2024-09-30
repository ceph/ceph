# -*- coding: utf-8 -*-

import http.cookies
import json
import logging
import sys
from typing import Optional

import cherrypy

from .. import mgr
from ..exceptions import InvalidCredentialsError, UserDoesNotExist
from ..services.auth import AuthManager, AuthType, BaseAuth, JwtManager, OAuth2
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

AUTH_SCHEMA = {
    "token": (str, "Authentication Token"),
    "username": (str, "Username"),
    "permissions": ({
        "cephfs": ([str], "")
    }, "List of permissions acquired"),
    "pwdExpirationDate": (str, "Password expiration date"),
    "sso": (bool, "Uses single sign on?"),
    "pwdUpdateRequired": (bool, "Is password update required?")
}


@APIRouter('/auth', secure=False)
@APIDoc("Initiate a session with Ceph", "Auth")
class Auth(RESTController, ControllerAuthMixin):
    """
    Provide authenticates and returns JWT token.
    """
    @EndpointDoc("Dashboard Authentication",
                 parameters={
                     'username': (str, 'Username'),
                     'password': (str, 'Password'),
                     'ttl': (int, 'Token Time to Live (in hours)')
                 },
                 responses={201: AUTH_SCHEMA})
    def create(self, username, password, ttl: Optional[int] = None):
        # pylint: disable=R0912
        user_data = AuthManager.authenticate(username, password)
        user_perms, pwd_expiration_date, pwd_update_required = None, None, None
        max_attempt = Settings.ACCOUNT_LOCKOUT_ATTEMPTS
        origin = cherrypy.request.headers.get('Origin', None)
        try:
            fsid = mgr.get('config')['fsid']
        except KeyError:
            fsid = ''
        if max_attempt == 0 or mgr.ACCESS_CTRL_DB.get_attempt(username) < max_attempt:  # pylint: disable=R1702,line-too-long # noqa: E501
            if user_data:
                user_perms = user_data.get('permissions')
                pwd_expiration_date = user_data.get('pwdExpirationDate', None)
                pwd_update_required = user_data.get('pwdUpdateRequired', False)

            if user_perms is not None:
                url_prefix = 'https' if mgr.get_localized_module_option('ssl') else 'http'

                logger.info('Login successful: %s', username)
                mgr.ACCESS_CTRL_DB.reset_attempt(username)
                mgr.ACCESS_CTRL_DB.save()
                token = JwtManager.gen_token(username, ttl=ttl)

                # For backward-compatibility: PyJWT versions < 2.0.0 return bytes.
                token = token.decode('utf-8') if isinstance(token, bytes) else token

                self._set_token_cookie(url_prefix, token)
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
                        cluster_configurations = multicluster_config['config'][fsid]
                        for config_item in cluster_configurations:
                            if config_item['user'] == username or config_item['cluster_alias'] == 'local-cluster':  # noqa E501  #pylint: disable=line-too-long
                                config_item['token'] = token  # Update token
                                break
                        else:
                            cluster_configurations.append({
                                "name": fsid,
                                "url": origin,
                                "cluster_alias": "local-cluster",
                                "user": username,
                                "token": token
                            })
                    else:
                        multicluster_config['config'][fsid] = [{
                            "name": fsid,
                            "url": origin,
                            "cluster_alias": "local-cluster",
                            "user": username,
                            "token": token
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
                                    "user": username,
                                    "token": token
                                }
                            ]
                        }
                    }
                Settings.MULTICLUSTER_CONFIG = json.dumps(multicluster_config)
                return {
                    'token': token,
                    'username': username,
                    'permissions': user_perms,
                    'pwdExpirationDate': pwd_expiration_date,
                    'sso': BaseAuth.from_protocol(mgr.SSO_DB.protocol).sso,
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
        logger.debug('Logout started')
        token = JwtManager.get_token(cherrypy.request)
        JwtManager.blocklist_token(token)
        self._delete_token_cookie(token)
        return {
            'redirect_url': BaseAuth.from_db(mgr.SSO_DB).LOGOUT_URL,
            'protocol': BaseAuth.from_db(mgr.SSO_DB).get_auth_name()
        }

    @RESTController.Collection('POST', query_params=['token'])
    @EndpointDoc("Check token Authentication",
                 parameters={'token': (str, 'Authentication Token')},
                 responses={201: AUTH_CHECK_SCHEMA})
    def check(self, token):
        if token:
            if mgr.SSO_DB.protocol == AuthType.OAUTH2:
                user = OAuth2.get_user(token)
            else:
                user = JwtManager.get_user(token)
            if user:
                return {
                    'username': user.username,
                    'permissions': user.permissions_dict(),
                    'sso': BaseAuth.from_db(mgr.SSO_DB).sso,
                    'pwdUpdateRequired': user.pwd_update_required
                }
        return {
            'login_url': BaseAuth.from_db(mgr.SSO_DB).LOGIN_URL,
            'cluster_status': ClusterModel.from_db().dict()['status']
        }
