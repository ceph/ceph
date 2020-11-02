# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging

import cherrypy

from .. import mgr
from ..exceptions import DashboardException, UserDoesNotExist
from ..services.auth import AuthManager, JwtManager
from . import ApiController, ControllerDoc, EndpointDoc, RESTController, allow_empty_body

logger = logging.getLogger('controllers.auth')

AUTH_CHECK_SCHEMA = {
    "username": (str, "Username"),
    "permissions": ({
        "cephfs": ([str], "")
    }, "List of permissions acquired"),
    "sso": (bool, "Uses single sign on?"),
    "pwdUpdateRequired": (bool, "Is password update required?")
}


@ApiController('/auth', secure=False)
@ControllerDoc("Initiate a session with Ceph", "Auth")
class Auth(RESTController):
    """
    Provide authenticates and returns JWT token.
    """

    def disable_user_access(self, username):
        try:
            user = mgr.ACCESS_CTRL_DB.get_user(username)
        except UserDoesNotExist:
            raise cherrypy.HTTPError(404)

        user.enabled = False
        mgr.ACCESS_CTRL_DB.save()
        #  raise DashboardException(msg='User Disabled',
        #                         code='User Disabled',
        #                         component='auth')

    def create(self, username, password):
        user_data = AuthManager.authenticate(username, password)
        user_perms, pwd_expiration_date, pwd_update_required = None, None, None
        if user_data:
            user_perms = user_data.get('permissions')
            pwd_expiration_date = user_data.get('pwdExpirationDate', None)
            pwd_update_required = user_data.get('pwdUpdateRequired', False)

        if user_perms is not None:
            logger.debug('Login successful')
            token = JwtManager.gen_token(username)
            token = token.decode('utf-8')
            cherrypy.response.headers['Authorization'] = "Bearer: {}".format(token)
            return {
                'token': token,
                'username': username,
                'permissions': user_perms,
                'pwdExpirationDate': pwd_expiration_date,
                'sso': mgr.SSO_DB.protocol == 'saml2',
                'pwdUpdateRequired': pwd_update_required
            }

        logger.debug('Login failed')
        #  Need to store the value of invalid login attempts for a particular username and
        #  incrementing its value after each login failure and then fetch that value and if it
        #  equals to 10 then calling the disable_user method
        self.disable_user_access(username)
        raise DashboardException(msg='Invalid credentials',
                                 code='invalid_credentials',
                                 component='auth')

    @RESTController.Collection('POST')
    @allow_empty_body
    def logout(self):
        logger.debug('Logout successful')
        token = JwtManager.get_token_from_header()
        JwtManager.blocklist_token(token)
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
        }
