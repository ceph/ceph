# -*- coding: utf-8 -*-
from __future__ import absolute_import

import cherrypy
import jwt

from . import ApiController, RESTController
from .. import logger, mgr
from ..exceptions import DashboardException
from ..services.auth import AuthManager, JwtManager
from ..services.access_control import UserDoesNotExist


@ApiController('/auth', secure=False)
class Auth(RESTController):
    """
    Provide authenticates and returns JWT token.
    """

    def create(self, username, password):
        user_perms = AuthManager.authenticate(username, password)
        if user_perms is not None:
            logger.debug('Login successful')
            token = JwtManager.gen_token(username)
            token = token.decode('utf-8')
            logger.debug("JWT Token: %s", token)
            cherrypy.response.headers['Authorization'] = "Bearer: {}".format(token)
            return {
                'token': token,
                'username': username,
                'permissions': user_perms
            }

        logger.debug('Login failed')
        raise DashboardException(msg='Invalid credentials',
                                 code='invalid_credentials',
                                 component='auth')

    @RESTController.Collection('POST')
    def logout(self):
        logger.debug('Logout successful')
        token = JwtManager.get_token_from_header()
        JwtManager.blacklist_token(token)
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

    @RESTController.Collection('POST')
    def check(self, token):
        if token:
            try:
                token = JwtManager.decode_token(token)
                if not JwtManager.is_blacklisted(token['jti']):
                    user = AuthManager.get_user(token['username'])
                    if user.lastUpdate <= token['iat']:
                        return {
                            'username': user.username,
                            'permissions': user.permissions_dict(),
                        }

                    logger.debug("AMT: user info changed after token was"
                                 " issued, iat=%s lastUpdate=%s",
                                 token['iat'], user.lastUpdate)
                else:
                    logger.debug('AMT: Token is black-listed')
            except jwt.exceptions.ExpiredSignatureError:
                logger.debug("AMT: Token has expired")
            except jwt.exceptions.InvalidTokenError:
                logger.debug("AMT: Failed to decode token")
            except UserDoesNotExist:
                logger.debug("AMT: Invalid token: user %s does not exist",
                             token['username'])
        return {
            'login_url': self._get_login_url()
        }
