# -*- coding: utf-8 -*-
from __future__ import absolute_import

try:
    import Cookie
except ImportError:
    import http.cookies as Cookie
import sys
import jwt

from . import ApiController, RESTController, \
    allow_empty_body, set_cookies
from .. import logger, mgr
from ..exceptions import DashboardException
from ..services.auth import AuthManager, JwtManager
from ..services.access_control import UserDoesNotExist
# Python 3.8 introduced `samesite` attribute:
# https://docs.python.org/3/library/http.cookies.html#morsel-objects
if sys.version_info < (3, 8):
    Cookie.Morsel._reserved["samesite"] = "SameSite"  # type: ignore  # pylint: disable=W0212


@ApiController('/auth', secure=False)
class Auth(RESTController):
    """
    Provide authenticates and returns JWT token.
    """

    def create(self, username, password):
        user_perms = AuthManager.authenticate(username, password)
        if user_perms is not None:
            url_prefix = 'https' if mgr.get_localized_module_option('ssl') else 'http'
            logger.debug('Login successful')
            token = JwtManager.gen_token(username)

            # For backward-compatibility: PyJWT versions < 2.0.0 return bytes.
            token = token.decode('utf-8') if isinstance(token, bytes) else token

            set_cookies(url_prefix, token)
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
    @allow_empty_body
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
