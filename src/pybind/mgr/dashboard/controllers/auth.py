# -*- coding: utf-8 -*-
from __future__ import absolute_import

import cherrypy

from . import ApiController, RESTController
from .. import logger
from ..exceptions import DashboardException
from ..services.auth import AuthManager, JwtManager


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

    def bulk_delete(self):
        token = JwtManager.get_token_from_header()
        JwtManager.blacklist_token(token)
