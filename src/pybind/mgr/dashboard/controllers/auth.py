# -*- coding: utf-8 -*-
from __future__ import absolute_import

import time
import sys

import bcrypt
import cherrypy

from ..tools import ApiController, RESTController, Session
from .. import logger, mgr


@ApiController('auth')
class Auth(RESTController):
    """
    Provide login and logout actions.

    Supported config-keys:

      | KEY             | DEFAULT | DESCR                                     |
      ------------------------------------------------------------------------|
      | username        | None    | Username                                  |
      | password        | None    | Password encrypted using bcrypt           |
      | session-expire  | 1200    | Session will expire after <expires>       |
      |                           | seconds without activity                  |
    """

    @RESTController.args_from_json
    def create(self, username, password, stay_signed_in=False):
        now = time.time()
        config_username = mgr.get_config('username', None)
        config_password = mgr.get_config('password', None)
        hash_password = Auth.password_hash(password,
                                           config_password)
        if username == config_username and hash_password == config_password:
            cherrypy.session.regenerate()
            cherrypy.session[Session.USERNAME] = username
            cherrypy.session[Session.TS] = now
            cherrypy.session[Session.EXPIRE_AT_BROWSER_CLOSE] = not stay_signed_in
            logger.debug('Login successful')
            return {'username': username}

        cherrypy.response.status = 403
        if config_username is None:
            logger.warning('No Credentials configured. Need to call `ceph dashboard '
                           'set-login-credentials <username> <password>` first.')
        else:
            logger.debug('Login failed')
        return {'detail': 'Invalid credentials'}

    def bulk_delete(self):
        logger.debug('Logout successful')
        cherrypy.session[Session.USERNAME] = None
        cherrypy.session[Session.TS] = None

    @staticmethod
    def password_hash(password, salt_password=None):
        if not salt_password:
            salt_password = bcrypt.gensalt()
        if sys.version_info > (3, 0):
            return bcrypt.hashpw(password, salt_password)
        return bcrypt.hashpw(password.encode('utf8'), salt_password)

    @staticmethod
    def check_auth():
        username = cherrypy.session.get(Session.USERNAME)
        if not username:
            logger.debug('Unauthorized access to %s',
                         cherrypy.url(relative='server'))
            raise cherrypy.HTTPError(401, 'You are not authorized to access '
                                          'that resource')
        now = time.time()
        expires = float(mgr.get_config(
            'session-expire', Session.DEFAULT_EXPIRE))
        if expires > 0:
            username_ts = cherrypy.session.get(Session.TS, None)
            if username_ts and float(username_ts) < (now - expires):
                cherrypy.session[Session.USERNAME] = None
                cherrypy.session[Session.TS] = None
                logger.debug('Session expired')
                raise cherrypy.HTTPError(401,
                                         'Session expired. You are not '
                                         'authorized to access that resource')
        cherrypy.session[Session.TS] = now

    @staticmethod
    def set_login_credentials(username, password):
        mgr.set_config('username', username)
        hashed_passwd = Auth.password_hash(password)
        mgr.set_config('password', hashed_passwd)
