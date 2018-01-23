# -*- coding: utf-8 -*-

import bcrypt
import cherrypy
import time
from cherrypy import tools

class Auth(object):
    """
    Provide login and logout actions.

    Supported config-keys:

      | KEY             | DEFAULT | DESCR                                                        |
      --------------------------------------------------------------------------------------------
      | username        | None    | Username                                                     |
      | password        | None    | Password encrypted using bcrypt                              |
      | session-expire  | 1200    | Session will expire after <expires> seconds without activity |
    """

    SESSION_KEY = '_username'
    SESSION_KEY_TS = '_username_ts'

    DEFAULT_SESSION_EXPIRE = 1200

    def __init__(self, module):
        self.module = module
        self.log = self.module.log

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['POST'])
    @tools.json_out()
    def login(self, username=None, password=None):
        now = int(time.time())
        config_username = self.module.get_localized_config('username', None)
        config_password = self.module.get_localized_config('password', None)
        hash_password = bcrypt.hashpw(password.encode('utf8'), config_password)
        if username == config_username and hash_password == config_password:
            cherrypy.session.regenerate()
            cherrypy.session[Auth.SESSION_KEY] = username
            cherrypy.session[Auth.SESSION_KEY_TS] = now
            self.log.debug("Login successful")
            return {'username': username}
        else:
            cherrypy.response.status = 403
            self.log.debug("Login fail")
            return {'detail': 'Invalid credentials'}

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['POST'])
    def logout(self):
        self.log.debug("Logout successful")
        cherrypy.session[Auth.SESSION_KEY] = None
        cherrypy.session[Auth.SESSION_KEY_TS] = None

    def check_auth(self):
        username = cherrypy.session.get(Auth.SESSION_KEY)
        if not username:
            self.log.debug("Unauthorized")
            raise cherrypy.HTTPError(401,
                                     'You are not authorized to access that resource')
        now = int(time.time())
        expires = int(self.module.get_localized_config('session-expire', Auth.DEFAULT_SESSION_EXPIRE))
        if expires > 0:
            username_ts = cherrypy.session.get(Auth.SESSION_KEY_TS, None)
            if username_ts and username_ts < now - expires:
                cherrypy.session[Auth.SESSION_KEY] = None
                cherrypy.session[Auth.SESSION_KEY_TS] = None
                self.log.debug("Session expired.")
                raise cherrypy.HTTPError(401,
                                         'Session expired. You are not authorized to access that resource')
        cherrypy.session[Auth.SESSION_KEY_TS] = now
