# -*- coding: utf-8 -*-
from __future__ import absolute_import

from base64 import b64encode
import json
import os
import threading
import time
import uuid

import cherrypy
import jwt

from .access_control import LocalAuthenticator, UserDoesNotExist
from .. import mgr, logger


class JwtManager(object):
    JWT_TOKEN_BLACKLIST_KEY = "jwt_token_black_list"
    JWT_TOKEN_TTL = 28800  # default 8 hours
    JWT_ALGORITHM = 'HS256'
    _secret = None

    LOCAL_USER = threading.local()

    @staticmethod
    def _gen_secret():
        secret = os.urandom(16)
        return b64encode(secret).decode('utf-8')

    @classmethod
    def init(cls):
        # generate a new secret if it does not exist
        secret = mgr.get_store('jwt_secret')
        if secret is None:
            secret = cls._gen_secret()
            mgr.set_store('jwt_secret', secret)
        cls._secret = secret

    @classmethod
    def gen_token(cls, username):
        if not cls._secret:
            cls.init()
        ttl = mgr.get_module_option('jwt_token_ttl', cls.JWT_TOKEN_TTL)
        ttl = int(ttl)
        now = int(time.time())
        payload = {
            'iss': 'ceph-dashboard',
            'jti': str(uuid.uuid4()),
            'exp': now + ttl,
            'iat': now,
            'username': username
        }
        return jwt.encode(payload, cls._secret, algorithm=cls.JWT_ALGORITHM)

    @classmethod
    def decode_token(cls, token):
        if not cls._secret:
            cls.init()
        return jwt.decode(token, cls._secret, algorithms=cls.JWT_ALGORITHM)

    @classmethod
    def get_token_from_header(cls):
        auth_header = cherrypy.request.headers.get('authorization')
        if auth_header is not None:
            scheme, params = auth_header.split(' ', 1)
            if scheme.lower() == 'bearer':
                return params
        return None

    @classmethod
    def set_user(cls, token):
        cls.LOCAL_USER.username = token['username']

    @classmethod
    def reset_user(cls):
        cls.set_user({'username': None, 'permissions': None})

    @classmethod
    def get_username(cls):
        return getattr(cls.LOCAL_USER, 'username', None)

    @classmethod
    def blacklist_token(cls, token):
        token = jwt.decode(token, verify=False)
        blacklist_json = mgr.get_store(cls.JWT_TOKEN_BLACKLIST_KEY)
        if not blacklist_json:
            blacklist_json = "{}"
        bl_dict = json.loads(blacklist_json)
        now = time.time()

        # remove expired tokens
        to_delete = []
        for jti, exp in bl_dict.items():
            if exp < now:
                to_delete.append(jti)
        for jti in to_delete:
            del bl_dict[jti]

        bl_dict[token['jti']] = token['exp']
        mgr.set_store(cls.JWT_TOKEN_BLACKLIST_KEY, json.dumps(bl_dict))

    @classmethod
    def is_blacklisted(cls, jti):
        blacklist_json = mgr.get_store(cls.JWT_TOKEN_BLACKLIST_KEY)
        if not blacklist_json:
            blacklist_json = "{}"
        bl_dict = json.loads(blacklist_json)
        return jti in bl_dict


class AuthManager(object):
    AUTH_PROVIDER = None

    @classmethod
    def initialize(cls):
        cls.AUTH_PROVIDER = LocalAuthenticator()

    @classmethod
    def get_user(cls, username):
        return cls.AUTH_PROVIDER.get_user(username)

    @classmethod
    def authenticate(cls, username, password):
        return cls.AUTH_PROVIDER.authenticate(username, password)

    @classmethod
    def authorize(cls, username, scope, permissions):
        return cls.AUTH_PROVIDER.authorize(username, scope, permissions)


class AuthManagerTool(cherrypy.Tool):
    def __init__(self):
        super(AuthManagerTool, self).__init__(
            'before_handler', self._check_authentication, priority=20)

    def _check_authentication(self):
        JwtManager.reset_user()
        token = JwtManager.get_token_from_header()
        logger.debug("AMT: token: %s", token)
        if token:
            try:
                token = JwtManager.decode_token(token)
                if not JwtManager.is_blacklisted(token['jti']):
                    user = AuthManager.get_user(token['username'])
                    if user.lastUpdate <= token['iat']:
                        self._check_authorization(token)
                        return

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

        logger.debug('AMT: Unauthorized access to %s',
                     cherrypy.url(relative='server'))
        raise cherrypy.HTTPError(401, 'You are not authorized to access '
                                      'that resource')

    def _check_authorization(self, token):
        logger.debug("AMT: checking authorization...")
        username = token['username']
        handler = cherrypy.request.handler.callable
        controller = handler.__self__
        sec_scope = getattr(controller, '_security_scope', None)
        sec_perms = getattr(handler, '_security_permissions', None)
        JwtManager.set_user(token)

        if not sec_scope:
            # controller does not define any authorization restrictions
            return

        logger.debug("AMT: checking '%s' access to '%s' scope", sec_perms,
                     sec_scope)

        if not sec_perms:
            logger.debug("Fail to check permission on: %s:%s", controller,
                         handler)
            raise cherrypy.HTTPError(403, "You don't have permissions to "
                                          "access that resource")

        if not AuthManager.authorize(username, sec_scope, sec_perms):
            raise cherrypy.HTTPError(403, "You don't have permissions to "
                                          "access that resource")
