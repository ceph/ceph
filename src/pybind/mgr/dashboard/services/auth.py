# -*- coding: utf-8 -*-

import abc
import base64
import hashlib
import hmac
import json
import logging
import os
import threading
import time
import uuid
from typing import Optional, Type

import cherrypy

from .. import mgr
from ..exceptions import ExpiredSignatureError, InvalidAlgorithmError, InvalidTokenError
from .access_control import LocalAuthenticator, UserDoesNotExist

cherrypy.config.update({
    'response.headers.server': 'Ceph-Dashboard',
    'response.headers.content-security-policy': "frame-ancestors 'self';",
    'response.headers.x-content-type-options': 'nosniff',
    'response.headers.strict-transport-security': 'max-age=63072000; includeSubDomains; preload'
})


class JwtManager(object):
    JWT_TOKEN_BLOCKLIST_KEY = "jwt_token_block_list"
    JWT_TOKEN_TTL = 28800  # default 8 hours
    JWT_ALGORITHM = 'HS256'
    _secret = None

    LOCAL_USER = threading.local()

    @staticmethod
    def _gen_secret():
        secret = os.urandom(16)
        return base64.b64encode(secret).decode('utf-8')

    @classmethod
    def init(cls):
        cls.logger = logging.getLogger('jwt')  # type: ignore
        # generate a new secret if it does not exist
        secret = mgr.get_store('jwt_secret')
        if secret is None:
            secret = cls._gen_secret()
            mgr.set_store('jwt_secret', secret)
        cls._secret = secret

    @classmethod
    def array_to_base64_string(cls, message):
        jsonstr = json.dumps(message, sort_keys=True).replace(" ", "")
        string_bytes = base64.urlsafe_b64encode(bytes(jsonstr, 'UTF-8'))
        return string_bytes.decode('UTF-8').replace("=", "")

    @classmethod
    def encode(cls, message, secret):
        header = {"alg": cls.JWT_ALGORITHM, "typ": "JWT"}
        base64_header = cls.array_to_base64_string(header)
        base64_message = cls.array_to_base64_string(message)
        base64_secret = base64.urlsafe_b64encode(hmac.new(
            bytes(secret, 'UTF-8'),
            msg=bytes(base64_header + "." + base64_message, 'UTF-8'),
            digestmod=hashlib.sha256
        ).digest()).decode('UTF-8').replace("=", "")
        return base64_header + "." + base64_message + "." + base64_secret

    @classmethod
    def decode(cls, message, secret):
        split_message = message.split(".")
        base64_header = split_message[0]
        base64_message = split_message[1]
        base64_secret = split_message[2]

        decoded_header = json.loads(base64.urlsafe_b64decode(base64_header))

        if decoded_header['alg'] != cls.JWT_ALGORITHM:
            raise InvalidAlgorithmError()

        incoming_secret = base64.urlsafe_b64encode(hmac.new(
            bytes(secret, 'UTF-8'),
            msg=bytes(base64_header + "." + base64_message, 'UTF-8'),
            digestmod=hashlib.sha256
        ).digest()).decode('UTF-8').replace("=", "")

        if base64_secret != incoming_secret:
            raise InvalidTokenError()

        # We add ==== as padding to ignore the requirement to have correct padding in
        # the urlsafe_b64decode method.
        decoded_message = json.loads(base64.urlsafe_b64decode(base64_message + "===="))
        now = int(time.time())
        if decoded_message['exp'] < now:
            raise ExpiredSignatureError()

        return decoded_message

    @classmethod
    def gen_token(cls, username, ttl: Optional[int] = None):
        if not cls._secret:
            cls.init()
        if ttl is None:
            ttl = mgr.get_module_option('jwt_token_ttl', cls.JWT_TOKEN_TTL)
        else:
            ttl = int(ttl) * 60 * 60  # convert hours to seconds
        now = int(time.time())
        payload = {
            'iss': 'ceph-dashboard',
            'jti': str(uuid.uuid4()),
            'exp': now + ttl,
            'iat': now,
            'username': username
        }
        return cls.encode(payload, cls._secret)  # type: ignore

    @classmethod
    def decode_token(cls, token):
        if not cls._secret:
            cls.init()
        return cls.decode(token, cls._secret)  # type: ignore

    @classmethod
    def get_token_from_header(cls):
        auth_cookie_name = 'token'
        try:
            # use cookie
            return cherrypy.request.cookie[auth_cookie_name].value
        except KeyError:
            try:
                # fall-back: use Authorization header
                auth_header = cherrypy.request.headers.get('authorization')
                if auth_header is not None:
                    scheme, params = auth_header.split(' ', 1)
                    if scheme.lower() == 'bearer':
                        return params
            except IndexError:
                return None

    @classmethod
    def set_user(cls, username):
        cls.LOCAL_USER.username = username

    @classmethod
    def reset_user(cls):
        cls.set_user(None)

    @classmethod
    def get_username(cls):
        return getattr(cls.LOCAL_USER, 'username', None)

    @classmethod
    def get_user(cls, token):
        try:
            dtoken = cls.decode_token(token)
            if not cls.is_blocklisted(dtoken['jti']):
                user = AuthManager.get_user(dtoken['username'])
                if user.last_update <= dtoken['iat']:
                    return user
                cls.logger.debug(  # type: ignore
                    "user info changed after token was issued, iat=%s last_update=%s",
                    dtoken['iat'], user.last_update
                )
            else:
                cls.logger.debug('Token is block-listed')  # type: ignore
        except ExpiredSignatureError:
            cls.logger.debug("Token has expired")  # type: ignore
        except InvalidTokenError:
            cls.logger.debug("Failed to decode token")  # type: ignore
        except InvalidAlgorithmError:
            cls.logger.debug("Only the HS256 algorithm is supported.")  # type: ignore
        except UserDoesNotExist:
            cls.logger.debug(  # type: ignore
                "Invalid token: user %s does not exist", dtoken['username']
            )
        return None

    @classmethod
    def blocklist_token(cls, token):
        token = cls.decode_token(token)
        blocklist_json = mgr.get_store(cls.JWT_TOKEN_BLOCKLIST_KEY)
        if not blocklist_json:
            blocklist_json = "{}"
        bl_dict = json.loads(blocklist_json)
        now = time.time()

        # remove expired tokens
        to_delete = []
        for jti, exp in bl_dict.items():
            if exp < now:
                to_delete.append(jti)
        for jti in to_delete:
            del bl_dict[jti]

        bl_dict[token['jti']] = token['exp']
        mgr.set_store(cls.JWT_TOKEN_BLOCKLIST_KEY, json.dumps(bl_dict))

    @classmethod
    def is_blocklisted(cls, jti):
        blocklist_json = mgr.get_store(cls.JWT_TOKEN_BLOCKLIST_KEY)
        if not blocklist_json:
            blocklist_json = "{}"
        bl_dict = json.loads(blocklist_json)
        return jti in bl_dict


class AuthManager(object):
    AUTH_PROVIDER = None

    @classmethod
    def initialize(cls):
        cls.AUTH_PROVIDER = LocalAuthenticator()

    @classmethod
    def get_user(cls, username):
        return cls.AUTH_PROVIDER.get_user(username)  # type: ignore

    @classmethod
    def authenticate(cls, username, password):
        return cls.AUTH_PROVIDER.authenticate(username, password)  # type: ignore

    @classmethod
    def authorize(cls, username, scope, permissions):
        return cls.AUTH_PROVIDER.authorize(username, scope, permissions)  # type: ignore


class BaseAuth(abc.ABC):
    @abc.abstractmethod
    def __init__(self, logger: logging.Logger) -> None:
        pass

    @abc.abstractmethod
    def authenticate(self) -> Optional[str]:
        pass

    @abc.abstractmethod
    def authorize(self, username: str, scope, permissions):
        pass


class Local(BaseAuth):
    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger

    def authenticate(self) -> Optional[str]:
        JwtManager.reset_user()
        token = JwtManager.get_token_from_header()
        if token:
            user = JwtManager.get_user(token)
            if user:
                return user.username
        return None

        """"
        resp_head = cherrypy.response.headers
        req_head = cherrypy.request.headers
        req_header_cross_origin_url = req_head.get('Access-Control-Allow-Origin')
        cross_origin_urls = mgr.get_module_option('cross_origin_url', '')
        cross_origin_url_list = [url.strip() for url in cross_origin_urls.split(',')]

        if req_header_cross_origin_url in cross_origin_url_list:
            resp_head['Access-Control-Allow-Origin'] = req_header_cross_origin_url

        self.logger.debug('Unauthorized access to %s',
                          cherrypy.url(relative='server'))
        raise cherrypy.HTTPError(401, 'You are not authorized to access '
                                      'that resource')
        """

    def authorize(self, username, scope, perms) -> Optional[bool]:
        JwtManager.set_user(username)
        return AuthManager.authorize(username, scope, perms)


class AuthManagerTool(cherrypy.Tool):
    def __init__(self, backend: str):
        self.logger = logging.getLogger('auth')
        backend_cls = self.get_auth_backend(backend.lower())
        self.logger.debug("Using auth backend: '%s'", backend_cls.__name__.lower())
        self.auth = backend_cls(logger=self.logger)
        super().__init__('before_handler', self.authenticate, priority=20)

    @staticmethod
    def get_auth_backend(backend_name: str) -> Type[BaseAuth]:
        for subclass in BaseAuth.__subclasses__():
            if subclass.__name__.lower() == backend_name:
                return subclass
        else:
            raise ValueError(f"Unknown auth backend: '{backend_name}'")

    def authenticate(self):
        self.logger.debug("checking authentication for request '%s'...", cherrypy.request.unique_id)
        username = self.auth.authenticate()
        self.logger.debug("request authenticated from user '%s'", username)
        if username:
            cherrypy.request.username = username
            self.authorize(username)
        else:
            self.logger.debug("Invalid authentication credentials")
            raise cherrypy.HTTPError(401, "Invalid authentication credentials")

    def authorize(self, username):
        method = cherrypy.request.method
        url = cherrypy.request.path_info
        self.logger.debug("checking authorization for %s '%s' by '%s'...", method, url, username)

        handler = cherrypy.request.handler.callable
        controller = handler.__self__
        scope = getattr(controller, '._security_scope', None)
        if scope is None:
            # Unrestricted access
            return

        try:
            perms = handler._security_permissions
        except AttributeError:
            self.logger.debug("Fail to check permission on: %s:%s", controller, handler)
            raise cherrypy.HTTPError(403, "You are not allowed to access this resource")

        self.logger.debug("checking '%s' access to '%s' scope", perms, scope)
        if self.auth.authorize(username=username, scope=scope, perms=perms):
            self.logger.debug("user '%s' authorized to %s '%s'", username, method, url)
            return
        else:
            self.logger.debug("User '%s' cannot %s '%s'", username, method, url)
            raise cherrypy.HTTPError(403, "You are not allowed to access this resource")
