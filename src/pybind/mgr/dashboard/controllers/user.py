# -*- coding: utf-8 -*-
from __future__ import absolute_import

from . import ApiController, AuthRequired, RESTController
from .. import mgr
from .auth import Auth


@ApiController('user')
@AuthRequired()
class User(RESTController):
    # pylint: disable=unused-argument
    def get(self, username):
        config_username = mgr.get_config('username', None)

        return {
            'username': config_username
        }

    @RESTController.args_from_json
    def set(self, username, password=None):
        mgr.set_config('username', username)

        if password:
            hashed_passwd = Auth.password_hash(password)
            mgr.set_config('password', hashed_passwd)

        return {
            'username': username
        }
