# -*- coding: utf-8 -*-
from __future__ import absolute_import

import cherrypy

from . import ApiController, RESTController
from .. import mgr
from ..exceptions import DashboardException, UserAlreadyExists, \
    UserDoesNotExist
from ..security import Scope
from ..services.access_control import SYSTEM_ROLES
from ..services.auth import JwtManager
import re


# minimum password complexity rules
def check_password_complexity(password, username):
    """
    Suggested rules for password complexity
    - at least 6 chars in length
    - must not be the same as the user account name
    - consist of characters from the following groups
    - alphabetic a-z, A-Z
    - numbers 0-9
    - special chars: !_@
    - must use at least 1 special char
    """
    if password == username:
        raise DashboardException(msg='Password is the same as the username.\
                                      It has to be different',
                                 code='password-the-same-as-username',
                                 component='Update/Create user')

    password_min_complex = re.compile('^(?=\\S{6,20}$)(?=.*[!_@])(?=.*[a-z])'
                                      '(?=.*[A-Z])(?=.*[0-9])')

    if not password_min_complex.match(password):
        raise DashboardException(msg='Password is not strong enough. <br/>\
                                      It has to contains at least 6 chars. <br/>\
                                      It must use at least 1 special char (!_@). <br/>\
                                      It has to consist of alphabetic\
                                       (a-z and A-Z) and numeric (0-9) chars.',
                                 code='not-strong-enough-password',
                                 component='Update/Create user')

                                 
@ApiController('/user', Scope.USER)
class User(RESTController):
    @staticmethod
    def _user_to_dict(user):
        result = user.to_dict()
        del result['password']
        return result

    @staticmethod
    def _get_user_roles(roles):
        all_roles = dict(mgr.ACCESS_CTRL_DB.roles)
        all_roles.update(SYSTEM_ROLES)
        try:
            return [all_roles[rolename] for rolename in roles]
        except KeyError:
            raise DashboardException(msg='Role does not exist',
                                     code='role_does_not_exist',
                                     component='user')

    def list(self):
        users = mgr.ACCESS_CTRL_DB.users
        result = [User._user_to_dict(u) for _, u in users.items()]
        return result

    def get(self, username):
        try:
            user = mgr.ACCESS_CTRL_DB.get_user(username)
        except UserDoesNotExist:
            raise cherrypy.HTTPError(404)
        return User._user_to_dict(user)

    def create(self, username=None, password=None, name=None, email=None, roles=None):
        if not username:
            raise DashboardException(msg='Username is required',
                                     code='username_required',
                                     component='user')
        user_roles = None
        if roles:
            user_roles = User._get_user_roles(roles)
        try:
            if password:
                check_password_complexity(password, username)
            user = mgr.ACCESS_CTRL_DB.create_user(username, password, name, email)
        except UserAlreadyExists:
            raise DashboardException(msg='Username already exists',
                                     code='username_already_exists',
                                     component='user')
        if user_roles:
            user.set_roles(user_roles)
        mgr.ACCESS_CTRL_DB.save()
        return User._user_to_dict(user)

    def delete(self, username):
        session_username = JwtManager.get_username()
        if session_username == username:
            raise DashboardException(msg='Cannot delete current user',
                                     code='cannot_delete_current_user',
                                     component='user')
        try:
            mgr.ACCESS_CTRL_DB.delete_user(username)
        except UserDoesNotExist:
            raise cherrypy.HTTPError(404)
        mgr.ACCESS_CTRL_DB.save()

    def set(self, username, password=None, name=None, email=None, roles=None):
        try:
            user = mgr.ACCESS_CTRL_DB.get_user(username)
        except UserDoesNotExist:
            raise cherrypy.HTTPError(404)
        if not password and not name and not email and (user.to_dict())['roles'] == roles:
            raise DashboardException(msg='All fields are empty, cannot update the user',
                                     code='cannot_update_user',
                                     component='user')
        user_roles = []
        if roles:
            user_roles = User._get_user_roles(roles)
        if password:
            check_password_complexity(password, username)
            user.set_password(password)
        user.name = name
        user.email = email
        user.set_roles(user_roles)
        mgr.ACCESS_CTRL_DB.save()
        return User._user_to_dict(user)
