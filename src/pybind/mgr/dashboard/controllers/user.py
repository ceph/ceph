# -*- coding: utf-8 -*-
from __future__ import absolute_import

import cherrypy

from . import BaseController, ApiController, RESTController, Endpoint
from .. import mgr
from ..exceptions import DashboardException, UserAlreadyExists, \
    UserDoesNotExist
from ..security import Scope
from ..services.access_control import SYSTEM_ROLES, PasswordCheck
from ..services.auth import JwtManager


def check_password_complexity(password, username, old_password=None):
    password_complexity = PasswordCheck(password, username, old_password)
    if password_complexity.check_if_as_the_old_password():
        raise DashboardException(msg='Password cannot be the\
                                      same as the previous one.',
                                 code='not-strong-enough-password',
                                 component='user')
    if password_complexity.check_if_contains_username():
        raise DashboardException(msg='Password cannot contain username.',
                                 code='not-strong-enough-password',
                                 component='user')
    if password_complexity.check_if_contains_forbidden_words():
        raise DashboardException(msg='Password cannot contain keywords.',
                                 code='not-strong-enough-password',
                                 component='user')
    if password_complexity.check_if_repetetive_characters():
        raise DashboardException(msg='Password cannot contain repetitive\
                                      characters.',
                                 code='not-strong-enough-password',
                                 component='user')
    if password_complexity.check_if_sequential_characters():
        raise DashboardException(msg='Password cannot contain sequential\
                                      characters.',
                                 code='not-strong-enough-password',
                                 component='user')
    if password_complexity.check_password_characters() < 10:
        raise DashboardException(msg='Password is too weak.',
                                 code='not-strong-enough-password',
                                 component='user')


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
        if password:
            check_password_complexity(password, username)
        try:
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


@ApiController('/user/{username}')
class UserChangePassword(BaseController):
    @Endpoint('POST')
    def change_password(self, username, old_password, new_password):
        session_username = JwtManager.get_username()
        if username != session_username:
            raise DashboardException(msg='Invalid user context',
                                     code='invalid_user_context',
                                     component='user')
        try:
            user = mgr.ACCESS_CTRL_DB.get_user(session_username)
        except UserDoesNotExist:
            raise cherrypy.HTTPError(404)
        if not user.compare_password(old_password):
            raise DashboardException(msg='Invalid old password',
                                     code='invalid_old_password',
                                     component='user')
        check_password_complexity(new_password, username, old_password)
        user.set_password(new_password)
        mgr.ACCESS_CTRL_DB.save()
