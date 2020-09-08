# -*- coding: utf-8 -*-
from __future__ import absolute_import

from datetime import datetime

import time

import cherrypy

from . import BaseController, ApiController, RESTController, Endpoint, \
    allow_empty_body, ControllerDoc, EndpointDoc
from .. import mgr
from ..exceptions import DashboardException, UserAlreadyExists, \
    UserDoesNotExist, PasswordPolicyException, PwdExpirationDateNotValid
from ..security import Scope
from ..services.access_control import SYSTEM_ROLES, PasswordPolicy
from ..services.auth import JwtManager

USER_SCHEMA = ([{
    "username": (str, 'Username of the user'),
    "roles": ([str], 'User Roles'),
    "name": (str, 'User Name'),
    "email": (str, 'User email address'),
    "lastUpdate": (int, 'Details last updated'),
    "enabled": (bool, 'Is the user enabled?'),
    "pwdExpirationDate": (str, 'Password Expiration date'),
    "pwdUpdateRequired": (bool, 'Is Password Update Required?')
}], '')


def validate_password_policy(password, username=None, old_password=None):
    """
    :param password: The password to validate.
    :param username: The name of the user (optional).
    :param old_password: The old password (optional).
    :return: Returns the password complexity credits.
    :rtype: int
    :raises DashboardException: If a password policy fails.
    """
    pw_policy = PasswordPolicy(password, username, old_password)
    try:
        pw_policy.check_all()
        return pw_policy.complexity_credits
    except PasswordPolicyException as ex:
        raise DashboardException(msg=str(ex),
                                 code='password_policy_validation_failed',
                                 component='user')


@ApiController('/user', Scope.USER)
@ControllerDoc("Display User Details", "User")
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
    @EndpointDoc("Get List Of Users",
                 responses={200: USER_SCHEMA})
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

    def create(self, username=None, password=None, name=None, email=None,
               roles=None, enabled=True, pwdExpirationDate=None, pwdUpdateRequired=True):
        if not username:
            raise DashboardException(msg='Username is required',
                                     code='username_required',
                                     component='user')
        user_roles = None
        if roles:
            user_roles = User._get_user_roles(roles)
        if password:
            validate_password_policy(password, username)
        try:
            user = mgr.ACCESS_CTRL_DB.create_user(username, password, name,
                                                  email, enabled, pwdExpirationDate,
                                                  pwdUpdateRequired)
        except UserAlreadyExists:
            raise DashboardException(msg='Username already exists',
                                     code='username_already_exists',
                                     component='user')
        except PwdExpirationDateNotValid:
            raise DashboardException(msg='Password expiration date must not be in '
                                         'the past',
                                     code='pwd_past_expiration_date',
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

    def set(self, username, password=None, name=None, email=None, roles=None,
            enabled=None, pwdExpirationDate=None, pwdUpdateRequired=False):
        if JwtManager.get_username() == username and enabled is False:
            raise DashboardException(msg='You are not allowed to disable your user',
                                     code='cannot_disable_current_user',
                                     component='user')

        try:
            user = mgr.ACCESS_CTRL_DB.get_user(username)
        except UserDoesNotExist:
            raise cherrypy.HTTPError(404)
        user_roles = []
        if roles:
            user_roles = User._get_user_roles(roles)
        if password:
            validate_password_policy(password, username)
            user.set_password(password)
        if pwdExpirationDate and \
           (pwdExpirationDate < int(time.mktime(datetime.utcnow().timetuple()))):
            raise DashboardException(
                msg='Password expiration date must not be in the past',
                code='pwd_past_expiration_date', component='user')
        user.name = name
        user.email = email
        if enabled is not None:
            user.enabled = enabled
        user.pwd_expiration_date = pwdExpirationDate
        user.set_roles(user_roles)
        user.pwd_update_required = pwdUpdateRequired
        mgr.ACCESS_CTRL_DB.save()
        return User._user_to_dict(user)


@ApiController('/user')
@ControllerDoc("Get User Password Policy Details", "UserPasswordPolicy")
class UserPasswordPolicy(RESTController):

    @Endpoint('POST')
    @allow_empty_body
    def validate_password(self, password, username=None, old_password=None):
        """
        Check if the password meets the password policy.
        :param password: The password to validate.
        :param username: The name of the user (optional).
        :param old_password: The old password (optional).
        :return: An object with properties valid, credits and valuation.
        'credits' contains the password complexity credits and
        'valuation' the textual summary of the validation.
        """
        result = {'valid': False, 'credits': 0, 'valuation': None}
        try:
            result['credits'] = validate_password_policy(password, username, old_password)
            if result['credits'] < 15:
                result['valuation'] = 'Weak'
            elif result['credits'] < 20:
                result['valuation'] = 'OK'
            elif result['credits'] < 25:
                result['valuation'] = 'Strong'
            else:
                result['valuation'] = 'Very strong'
            result['valid'] = True
        except DashboardException as ex:
            result['valuation'] = str(ex)
        return result


@ApiController('/user/{username}')
@ControllerDoc("Change User Password", "UserChangePassword")
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
        validate_password_policy(new_password, username, old_password)
        user.set_password(new_password)
        mgr.ACCESS_CTRL_DB.save()
