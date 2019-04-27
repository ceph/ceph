# -*- coding: utf-8 -*-
from __future__ import absolute_import

import cherrypy

from . import ApiController, RESTController, UiApiController
from .. import mgr
from ..exceptions import RoleDoesNotExist, DashboardException,\
    RoleIsAssociatedWithUser, RoleAlreadyExists
from ..security import Scope as SecurityScope, Permission
from ..services.access_control import SYSTEM_ROLES


@ApiController('/role', SecurityScope.USER)
class Role(RESTController):
    @staticmethod
    def _role_to_dict(role):
        role_dict = role.to_dict()
        role_dict['system'] = role_dict['name'] in SYSTEM_ROLES
        return role_dict

    @staticmethod
    def _validate_permissions(scopes_permissions):
        if scopes_permissions:
            for scope, permissions in scopes_permissions.items():
                if scope not in SecurityScope.all_scopes():
                    raise DashboardException(msg='Invalid scope',
                                             code='invalid_scope',
                                             component='role')
                if any(permission not in Permission.all_permissions()
                       for permission in permissions):
                    raise DashboardException(msg='Invalid permission',
                                             code='invalid_permission',
                                             component='role')

    @staticmethod
    def _set_permissions(role, scopes_permissions):
        role.reset_scope_permissions()
        if scopes_permissions:
            for scope, permissions in scopes_permissions.items():
                if permissions:
                    role.set_scope_permissions(scope, permissions)

    def list(self):
        roles = dict(mgr.ACCESS_CTRL_DB.roles)
        roles.update(SYSTEM_ROLES)
        roles = sorted(roles.values(), key=lambda role: role.name)
        return [Role._role_to_dict(r) for r in roles]

    def get(self, name):
        role = SYSTEM_ROLES.get(name)
        if not role:
            try:
                role = mgr.ACCESS_CTRL_DB.get_role(name)
            except RoleDoesNotExist:
                raise cherrypy.HTTPError(404)
        return Role._role_to_dict(role)

    def create(self, name=None, description=None, scopes_permissions=None):
        if not name:
            raise DashboardException(msg='Name is required',
                                     code='name_required',
                                     component='role')
        Role._validate_permissions(scopes_permissions)
        try:
            role = mgr.ACCESS_CTRL_DB.create_role(name, description)
        except RoleAlreadyExists:
            raise DashboardException(msg='Role already exists',
                                     code='role_already_exists',
                                     component='role')
        Role._set_permissions(role, scopes_permissions)
        mgr.ACCESS_CTRL_DB.save()
        return Role._role_to_dict(role)

    def set(self, name, description=None, scopes_permissions=None):
        try:
            role = mgr.ACCESS_CTRL_DB.get_role(name)
        except RoleDoesNotExist:
            if name in SYSTEM_ROLES:
                raise DashboardException(msg='Cannot update system role',
                                         code='cannot_update_system_role',
                                         component='role')
            raise cherrypy.HTTPError(404)
        Role._validate_permissions(scopes_permissions)
        Role._set_permissions(role, scopes_permissions)
        role.description = description
        mgr.ACCESS_CTRL_DB.update_users_with_roles(role)
        mgr.ACCESS_CTRL_DB.save()
        return Role._role_to_dict(role)

    def delete(self, name):
        try:
            mgr.ACCESS_CTRL_DB.delete_role(name)
        except RoleDoesNotExist:
            if name in SYSTEM_ROLES:
                raise DashboardException(msg='Cannot delete system role',
                                         code='cannot_delete_system_role',
                                         component='role')
            raise cherrypy.HTTPError(404)
        except RoleIsAssociatedWithUser:
            raise DashboardException(msg='Role is associated with user',
                                     code='role_is_associated_with_user',
                                     component='role')
        mgr.ACCESS_CTRL_DB.save()


@UiApiController('/scope', SecurityScope.USER)
class Scope(RESTController):
    def list(self):
        return SecurityScope.all_scopes()
