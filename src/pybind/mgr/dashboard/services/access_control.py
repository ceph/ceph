# -*- coding: utf-8 -*-
# pylint: disable=too-many-arguments,too-many-return-statements
# pylint: disable=too-many-branches, too-many-locals, too-many-statements
from __future__ import absolute_import

import errno
import json
import threading
import time

import bcrypt

from mgr_module import CLIReadCommand, CLIWriteCommand

from .. import mgr, logger
from ..security import Scope, Permission
from ..exceptions import RoleAlreadyExists, RoleDoesNotExist, ScopeNotValid, \
                         PermissionNotValid, RoleIsAssociatedWithUser, \
                         UserAlreadyExists, UserDoesNotExist, ScopeNotInRole, \
                         RoleNotInUser


# password hashing algorithm
def password_hash(password, salt_password=None):
    if not password:
        return None
    if not salt_password:
        salt_password = bcrypt.gensalt()
    else:
        salt_password = salt_password.encode('utf8')
    return bcrypt.hashpw(password.encode('utf8'), salt_password).decode('utf8')


_P = Permission  # short alias


class Role(object):
    def __init__(self, name, description=None, scope_permissions=None):
        self.name = name
        self.description = description
        if scope_permissions is None:
            self.scopes_permissions = {}
        else:
            self.scopes_permissions = scope_permissions

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return self.name == other.name

    def set_scope_permissions(self, scope, permissions):
        if not Scope.valid_scope(scope):
            raise ScopeNotValid(scope)
        for perm in permissions:
            if not Permission.valid_permission(perm):
                raise PermissionNotValid(perm)

        permissions.sort()
        self.scopes_permissions[scope] = permissions

    def del_scope_permissions(self, scope):
        if scope not in self.scopes_permissions:
            raise ScopeNotInRole(scope, self.name)
        del self.scopes_permissions[scope]

    def reset_scope_permissions(self):
        self.scopes_permissions = {}

    def authorize(self, scope, permissions):
        if scope in self.scopes_permissions:
            role_perms = self.scopes_permissions[scope]
            for perm in permissions:
                if perm not in role_perms:
                    return False
            return True
        return False

    def to_dict(self):
        return {
            'name': self.name,
            'description': self.description,
            'scopes_permissions': self.scopes_permissions
        }

    @classmethod
    def from_dict(cls, r_dict):
        return Role(r_dict['name'], r_dict['description'],
                    r_dict['scopes_permissions'])


# static pre-defined system roles
# this roles cannot be deleted nor updated

# admin role provides all permissions for all scopes
ADMIN_ROLE = Role('administrator', 'Administrator', {
    scope_name: Permission.all_permissions()
    for scope_name in Scope.all_scopes()
})


# read-only role provides read-only permission for all scopes
READ_ONLY_ROLE = Role('read-only', 'Read-Only', {
    scope_name: [_P.READ] for scope_name in Scope.all_scopes()
    if scope_name != Scope.DASHBOARD_SETTINGS
})


# block manager role provides all permission for block related scopes
BLOCK_MGR_ROLE = Role('block-manager', 'Block Manager', {
    Scope.RBD_IMAGE: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
    Scope.POOL: [_P.READ],
    Scope.ISCSI: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
    Scope.RBD_MIRRORING: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
})


# RadosGW manager role provides all permissions for block related scopes
RGW_MGR_ROLE = Role('rgw-manager', 'RGW Manager', {
    Scope.RGW: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
    Scope.CONFIG_OPT: [_P.READ],
})


# Cluster manager role provides all permission for OSDs, Monitors, and
# Config options
CLUSTER_MGR_ROLE = Role('cluster-manager', 'Cluster Manager', {
    Scope.HOSTS: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
    Scope.OSD: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
    Scope.MONITOR: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
    Scope.MANAGER: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
    Scope.CONFIG_OPT: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
    Scope.LOG: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
})


# Pool manager role provides all permissions for pool related scopes
POOL_MGR_ROLE = Role('pool-manager', 'Pool Manager', {
    Scope.POOL: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
    Scope.CONFIG_OPT: [_P.READ],
})

# Pool manager role provides all permissions for CephFS related scopes
CEPHFS_MGR_ROLE = Role('cephfs-manager', 'CephFS Manager', {
    Scope.CEPHFS: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
    Scope.CONFIG_OPT: [_P.READ],
})

GANESHA_MGR_ROLE = Role('ganesha-manager', 'NFS Ganesha Manager', {
    Scope.NFS_GANESHA: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
    Scope.CEPHFS: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
    Scope.RGW: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
    Scope.CONFIG_OPT: [_P.READ],
})


SYSTEM_ROLES = {
    ADMIN_ROLE.name: ADMIN_ROLE,
    READ_ONLY_ROLE.name: READ_ONLY_ROLE,
    BLOCK_MGR_ROLE.name: BLOCK_MGR_ROLE,
    RGW_MGR_ROLE.name: RGW_MGR_ROLE,
    CLUSTER_MGR_ROLE.name: CLUSTER_MGR_ROLE,
    POOL_MGR_ROLE.name: POOL_MGR_ROLE,
    CEPHFS_MGR_ROLE.name: CEPHFS_MGR_ROLE,
    GANESHA_MGR_ROLE.name: GANESHA_MGR_ROLE,
}


class User(object):
    def __init__(self, username, password, name=None, email=None, roles=None,
                 lastUpdate=None):
        self.username = username
        self.password = password
        self.name = name
        self.email = email
        if roles is None:
            self.roles = set()
        else:
            self.roles = roles
        if lastUpdate is None:
            self.refreshLastUpdate()
        else:
            self.lastUpdate = lastUpdate

    def refreshLastUpdate(self):
        self.lastUpdate = int(time.mktime(time.gmtime()))

    def set_password(self, password):
        self.password = password_hash(password)
        self.refreshLastUpdate()

    def set_roles(self, roles):
        self.roles = set(roles)
        self.refreshLastUpdate()

    def add_roles(self, roles):
        self.roles = self.roles.union(set(roles))
        self.refreshLastUpdate()

    def del_roles(self, roles):
        for role in roles:
            if role not in self.roles:
                raise RoleNotInUser(role.name, self.username)
        self.roles.difference_update(set(roles))
        self.refreshLastUpdate()

    def authorize(self, scope, permissions):
        for role in self.roles:
            if role.authorize(scope, permissions):
                return True
        return False

    def permissions_dict(self):
        perms = {}
        for role in self.roles:
            for scope, perms_list in role.scopes_permissions.items():
                if scope in perms:
                    perms_tmp = set(perms[scope]).union(set(perms_list))
                    perms[scope] = list(perms_tmp)
                else:
                    perms[scope] = perms_list

        return perms

    def to_dict(self):
        return {
            'username': self.username,
            'password': self.password,
            'roles': sorted([r.name for r in self.roles]),
            'name': self.name,
            'email': self.email,
            'lastUpdate': self.lastUpdate
        }

    @classmethod
    def from_dict(cls, u_dict, roles):
        return User(u_dict['username'], u_dict['password'], u_dict['name'],
                    u_dict['email'], {roles[r] for r in u_dict['roles']},
                    u_dict['lastUpdate'])


class AccessControlDB(object):
    VERSION = 1
    ACDB_CONFIG_KEY = "accessdb_v"

    def __init__(self, version, users, roles):
        self.users = users
        self.version = version
        self.roles = roles
        self.lock = threading.RLock()

    def create_role(self, name, description=None):
        with self.lock:
            if name in SYSTEM_ROLES or name in self.roles:
                raise RoleAlreadyExists(name)
            role = Role(name, description)
            self.roles[name] = role
            return role

    def get_role(self, name):
        with self.lock:
            if name not in self.roles:
                raise RoleDoesNotExist(name)
            return self.roles[name]

    def delete_role(self, name):
        with self.lock:
            if name not in self.roles:
                raise RoleDoesNotExist(name)
            role = self.roles[name]

            # check if role is not associated with a user
            for username, user in self.users.items():
                if role in user.roles:
                    raise RoleIsAssociatedWithUser(name, username)

            del self.roles[name]

    def create_user(self, username, password, name, email):
        logger.debug("AC: creating user: username=%s", username)
        with self.lock:
            if username in self.users:
                raise UserAlreadyExists(username)
            user = User(username, password_hash(password), name, email)
            self.users[username] = user
            return user

    def get_user(self, username):
        with self.lock:
            if username not in self.users:
                raise UserDoesNotExist(username)
            return self.users[username]

    def delete_user(self, username):
        with self.lock:
            if username not in self.users:
                raise UserDoesNotExist(username)
            del self.users[username]

    def update_users_with_roles(self, role):
        with self.lock:
            if not role:
                return
            for _, user in self.users.items():
                if role in user.roles:
                    user.refreshLastUpdate()

    def save(self):
        with self.lock:
            db = {
                'users': {un: u.to_dict() for un, u in self.users.items()},
                'roles': {rn: r.to_dict() for rn, r in self.roles.items()},
                'version': self.version
            }
            mgr.set_store(self.accessdb_config_key(), json.dumps(db))

    @classmethod
    def accessdb_config_key(cls, version=None):
        if version is None:
            version = cls.VERSION
        return "{}{}".format(cls.ACDB_CONFIG_KEY, version)

    def check_and_update_db(self):
        logger.debug("AC: Checking for previews DB versions")
        if self.VERSION == 1:  # current version
            # check if there is username/password from previous version
            username = mgr.get_module_option('username', None)
            password = mgr.get_module_option('password', None)
            if username and password:
                logger.debug("AC: Found single user credentials: user=%s",
                             username)
                # found user credentials
                user = self.create_user(username, "", None, None)
                # password is already hashed, so setting manually
                user.password = password
                user.add_roles([ADMIN_ROLE])
                self.save()
        else:
            raise NotImplementedError()

    @classmethod
    def load(cls):
        logger.info("AC: Loading user roles DB version=%s", cls.VERSION)

        json_db = mgr.get_store(cls.accessdb_config_key())
        if json_db is None:
            logger.debug("AC: No DB v%s found, creating new...", cls.VERSION)
            db = cls(cls.VERSION, {}, {})
            # check if we can update from a previous version database
            db.check_and_update_db()
            return db

        db = json.loads(json_db)
        roles = {rn: Role.from_dict(r)
                 for rn, r in db.get('roles', {}).items()}
        users = {un: User.from_dict(u, dict(roles, **SYSTEM_ROLES))
                 for un, u in db.get('users', {}).items()}
        return cls(db['version'], users, roles)


def load_access_control_db():
    mgr.ACCESS_CTRL_DB = AccessControlDB.load()


# CLI dashboard access control scope commands

@CLIWriteCommand('dashboard set-login-credentials',
                 'name=username,type=CephString '
                 'name=password,type=CephString',
                 'Set the login credentials')
def set_login_credentials_cmd(_, username, password):
    try:
        user = mgr.ACCESS_CTRL_DB.get_user(username)
        user.set_password(password)
    except UserDoesNotExist:
        user = mgr.ACCESS_CTRL_DB.create_user(username, password, None, None)
        user.set_roles([ADMIN_ROLE])

    mgr.ACCESS_CTRL_DB.save()

    return 0, '''\
******************************************************************
***          WARNING: this command is deprecated.              ***
*** Please use the ac-user-* related commands to manage users. ***
******************************************************************
Username and password updated''', ''


@CLIReadCommand('dashboard ac-role-show',
                'name=rolename,type=CephString,req=false',
                'Show role info')
def ac_role_show_cmd(_, rolename=None):
    if not rolename:
        roles = dict(mgr.ACCESS_CTRL_DB.roles)
        roles.update(SYSTEM_ROLES)
        roles_list = [name for name, _ in roles.items()]
        return 0, json.dumps(roles_list), ''
    try:
        role = mgr.ACCESS_CTRL_DB.get_role(rolename)
    except RoleDoesNotExist as ex:
        if rolename not in SYSTEM_ROLES:
            return -errno.ENOENT, '', str(ex)
        role = SYSTEM_ROLES[rolename]
    return 0, json.dumps(role.to_dict()), ''


@CLIWriteCommand('dashboard ac-role-create',
                 'name=rolename,type=CephString '
                 'name=description,type=CephString,req=false',
                 'Create a new access control role')
def ac_role_create_cmd(_, rolename, description=None):
    try:
        role = mgr.ACCESS_CTRL_DB.create_role(rolename, description)
        mgr.ACCESS_CTRL_DB.save()
        return 0, json.dumps(role.to_dict()), ''
    except RoleAlreadyExists as ex:
        return -errno.EEXIST, '', str(ex)


@CLIWriteCommand('dashboard ac-role-delete',
                 'name=rolename,type=CephString',
                 'Delete an access control role')
def ac_role_delete_cmd(_, rolename):
    try:
        mgr.ACCESS_CTRL_DB.delete_role(rolename)
        mgr.ACCESS_CTRL_DB.save()
        return 0, "Role '{}' deleted".format(rolename), ""
    except RoleDoesNotExist as ex:
        if rolename in SYSTEM_ROLES:
            return -errno.EPERM, '', "Cannot delete system role '{}'" \
                                        .format(rolename)
        return -errno.ENOENT, '', str(ex)
    except RoleIsAssociatedWithUser as ex:
        return -errno.EPERM, '', str(ex)


@CLIWriteCommand('dashboard ac-role-add-scope-perms',
                 'name=rolename,type=CephString '
                 'name=scopename,type=CephString '
                 'name=permissions,type=CephString,n=N',
                 'Add the scope permissions for a role')
def ac_role_add_scope_perms_cmd(_, rolename, scopename, permissions):
    try:
        role = mgr.ACCESS_CTRL_DB.get_role(rolename)
        perms_array = [perm.strip() for perm in permissions]
        role.set_scope_permissions(scopename, perms_array)
        mgr.ACCESS_CTRL_DB.update_users_with_roles(role)
        mgr.ACCESS_CTRL_DB.save()
        return 0, json.dumps(role.to_dict()), ''
    except RoleDoesNotExist as ex:
        if rolename in SYSTEM_ROLES:
            return -errno.EPERM, '', "Cannot update system role '{}'" \
                                        .format(rolename)
        return -errno.ENOENT, '', str(ex)
    except ScopeNotValid as ex:
        return -errno.EINVAL, '', str(ex) + "\n Possible values: {}" \
                                            .format(Scope.all_scopes())
    except PermissionNotValid as ex:
        return -errno.EINVAL, '', str(ex) + \
                                    "\n Possible values: {}" \
                                    .format(Permission.all_permissions())


@CLIWriteCommand('dashboard ac-role-del-scope-perms',
                 'name=rolename,type=CephString '
                 'name=scopename,type=CephString',
                 'Delete the scope permissions for a role')
def ac_role_del_scope_perms_cmd(_, rolename, scopename):
    try:
        role = mgr.ACCESS_CTRL_DB.get_role(rolename)
        role.del_scope_permissions(scopename)
        mgr.ACCESS_CTRL_DB.update_users_with_roles(role)
        mgr.ACCESS_CTRL_DB.save()
        return 0, json.dumps(role.to_dict()), ''
    except RoleDoesNotExist as ex:
        if rolename in SYSTEM_ROLES:
            return -errno.EPERM, '', "Cannot update system role '{}'" \
                                        .format(rolename)
        return -errno.ENOENT, '', str(ex)
    except ScopeNotInRole as ex:
        return -errno.ENOENT, '', str(ex)


@CLIReadCommand('dashboard ac-user-show',
                'name=username,type=CephString,req=false',
                'Show user info')
def ac_user_show_cmd(_, username=None):
    if not username:
        users = mgr.ACCESS_CTRL_DB.users
        users_list = [name for name, _ in users.items()]
        return 0, json.dumps(users_list), ''
    try:
        user = mgr.ACCESS_CTRL_DB.get_user(username)
        return 0, json.dumps(user.to_dict()), ''
    except UserDoesNotExist as ex:
        return -errno.ENOENT, '', str(ex)


@CLIWriteCommand('dashboard ac-user-create',
                 'name=username,type=CephString '
                 'name=password,type=CephString,req=false '
                 'name=rolename,type=CephString,req=false '
                 'name=name,type=CephString,req=false '
                 'name=email,type=CephString,req=false',
                 'Create a user')
def ac_user_create_cmd(_, username, password=None, rolename=None, name=None,
                       email=None):
    try:
        role = mgr.ACCESS_CTRL_DB.get_role(rolename) if rolename else None
    except RoleDoesNotExist as ex:
        if rolename not in SYSTEM_ROLES:
            return -errno.ENOENT, '', str(ex)
        role = SYSTEM_ROLES[rolename]

    try:
        user = mgr.ACCESS_CTRL_DB.create_user(username, password, name, email)
    except UserAlreadyExists as ex:
        return -errno.EEXIST, '', str(ex)

    if role:
        user.set_roles([role])
    mgr.ACCESS_CTRL_DB.save()
    return 0, json.dumps(user.to_dict()), ''


@CLIWriteCommand('dashboard ac-user-delete',
                 'name=username,type=CephString',
                 'Delete user')
def ac_user_delete_cmd(_, username):
    try:
        mgr.ACCESS_CTRL_DB.delete_user(username)
        mgr.ACCESS_CTRL_DB.save()
        return 0, "User '{}' deleted".format(username), ""
    except UserDoesNotExist as ex:
        return -errno.ENOENT, '', str(ex)


@CLIWriteCommand('dashboard ac-user-set-roles',
                 'name=username,type=CephString '
                 'name=roles,type=CephString,n=N',
                 'Set user roles')
def ac_user_set_roles_cmd(_, username, roles):
    rolesname = roles
    roles = []
    for rolename in rolesname:
        try:
            roles.append(mgr.ACCESS_CTRL_DB.get_role(rolename))
        except RoleDoesNotExist as ex:
            if rolename not in SYSTEM_ROLES:
                return -errno.ENOENT, '', str(ex)
            roles.append(SYSTEM_ROLES[rolename])
    try:
        user = mgr.ACCESS_CTRL_DB.get_user(username)
        user.set_roles(roles)
        mgr.ACCESS_CTRL_DB.save()
        return 0, json.dumps(user.to_dict()), ''
    except UserDoesNotExist as ex:
        return -errno.ENOENT, '', str(ex)


@CLIWriteCommand('dashboard ac-user-add-roles',
                 'name=username,type=CephString '
                 'name=roles,type=CephString,n=N',
                 'Add roles to user')
def ac_user_add_roles_cmd(_, username, roles):
    rolesname = roles
    roles = []
    for rolename in rolesname:
        try:
            roles.append(mgr.ACCESS_CTRL_DB.get_role(rolename))
        except RoleDoesNotExist as ex:
            if rolename not in SYSTEM_ROLES:
                return -errno.ENOENT, '', str(ex)
            roles.append(SYSTEM_ROLES[rolename])
    try:
        user = mgr.ACCESS_CTRL_DB.get_user(username)
        user.add_roles(roles)
        mgr.ACCESS_CTRL_DB.save()
        return 0, json.dumps(user.to_dict()), ''
    except UserDoesNotExist as ex:
        return -errno.ENOENT, '', str(ex)


@CLIWriteCommand('dashboard ac-user-del-roles',
                 'name=username,type=CephString '
                 'name=roles,type=CephString,n=N',
                 'Delete roles from user')
def ac_user_del_roles_cmd(_, username, roles):
    rolesname = roles
    roles = []
    for rolename in rolesname:
        try:
            roles.append(mgr.ACCESS_CTRL_DB.get_role(rolename))
        except RoleDoesNotExist as ex:
            if rolename not in SYSTEM_ROLES:
                return -errno.ENOENT, '', str(ex)
            roles.append(SYSTEM_ROLES[rolename])
    try:
        user = mgr.ACCESS_CTRL_DB.get_user(username)
        user.del_roles(roles)
        mgr.ACCESS_CTRL_DB.save()
        return 0, json.dumps(user.to_dict()), ''
    except UserDoesNotExist as ex:
        return -errno.ENOENT, '', str(ex)
    except RoleNotInUser as ex:
        return -errno.ENOENT, '', str(ex)


@CLIWriteCommand('dashboard ac-user-set-password',
                 'name=username,type=CephString '
                 'name=password,type=CephString',
                 'Set user password')
def ac_user_set_password(_, username, password):
    try:
        user = mgr.ACCESS_CTRL_DB.get_user(username)
        user.set_password(password)

        mgr.ACCESS_CTRL_DB.save()
        return 0, json.dumps(user.to_dict()), ''
    except UserDoesNotExist as ex:
        return -errno.ENOENT, '', str(ex)


@CLIWriteCommand('dashboard ac-user-set-info',
                 'name=username,type=CephString '
                 'name=name,type=CephString '
                 'name=email,type=CephString',
                 'Set user info')
def ac_user_set_info(_, username, name, email):
    try:
        user = mgr.ACCESS_CTRL_DB.get_user(username)
        if name:
            user.name = name
        if email:
            user.email = email
        mgr.ACCESS_CTRL_DB.save()
        return 0, json.dumps(user.to_dict()), ''
    except UserDoesNotExist as ex:
        return -errno.ENOENT, '', str(ex)


class LocalAuthenticator(object):
    def __init__(self):
        load_access_control_db()

    def get_user(self, username):
        return mgr.ACCESS_CTRL_DB.get_user(username)

    def authenticate(self, username, password):
        try:
            user = mgr.ACCESS_CTRL_DB.get_user(username)
            if user.password:
                pass_hash = password_hash(password, user.password)
                if pass_hash == user.password:
                    return user.permissions_dict()
        except UserDoesNotExist:
            logger.debug("User '%s' does not exist", username)
        return None

    def authorize(self, username, scope, permissions):
        user = mgr.ACCESS_CTRL_DB.get_user(username)
        return user.authorize(scope, permissions)
