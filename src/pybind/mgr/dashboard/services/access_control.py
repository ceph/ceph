# -*- coding: utf-8 -*-
# pylint: disable=too-many-arguments,too-many-return-statements
# pylint: disable=too-many-branches, too-many-locals, too-many-statements

import errno
import json
import logging
import re
import threading
import time
from datetime import datetime, timedelta
from string import ascii_lowercase, ascii_uppercase, digits, punctuation
from typing import List, Optional, Sequence

import bcrypt
from mgr_module import CLICheckNonemptyFileInput, CLIReadCommand, CLIWriteCommand

from .. import mgr
from ..exceptions import PasswordPolicyException, PermissionNotValid, \
    PwdExpirationDateNotValid, RoleAlreadyExists, RoleDoesNotExist, \
    RoleIsAssociatedWithUser, RoleNotInUser, ScopeNotInRole, ScopeNotValid, \
    UserAlreadyExists, UserDoesNotExist
from ..security import Permission, Scope
from ..settings import Settings

logger = logging.getLogger('access_control')
DEFAULT_FILE_DESC = 'password/secret'


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


class PasswordPolicy(object):
    def __init__(self, password, username=None, old_password=None):
        """
        :param password: The new plain password.
        :type password: str
        :param username: The name of the user.
        :type username: str | None
        :param old_password: The old plain password.
        :type old_password: str | None
        """
        self.password = password
        self.username = username
        self.old_password = old_password
        self.forbidden_words = Settings.PWD_POLICY_EXCLUSION_LIST.split(',')
        self.complexity_credits = 0

    @staticmethod
    def _check_if_contains_word(password, word):
        return re.compile('(?:{0})'.format(word),
                          flags=re.IGNORECASE).search(password)

    def check_password_complexity(self):
        if not Settings.PWD_POLICY_CHECK_COMPLEXITY_ENABLED:
            return Settings.PWD_POLICY_MIN_COMPLEXITY
        digit_credit = 1
        small_letter_credit = 1
        big_letter_credit = 2
        special_character_credit = 3
        other_character_credit = 5
        self.complexity_credits = 0
        for ch in self.password:
            if ch in ascii_uppercase:
                self.complexity_credits += big_letter_credit
            elif ch in ascii_lowercase:
                self.complexity_credits += small_letter_credit
            elif ch in digits:
                self.complexity_credits += digit_credit
            elif ch in punctuation:
                self.complexity_credits += special_character_credit
            else:
                self.complexity_credits += other_character_credit
        return self.complexity_credits

    def check_is_old_password(self):
        if not Settings.PWD_POLICY_CHECK_OLDPWD_ENABLED:
            return False
        return self.old_password and self.password == self.old_password

    def check_if_contains_username(self):
        if not Settings.PWD_POLICY_CHECK_USERNAME_ENABLED:
            return False
        if not self.username:
            return False
        return self._check_if_contains_word(self.password, self.username)

    def check_if_contains_forbidden_words(self):
        if not Settings.PWD_POLICY_CHECK_EXCLUSION_LIST_ENABLED:
            return False
        return self._check_if_contains_word(self.password,
                                            '|'.join(self.forbidden_words))

    def check_if_sequential_characters(self):
        if not Settings.PWD_POLICY_CHECK_SEQUENTIAL_CHARS_ENABLED:
            return False
        for i in range(1, len(self.password) - 1):
            if ord(self.password[i - 1]) + 1 == ord(self.password[i])\
               == ord(self.password[i + 1]) - 1:
                return True
        return False

    def check_if_repetitive_characters(self):
        if not Settings.PWD_POLICY_CHECK_REPETITIVE_CHARS_ENABLED:
            return False
        for i in range(1, len(self.password) - 1):
            if self.password[i - 1] == self.password[i] == self.password[i + 1]:
                return True
        return False

    def check_password_length(self):
        if not Settings.PWD_POLICY_CHECK_LENGTH_ENABLED:
            return True
        return len(self.password) >= Settings.PWD_POLICY_MIN_LENGTH

    def check_all(self):
        """
        Perform all password policy checks.
        :raise PasswordPolicyException: If a password policy check fails.
        """
        if not Settings.PWD_POLICY_ENABLED:
            return
        if self.check_password_complexity() < Settings.PWD_POLICY_MIN_COMPLEXITY:
            raise PasswordPolicyException('Password is too weak.')
        if not self.check_password_length():
            raise PasswordPolicyException('Password is too weak.')
        if self.check_is_old_password():
            raise PasswordPolicyException('Password must not be the same as the previous one.')
        if self.check_if_contains_username():
            raise PasswordPolicyException('Password must not contain username.')
        result = self.check_if_contains_forbidden_words()
        if result:
            raise PasswordPolicyException('Password must not contain the keyword "{}".'.format(
                result.group(0)))
        if self.check_if_repetitive_characters():
            raise PasswordPolicyException('Password must not contain repetitive characters.')
        if self.check_if_sequential_characters():
            raise PasswordPolicyException('Password must not contain sequential characters.')


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
ADMIN_ROLE = Role(
    'administrator', 'allows full permissions for all security scopes', {
        scope_name: Permission.all_permissions()
        for scope_name in Scope.all_scopes()
    })


# read-only role provides read-only permission for all scopes
READ_ONLY_ROLE = Role(
    'read-only',
    'allows read permission for all security scope except dashboard settings and config-opt', {
        scope_name: [_P.READ] for scope_name in Scope.all_scopes()
        if scope_name not in (Scope.DASHBOARD_SETTINGS, Scope.CONFIG_OPT)
    })


# block manager role provides all permission for block related scopes
BLOCK_MGR_ROLE = Role(
    'block-manager', 'allows full permissions for rbd-image, rbd-mirroring, and iscsi scopes', {
        Scope.RBD_IMAGE: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
        Scope.POOL: [_P.READ],
        Scope.ISCSI: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
        Scope.RBD_MIRRORING: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
        Scope.GRAFANA: [_P.READ],
    })


# RadosGW manager role provides all permissions for block related scopes
RGW_MGR_ROLE = Role(
    'rgw-manager', 'allows full permissions for the rgw scope', {
        Scope.RGW: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
        Scope.GRAFANA: [_P.READ],
    })


# Cluster manager role provides all permission for OSDs, Monitors, and
# Config options
CLUSTER_MGR_ROLE = Role(
    'cluster-manager', """allows full permissions for the hosts, osd, mon, mgr,
    and config-opt scopes""", {
        Scope.HOSTS: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
        Scope.OSD: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
        Scope.MONITOR: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
        Scope.MANAGER: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
        Scope.CONFIG_OPT: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
        Scope.LOG: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
        Scope.GRAFANA: [_P.READ],
    })


# Pool manager role provides all permissions for pool related scopes
POOL_MGR_ROLE = Role(
    'pool-manager', 'allows full permissions for the pool scope', {
        Scope.POOL: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
        Scope.GRAFANA: [_P.READ],
    })

# CephFS manager role provides all permissions for CephFS related scopes
CEPHFS_MGR_ROLE = Role(
    'cephfs-manager', 'allows full permissions for the cephfs scope', {
        Scope.CEPHFS: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
        Scope.GRAFANA: [_P.READ],
    })

GANESHA_MGR_ROLE = Role(
    'ganesha-manager', 'allows full permissions for the nfs-ganesha scope', {
        Scope.NFS_GANESHA: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
        Scope.CEPHFS: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
        Scope.RGW: [_P.READ, _P.CREATE, _P.UPDATE, _P.DELETE],
        Scope.GRAFANA: [_P.READ],
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
                 last_update=None, enabled=True, pwd_expiration_date=None,
                 pwd_update_required=False):
        self.username = username
        self.password = password
        self.name = name
        self.email = email
        self.invalid_auth_attempt = 0
        if roles is None:
            self.roles = set()
        else:
            self.roles = roles
        if last_update is None:
            self.refresh_last_update()
        else:
            self.last_update = last_update
        self._enabled = enabled
        self.pwd_expiration_date = pwd_expiration_date
        if self.pwd_expiration_date is None:
            self.refresh_pwd_expiration_date()
        self.pwd_update_required = pwd_update_required

    def refresh_last_update(self):
        self.last_update = int(time.time())

    def refresh_pwd_expiration_date(self):
        if Settings.USER_PWD_EXPIRATION_SPAN > 0:
            expiration_date = datetime.utcnow() + timedelta(
                days=Settings.USER_PWD_EXPIRATION_SPAN)
            self.pwd_expiration_date = int(time.mktime(expiration_date.timetuple()))
        else:
            self.pwd_expiration_date = None

    @property
    def enabled(self):
        return self._enabled

    @enabled.setter
    def enabled(self, value):
        self._enabled = value
        self.refresh_last_update()

    def set_password(self, password):
        self.set_password_hash(password_hash(password))

    def set_password_hash(self, hashed_password):
        self.invalid_auth_attempt = 0
        self.password = hashed_password
        self.refresh_last_update()
        self.refresh_pwd_expiration_date()
        self.pwd_update_required = False

    def compare_password(self, password):
        """
        Compare the specified password with the user password.
        :param password: The plain password to check.
        :type password: str
        :return: `True` if the passwords are equal, otherwise `False`.
        :rtype: bool
        """
        pass_hash = password_hash(password, salt_password=self.password)
        return pass_hash == self.password

    def is_pwd_expired(self):
        if self.pwd_expiration_date:
            current_time = int(time.mktime(datetime.utcnow().timetuple()))
            return self.pwd_expiration_date < current_time
        return False

    def set_roles(self, roles):
        self.roles = set(roles)
        self.refresh_last_update()

    def add_roles(self, roles):
        self.roles = self.roles.union(set(roles))
        self.refresh_last_update()

    def del_roles(self, roles):
        for role in roles:
            if role not in self.roles:
                raise RoleNotInUser(role.name, self.username)
        self.roles.difference_update(set(roles))
        self.refresh_last_update()

    def authorize(self, scope, permissions):
        if self.pwd_update_required:
            return False

        for role in self.roles:
            if role.authorize(scope, permissions):
                return True
        return False

    def permissions_dict(self):
        # type: () -> dict
        perms = {}  # type: dict
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
            'lastUpdate': self.last_update,
            'enabled': self.enabled,
            'pwdExpirationDate': self.pwd_expiration_date,
            'pwdUpdateRequired': self.pwd_update_required
        }

    @classmethod
    def from_dict(cls, u_dict, roles):
        return User(u_dict['username'], u_dict['password'], u_dict['name'],
                    u_dict['email'], {roles[r] for r in u_dict['roles']},
                    u_dict['lastUpdate'], u_dict['enabled'],
                    u_dict['pwdExpirationDate'], u_dict['pwdUpdateRequired'])


class AccessControlDB(object):
    VERSION = 2
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

    def increment_attempt(self, username):
        with self.lock:
            if username in self.users:
                self.users[username].invalid_auth_attempt += 1

    def reset_attempt(self, username):
        with self.lock:
            if username in self.users:
                self.users[username].invalid_auth_attempt = 0

    def get_attempt(self, username):
        with self.lock:
            try:
                return self.users[username].invalid_auth_attempt
            except KeyError:
                return 0

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

    def create_user(self, username, password, name, email, enabled=True,
                    pwd_expiration_date=None, pwd_update_required=False):
        logger.debug("creating user: username=%s", username)
        with self.lock:
            if username in self.users:
                raise UserAlreadyExists(username)
            if pwd_expiration_date and \
               (pwd_expiration_date < int(time.mktime(datetime.utcnow().timetuple()))):
                raise PwdExpirationDateNotValid()
            user = User(username, password_hash(password), name, email, enabled=enabled,
                        pwd_expiration_date=pwd_expiration_date,
                        pwd_update_required=pwd_update_required)
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
                    user.refresh_last_update()

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
        logger.debug("Checking for previous DB versions")

        def check_migrate_v1_to_current():
            # Check if version 1 exists in the DB and migrate it to current version
            v1_db = mgr.get_store(self.accessdb_config_key(1))
            if v1_db:
                logger.debug("Found database v1 credentials")
                v1_db = json.loads(v1_db)

                for user, _ in v1_db['users'].items():
                    v1_db['users'][user]['enabled'] = True
                    v1_db['users'][user]['pwdExpirationDate'] = None
                    v1_db['users'][user]['pwdUpdateRequired'] = False

                self.roles = {rn: Role.from_dict(r) for rn, r in v1_db.get('roles', {}).items()}
                self.users = {un: User.from_dict(u, dict(self.roles, **SYSTEM_ROLES))
                              for un, u in v1_db.get('users', {}).items()}

                self.save()

        check_migrate_v1_to_current()

    @classmethod
    def load(cls):
        logger.info("Loading user roles DB version=%s", cls.VERSION)

        json_db = mgr.get_store(cls.accessdb_config_key())
        if json_db is None:
            logger.debug("No DB v%s found, creating new...", cls.VERSION)
            db = cls(cls.VERSION, {}, {})
            # check if we can update from a previous version database
            db.check_and_update_db()
            return db

        dict_db = json.loads(json_db)
        roles = {rn: Role.from_dict(r)
                 for rn, r in dict_db.get('roles', {}).items()}
        users = {un: User.from_dict(u, dict(roles, **SYSTEM_ROLES))
                 for un, u in dict_db.get('users', {}).items()}
        return cls(dict_db['version'], users, roles)


def load_access_control_db():
    mgr.ACCESS_CTRL_DB = AccessControlDB.load()  # type: ignore


# CLI dashboard access control scope commands

@CLIWriteCommand('dashboard set-login-credentials')
@CLICheckNonemptyFileInput(desc=DEFAULT_FILE_DESC)
def set_login_credentials_cmd(_, username: str, inbuf: str):
    '''
    Set the login credentials. Password read from -i <file>
    '''
    password = inbuf
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


@CLIReadCommand('dashboard ac-role-show')
def ac_role_show_cmd(_, rolename: Optional[str] = None):
    '''
    Show role info
    '''
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


@CLIWriteCommand('dashboard ac-role-create')
def ac_role_create_cmd(_, rolename: str, description: Optional[str] = None):
    '''
    Create a new access control role
    '''
    try:
        role = mgr.ACCESS_CTRL_DB.create_role(rolename, description)
        mgr.ACCESS_CTRL_DB.save()
        return 0, json.dumps(role.to_dict()), ''
    except RoleAlreadyExists as ex:
        return -errno.EEXIST, '', str(ex)


@CLIWriteCommand('dashboard ac-role-delete')
def ac_role_delete_cmd(_, rolename: str):
    '''
    Delete an access control role
    '''
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


@CLIWriteCommand('dashboard ac-role-add-scope-perms')
def ac_role_add_scope_perms_cmd(_,
                                rolename: str,
                                scopename: str,
                                permissions: Sequence[str]):
    '''
    Add the scope permissions for a role
    '''
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


@CLIWriteCommand('dashboard ac-role-del-scope-perms')
def ac_role_del_scope_perms_cmd(_, rolename: str, scopename: str):
    '''
    Delete the scope permissions for a role
    '''
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


@CLIReadCommand('dashboard ac-user-show')
def ac_user_show_cmd(_, username: Optional[str] = None):
    '''
    Show user info
    '''
    if not username:
        users = mgr.ACCESS_CTRL_DB.users
        users_list = [name for name, _ in users.items()]
        return 0, json.dumps(users_list), ''
    try:
        user = mgr.ACCESS_CTRL_DB.get_user(username)
        return 0, json.dumps(user.to_dict()), ''
    except UserDoesNotExist as ex:
        return -errno.ENOENT, '', str(ex)


@CLIWriteCommand('dashboard ac-user-create')
@CLICheckNonemptyFileInput(desc=DEFAULT_FILE_DESC)
def ac_user_create_cmd(_, username: str, inbuf: str,
                       rolename: Optional[str] = None,
                       name: Optional[str] = None,
                       email: Optional[str] = None,
                       enabled: bool = True,
                       force_password: bool = False,
                       pwd_expiration_date: Optional[int] = None,
                       pwd_update_required: bool = False):
    '''
    Create a user. Password read from -i <file>
    '''
    password = inbuf
    try:
        role = mgr.ACCESS_CTRL_DB.get_role(rolename) if rolename else None
    except RoleDoesNotExist as ex:
        if rolename not in SYSTEM_ROLES:
            return -errno.ENOENT, '', str(ex)
        role = SYSTEM_ROLES[rolename]

    try:
        if not force_password:
            pw_check = PasswordPolicy(password, username)
            pw_check.check_all()
        user = mgr.ACCESS_CTRL_DB.create_user(username, password, name, email,
                                              enabled, pwd_expiration_date,
                                              pwd_update_required)
    except PasswordPolicyException as ex:
        return -errno.EINVAL, '', str(ex)
    except UserAlreadyExists as ex:
        return 0, str(ex), ''

    if role:
        user.set_roles([role])
    mgr.ACCESS_CTRL_DB.save()
    return 0, json.dumps(user.to_dict()), ''


@CLIWriteCommand('dashboard ac-user-enable')
def ac_user_enable(_, username: str):
    '''
    Enable a user
    '''
    try:
        user = mgr.ACCESS_CTRL_DB.get_user(username)
        user.enabled = True
        mgr.ACCESS_CTRL_DB.reset_attempt(username)

        mgr.ACCESS_CTRL_DB.save()
        return 0, json.dumps(user.to_dict()), ''
    except UserDoesNotExist as ex:
        return -errno.ENOENT, '', str(ex)


@CLIWriteCommand('dashboard ac-user-disable')
def ac_user_disable(_, username: str):
    '''
    Disable a user
    '''
    try:
        user = mgr.ACCESS_CTRL_DB.get_user(username)
        user.enabled = False

        mgr.ACCESS_CTRL_DB.save()
        return 0, json.dumps(user.to_dict()), ''
    except UserDoesNotExist as ex:
        return -errno.ENOENT, '', str(ex)


@CLIWriteCommand('dashboard ac-user-delete')
def ac_user_delete_cmd(_, username: str):
    '''
    Delete user
    '''
    try:
        mgr.ACCESS_CTRL_DB.delete_user(username)
        mgr.ACCESS_CTRL_DB.save()
        return 0, "User '{}' deleted".format(username), ""
    except UserDoesNotExist as ex:
        return -errno.ENOENT, '', str(ex)


@CLIWriteCommand('dashboard ac-user-set-roles')
def ac_user_set_roles_cmd(_, username: str, roles: Sequence[str]):
    '''
    Set user roles
    '''
    rolesname = roles
    roles: List[Role] = []
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


@CLIWriteCommand('dashboard ac-user-add-roles')
def ac_user_add_roles_cmd(_, username: str, roles: Sequence[str]):
    '''
    Add roles to user
    '''
    rolesname = roles
    roles: List[Role] = []
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


@CLIWriteCommand('dashboard ac-user-del-roles')
def ac_user_del_roles_cmd(_, username: str, roles: Sequence[str]):
    '''
    Delete roles from user
    '''
    rolesname = roles
    roles: List[Role] = []
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


@CLIWriteCommand('dashboard ac-user-set-password')
@CLICheckNonemptyFileInput(desc=DEFAULT_FILE_DESC)
def ac_user_set_password(_, username: str, inbuf: str,
                         force_password: bool = False):
    '''
    Set user password from -i <file>
    '''
    password = inbuf
    try:
        user = mgr.ACCESS_CTRL_DB.get_user(username)
        if not force_password:
            pw_check = PasswordPolicy(password, user.name)
            pw_check.check_all()
        user.set_password(password)
        mgr.ACCESS_CTRL_DB.save()
        return 0, json.dumps(user.to_dict()), ''
    except PasswordPolicyException as ex:
        return -errno.EINVAL, '', str(ex)
    except UserDoesNotExist as ex:
        return -errno.ENOENT, '', str(ex)


@CLIWriteCommand('dashboard ac-user-set-password-hash')
@CLICheckNonemptyFileInput(desc=DEFAULT_FILE_DESC)
def ac_user_set_password_hash(_, username: str, inbuf: str):
    '''
    Set user password bcrypt hash from -i <file>
    '''
    hashed_password = inbuf
    try:
        # make sure the hashed_password is actually a bcrypt hash
        bcrypt.checkpw(b'', hashed_password.encode('utf-8'))
        user = mgr.ACCESS_CTRL_DB.get_user(username)
        user.set_password_hash(hashed_password)

        mgr.ACCESS_CTRL_DB.save()
        return 0, json.dumps(user.to_dict()), ''
    except ValueError:
        return -errno.EINVAL, '', 'Invalid password hash'
    except UserDoesNotExist as ex:
        return -errno.ENOENT, '', str(ex)


@CLIWriteCommand('dashboard ac-user-set-info')
def ac_user_set_info(_, username: str, name: str, email: str):
    '''
    Set user info
    '''
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
                if user.enabled and user.compare_password(password) \
                   and not user.is_pwd_expired():
                    return {'permissions': user.permissions_dict(),
                            'pwdExpirationDate': user.pwd_expiration_date,
                            'pwdUpdateRequired': user.pwd_update_required}
        except UserDoesNotExist:
            logger.debug("User '%s' does not exist", username)
        return None

    def authorize(self, username, scope, permissions):
        user = mgr.ACCESS_CTRL_DB.get_user(username)
        return user.authorize(scope, permissions)
