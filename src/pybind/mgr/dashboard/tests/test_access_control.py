# -*- coding: utf-8 -*-
# pylint: disable=dangerous-default-value,too-many-public-methods

import errno
import json
import time
import unittest
from datetime import datetime, timedelta

from mgr_module import ERROR_MSG_EMPTY_INPUT_FILE

from .. import mgr
from ..security import Permission, Scope
from ..services.access_control import SYSTEM_ROLES, AccessControlDB, \
    PasswordPolicy, load_access_control_db, password_hash
from ..settings import Settings
from . import CLICommandTestMixin, CmdException  # pylint: disable=no-name-in-module


class AccessControlTest(unittest.TestCase, CLICommandTestMixin):

    @classmethod
    def setUpClass(cls):
        cls.mock_kv_store()
        mgr.ACCESS_CONTROL_DB = None

    def setUp(self):
        self.CONFIG_KEY_DICT.clear()
        load_access_control_db()

    def load_persistent_db(self):
        config_key = AccessControlDB.accessdb_config_key()
        self.assertIn(config_key, self.CONFIG_KEY_DICT)
        db_json = self.CONFIG_KEY_DICT[config_key]
        db = json.loads(db_json)
        return db

    # The DB is written to persistent storage the first time it is saved.
    # However, should an operation fail due to <reasons>, we may end up in
    # a state where we have a completely empty CONFIG_KEY_DICT (our mock
    # equivalent to the persistent state). While this works for most of the
    # tests in this class, that would prevent us from testing things like
    # "run a command that is expected to fail, and then ensure nothing
    # happened", because we'd be asserting in `load_persistent_db()` due to
    # the map being empty.
    #
    # This function will therefore force state to be written to our mock
    # persistent state. We could have added this extra step to
    # `load_persistent_db()` directly, but that would conflict with the
    # upgrade tests. This way, we can selectively enforce this requirement
    # where we believe it to be necessary; generically speaking, this should
    # not be needed unless we're testing very specific behaviors.
    #
    def setup_and_load_persistent_db(self):
        mgr.ACCESS_CTRL_DB.save()
        self.load_persistent_db()

    def validate_persistent_role(self, rolename, scopes_permissions,
                                 description=None):
        db = self.load_persistent_db()
        self.assertIn('roles', db)
        self.assertIn(rolename, db['roles'])
        self.assertEqual(db['roles'][rolename]['name'], rolename)
        self.assertEqual(db['roles'][rolename]['description'], description)
        self.assertDictEqual(db['roles'][rolename]['scopes_permissions'],
                             scopes_permissions)

    def validate_persistent_no_role(self, rolename):
        db = self.load_persistent_db()
        self.assertIn('roles', db)
        self.assertNotIn(rolename, db['roles'])

    def validate_persistent_user(self, username, roles, password=None,
                                 name=None, email=None, last_update=None,
                                 enabled=True, pwdExpirationDate=None):
        db = self.load_persistent_db()
        self.assertIn('users', db)
        self.assertIn(username, db['users'])
        self.assertEqual(db['users'][username]['username'], username)
        self.assertListEqual(db['users'][username]['roles'], roles)
        if password:
            self.assertEqual(db['users'][username]['password'], password)
        if name:
            self.assertEqual(db['users'][username]['name'], name)
        if email:
            self.assertEqual(db['users'][username]['email'], email)
        if last_update:
            self.assertEqual(db['users'][username]['lastUpdate'], last_update)
        if pwdExpirationDate:
            self.assertEqual(db['users'][username]['pwdExpirationDate'], pwdExpirationDate)
        self.assertEqual(db['users'][username]['enabled'], enabled)

    def validate_persistent_no_user(self, username):
        db = self.load_persistent_db()
        self.assertIn('users', db)
        self.assertNotIn(username, db['users'])

    def test_create_role(self):
        role = self.exec_cmd('ac-role-create', rolename='test_role')
        self.assertDictEqual(role, {'name': 'test_role', 'description': None,
                                    'scopes_permissions': {}})
        self.validate_persistent_role('test_role', {})

    def test_create_role_with_desc(self):
        role = self.exec_cmd('ac-role-create', rolename='test_role',
                             description='Test Role')
        self.assertDictEqual(role, {'name': 'test_role',
                                    'description': 'Test Role',
                                    'scopes_permissions': {}})
        self.validate_persistent_role('test_role', {}, 'Test Role')

    def test_create_duplicate_role(self):
        self.test_create_role()

        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('ac-role-create', rolename='test_role')

        self.assertEqual(ctx.exception.retcode, -errno.EEXIST)
        self.assertEqual(str(ctx.exception), "Role 'test_role' already exists")

    def test_delete_role(self):
        self.test_create_role()
        out = self.exec_cmd('ac-role-delete', rolename='test_role')
        self.assertEqual(out, "Role 'test_role' deleted")
        self.validate_persistent_no_role('test_role')

    def test_delete_nonexistent_role(self):
        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('ac-role-delete', rolename='test_role')

        self.assertEqual(ctx.exception.retcode, -errno.ENOENT)
        self.assertEqual(str(ctx.exception), "Role 'test_role' does not exist")

    def test_show_single_role(self):
        self.test_create_role()
        role = self.exec_cmd('ac-role-show', rolename='test_role')
        self.assertDictEqual(role, {'name': 'test_role', 'description': None,
                                    'scopes_permissions': {}})

    def test_show_nonexistent_role(self):
        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('ac-role-show', rolename='test_role')

        self.assertEqual(ctx.exception.retcode, -errno.ENOENT)
        self.assertEqual(str(ctx.exception), "Role 'test_role' does not exist")

    def test_show_system_roles(self):
        roles = self.exec_cmd('ac-role-show')
        self.assertEqual(len(roles), len(SYSTEM_ROLES))
        for role in roles:
            self.assertIn(role, SYSTEM_ROLES)

    def test_show_system_role(self):
        role = self.exec_cmd('ac-role-show', rolename="read-only")
        self.assertEqual(role['name'], 'read-only')
        self.assertEqual(
            role['description'],
            'allows read permission for all security scope except dashboard settings and config-opt'
        )

    def test_delete_system_role(self):
        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('ac-role-delete', rolename='administrator')

        self.assertEqual(ctx.exception.retcode, -errno.EPERM)
        self.assertEqual(str(ctx.exception),
                         "Cannot delete system role 'administrator'")

    def test_add_role_scope_perms(self):
        self.test_create_role()
        self.exec_cmd('ac-role-add-scope-perms', rolename='test_role',
                      scopename=Scope.POOL,
                      permissions=[Permission.READ, Permission.DELETE])
        role = self.exec_cmd('ac-role-show', rolename='test_role')
        self.assertDictEqual(role, {'name': 'test_role',
                                    'description': None,
                                    'scopes_permissions': {
                                        Scope.POOL: [Permission.DELETE,
                                                     Permission.READ]
                                    }})
        self.validate_persistent_role('test_role', {
            Scope.POOL: [Permission.DELETE, Permission.READ]
        })

    def test_del_role_scope_perms(self):
        self.test_add_role_scope_perms()
        self.exec_cmd('ac-role-add-scope-perms', rolename='test_role',
                      scopename=Scope.MONITOR,
                      permissions=[Permission.READ, Permission.CREATE])
        self.validate_persistent_role('test_role', {
            Scope.POOL: [Permission.DELETE, Permission.READ],
            Scope.MONITOR: [Permission.CREATE, Permission.READ]
        })
        self.exec_cmd('ac-role-del-scope-perms', rolename='test_role',
                      scopename=Scope.POOL)
        role = self.exec_cmd('ac-role-show', rolename='test_role')
        self.assertDictEqual(role, {'name': 'test_role',
                                    'description': None,
                                    'scopes_permissions': {
                                        Scope.MONITOR: [Permission.CREATE,
                                                        Permission.READ]
                                    }})
        self.validate_persistent_role('test_role', {
            Scope.MONITOR: [Permission.CREATE, Permission.READ]
        })

    def test_add_role_scope_perms_nonexistent_role(self):

        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('ac-role-add-scope-perms', rolename='test_role',
                          scopename='pool',
                          permissions=['read', 'delete'])

        self.assertEqual(ctx.exception.retcode, -errno.ENOENT)
        self.assertEqual(str(ctx.exception), "Role 'test_role' does not exist")

    def test_add_role_invalid_scope_perms(self):
        self.test_create_role()

        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('ac-role-add-scope-perms', rolename='test_role',
                          scopename='invalidscope',
                          permissions=['read', 'delete'])

        self.assertEqual(ctx.exception.retcode, -errno.EINVAL)
        self.assertEqual(str(ctx.exception),
                         "Scope 'invalidscope' is not valid\n Possible values: "
                         "{}".format(Scope.all_scopes()))

    def test_add_role_scope_invalid_perms(self):
        self.test_create_role()

        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('ac-role-add-scope-perms', rolename='test_role',
                          scopename='pool', permissions=['invalidperm'])

        self.assertEqual(ctx.exception.retcode, -errno.EINVAL)
        self.assertEqual(str(ctx.exception),
                         "Permission 'invalidperm' is not valid\n Possible "
                         "values: {}".format(Permission.all_permissions()))

    def test_del_role_scope_perms_nonexistent_role(self):

        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('ac-role-del-scope-perms', rolename='test_role',
                          scopename='pool')

        self.assertEqual(ctx.exception.retcode, -errno.ENOENT)
        self.assertEqual(str(ctx.exception), "Role 'test_role' does not exist")

    def test_del_role_nonexistent_scope_perms(self):
        self.test_add_role_scope_perms()

        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('ac-role-del-scope-perms', rolename='test_role',
                          scopename='nonexistentscope')

        self.assertEqual(ctx.exception.retcode, -errno.ENOENT)
        self.assertEqual(str(ctx.exception),
                         "There are no permissions for scope 'nonexistentscope' "
                         "in role 'test_role'")

    def test_not_permitted_add_role_scope_perms(self):
        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('ac-role-add-scope-perms', rolename='read-only',
                          scopename='pool', permissions=['read', 'delete'])

        self.assertEqual(ctx.exception.retcode, -errno.EPERM)
        self.assertEqual(str(ctx.exception),
                         "Cannot update system role 'read-only'")

    def test_not_permitted_del_role_scope_perms(self):
        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('ac-role-del-scope-perms', rolename='read-only',
                          scopename='pool')

        self.assertEqual(ctx.exception.retcode, -errno.EPERM)
        self.assertEqual(str(ctx.exception),
                         "Cannot update system role 'read-only'")

    def test_create_user(self, username='admin', rolename=None, enabled=True,
                         pwdExpirationDate=None):
        user = self.exec_cmd('ac-user-create', username=username,
                             rolename=rolename, inbuf='admin',
                             name='{} User'.format(username),
                             email='{}@user.com'.format(username),
                             enabled=enabled, force_password=True,
                             pwd_expiration_date=pwdExpirationDate)

        pass_hash = password_hash('admin', user['password'])
        self.assertDictEqual(user, {
            'username': username,
            'password': pass_hash,
            'pwdExpirationDate': pwdExpirationDate,
            'pwdUpdateRequired': False,
            'lastUpdate': user['lastUpdate'],
            'name': '{} User'.format(username),
            'email': '{}@user.com'.format(username),
            'roles': [rolename] if rolename else [],
            'enabled': enabled
        })
        self.validate_persistent_user(username, [rolename] if rolename else [],
                                      pass_hash, '{} User'.format(username),
                                      '{}@user.com'.format(username),
                                      user['lastUpdate'], enabled)
        return user

    def test_create_disabled_user(self):
        self.test_create_user(enabled=False)

    def test_create_user_pwd_expiration_date(self):
        expiration_date = datetime.utcnow() + timedelta(days=10)
        expiration_date = int(time.mktime(expiration_date.timetuple()))
        self.test_create_user(pwdExpirationDate=expiration_date)

    def test_create_user_with_role(self):
        self.test_add_role_scope_perms()
        self.test_create_user(rolename='test_role')

    def test_create_user_with_system_role(self):
        self.test_create_user(rolename='administrator')

    def test_delete_user(self):
        self.test_create_user()
        out = self.exec_cmd('ac-user-delete', username='admin')
        self.assertEqual(out, "User 'admin' deleted")
        users = self.exec_cmd('ac-user-show')
        self.assertEqual(len(users), 0)
        self.validate_persistent_no_user('admin')

    def test_create_duplicate_user(self):
        self.test_create_user()
        ret = self.exec_cmd('ac-user-create', username='admin', inbuf='admin',
                            force_password=True)
        self.assertEqual(ret, "User 'admin' already exists")

    def test_create_users_with_dne_role(self):
        # one time call to setup our persistent db
        self.setup_and_load_persistent_db()

        # create a user with a role that does not exist; expect a failure
        try:
            self.exec_cmd('ac-user-create', username='foo',
                          rolename='dne_role', inbuf='foopass',
                          name='foo User', email='foo@user.com',
                          force_password=True)
        except CmdException as e:
            self.assertEqual(e.retcode, -errno.ENOENT)

        db = self.load_persistent_db()
        if 'users' in db:
            self.assertNotIn('foo', db['users'])

        # We could just finish our test here, given we ensured that the user
        # with a non-existent role is not in persistent storage. However,
        # we're going to test the database's consistency, making sure that
        # side-effects are not written to persistent storage once we commit
        # an unrelated operation. To ensure this, we'll issue another
        # operation that is sharing the same code path, and will check whether
        # the next operation commits dirty state.

        # create a role (this will be 'test_role')
        self.test_create_role()
        self.exec_cmd('ac-user-create', username='bar',
                      rolename='test_role', inbuf='barpass',
                      name='bar User', email='bar@user.com',
                      force_password=True)

        # validate db:
        #   user 'foo' should not exist
        #   user 'bar' should exist and have role 'test_role'
        self.validate_persistent_user('bar', ['test_role'])

        db = self.load_persistent_db()
        self.assertIn('users', db)
        self.assertNotIn('foo', db['users'])

    def test_delete_nonexistent_user(self):
        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('ac-user-delete', username='admin')

        self.assertEqual(ctx.exception.retcode, -errno.ENOENT)
        self.assertEqual(str(ctx.exception), "User 'admin' does not exist")

    def test_add_user_roles(self, username='admin',
                            roles=['pool-manager', 'block-manager']):
        user_orig = self.test_create_user(username)
        uroles = []
        for role in roles:
            uroles.append(role)
            uroles.sort()
            user = self.exec_cmd('ac-user-add-roles', username=username,
                                 roles=[role])
            self.assertLessEqual(uroles, user['roles'])
        self.validate_persistent_user(username, uroles)
        self.assertGreaterEqual(user['lastUpdate'], user_orig['lastUpdate'])

    def test_add_user_roles2(self):
        user_orig = self.test_create_user()
        user = self.exec_cmd('ac-user-add-roles', username="admin",
                             roles=['pool-manager', 'block-manager'])
        self.assertLessEqual(['block-manager', 'pool-manager'],
                             user['roles'])
        self.validate_persistent_user('admin', ['block-manager',
                                                'pool-manager'])
        self.assertGreaterEqual(user['lastUpdate'], user_orig['lastUpdate'])

    def test_add_user_roles_not_existent_user(self):
        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('ac-user-add-roles', username="admin",
                          roles=['pool-manager', 'block-manager'])

        self.assertEqual(ctx.exception.retcode, -errno.ENOENT)
        self.assertEqual(str(ctx.exception), "User 'admin' does not exist")

    def test_add_user_roles_not_existent_role(self):
        self.test_create_user()
        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('ac-user-add-roles', username="admin",
                          roles=['Invalid Role'])

        self.assertEqual(ctx.exception.retcode, -errno.ENOENT)
        self.assertEqual(str(ctx.exception),
                         "Role 'Invalid Role' does not exist")

    def test_set_user_roles(self):
        user_orig = self.test_create_user()
        user = self.exec_cmd('ac-user-add-roles', username="admin",
                             roles=['pool-manager'])
        self.assertLessEqual(['pool-manager'], user['roles'])
        self.validate_persistent_user('admin', ['pool-manager'])
        self.assertGreaterEqual(user['lastUpdate'], user_orig['lastUpdate'])
        user2 = self.exec_cmd('ac-user-set-roles', username="admin",
                              roles=['rgw-manager', 'block-manager'])
        self.assertLessEqual(['block-manager', 'rgw-manager'],
                             user2['roles'])
        self.validate_persistent_user('admin', ['block-manager',
                                                'rgw-manager'])
        self.assertGreaterEqual(user2['lastUpdate'], user['lastUpdate'])

    def test_set_user_roles_not_existent_user(self):
        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('ac-user-set-roles', username="admin",
                          roles=['pool-manager', 'block-manager'])

        self.assertEqual(ctx.exception.retcode, -errno.ENOENT)
        self.assertEqual(str(ctx.exception), "User 'admin' does not exist")

    def test_set_user_roles_not_existent_role(self):
        self.test_create_user()
        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('ac-user-set-roles', username="admin",
                          roles=['Invalid Role'])

        self.assertEqual(ctx.exception.retcode, -errno.ENOENT)
        self.assertEqual(str(ctx.exception),
                         "Role 'Invalid Role' does not exist")

    def test_del_user_roles(self):
        self.test_add_user_roles()
        user = self.exec_cmd('ac-user-del-roles', username="admin",
                             roles=['pool-manager'])
        self.assertLessEqual(['block-manager'], user['roles'])
        self.validate_persistent_user('admin', ['block-manager'])

    def test_del_user_roles_not_existent_user(self):
        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('ac-user-del-roles', username="admin",
                          roles=['pool-manager', 'block-manager'])

        self.assertEqual(ctx.exception.retcode, -errno.ENOENT)
        self.assertEqual(str(ctx.exception), "User 'admin' does not exist")

    def test_del_user_roles_not_existent_role(self):
        self.test_create_user()
        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('ac-user-del-roles', username="admin",
                          roles=['Invalid Role'])

        self.assertEqual(ctx.exception.retcode, -errno.ENOENT)
        self.assertEqual(str(ctx.exception),
                         "Role 'Invalid Role' does not exist")

    def test_del_user_roles_not_associated_role(self):
        self.test_create_user()
        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('ac-user-del-roles', username="admin",
                          roles=['rgw-manager'])

        self.assertEqual(ctx.exception.retcode, -errno.ENOENT)
        self.assertEqual(str(ctx.exception),
                         "Role 'rgw-manager' is not associated with user "
                         "'admin'")

    def test_show_user(self):
        self.test_add_user_roles()
        user = self.exec_cmd('ac-user-show', username='admin')
        pass_hash = password_hash('admin', user['password'])
        self.assertDictEqual(user, {
            'username': 'admin',
            'lastUpdate': user['lastUpdate'],
            'password': pass_hash,
            'pwdExpirationDate': None,
            'pwdUpdateRequired': False,
            'name': 'admin User',
            'email': 'admin@user.com',
            'roles': ['block-manager', 'pool-manager'],
            'enabled': True
        })

    def test_show_nonexistent_user(self):
        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('ac-user-show', username='admin')

        self.assertEqual(ctx.exception.retcode, -errno.ENOENT)
        self.assertEqual(str(ctx.exception), "User 'admin' does not exist")

    def test_show_all_users(self):
        self.test_add_user_roles('admin', ['administrator'])
        self.test_add_user_roles('guest', ['read-only'])
        users = self.exec_cmd('ac-user-show')
        self.assertEqual(len(users), 2)
        for user in users:
            self.assertIn(user, ['admin', 'guest'])

    def test_del_role_associated_with_user(self):
        self.test_create_role()
        self.test_add_user_roles('guest', ['test_role'])

        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('ac-role-delete', rolename='test_role')

        self.assertEqual(ctx.exception.retcode, -errno.EPERM)
        self.assertEqual(str(ctx.exception),
                         "Role 'test_role' is still associated with user "
                         "'guest'")

    def test_set_user_info(self):
        user_orig = self.test_create_user()
        user = self.exec_cmd('ac-user-set-info', username='admin',
                             name='Admin Name', email='admin@admin.com')
        pass_hash = password_hash('admin', user['password'])
        self.assertDictEqual(user, {
            'username': 'admin',
            'password': pass_hash,
            'pwdExpirationDate': None,
            'pwdUpdateRequired': False,
            'name': 'Admin Name',
            'email': 'admin@admin.com',
            'lastUpdate': user['lastUpdate'],
            'roles': [],
            'enabled': True
        })
        self.validate_persistent_user('admin', [], pass_hash, 'Admin Name',
                                      'admin@admin.com')
        self.assertEqual(user['lastUpdate'], user_orig['lastUpdate'])

    def test_set_user_info_nonexistent_user(self):
        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('ac-user-set-info', username='admin',
                          name='Admin Name', email='admin@admin.com')

        self.assertEqual(ctx.exception.retcode, -errno.ENOENT)
        self.assertEqual(str(ctx.exception), "User 'admin' does not exist")

    def test_set_user_password(self):
        user_orig = self.test_create_user()
        user = self.exec_cmd('ac-user-set-password', username='admin',
                             inbuf='newpass', force_password=True)
        pass_hash = password_hash('newpass', user['password'])
        self.assertDictEqual(user, {
            'username': 'admin',
            'password': pass_hash,
            'pwdExpirationDate': None,
            'pwdUpdateRequired': False,
            'name': 'admin User',
            'email': 'admin@user.com',
            'lastUpdate': user['lastUpdate'],
            'roles': [],
            'enabled': True
        })
        self.validate_persistent_user('admin', [], pass_hash, 'admin User',
                                      'admin@user.com')
        self.assertGreaterEqual(user['lastUpdate'], user_orig['lastUpdate'])

    def test_sanitize_password(self):
        self.test_create_user()
        password = 'myPass\\n\\r\\n'
        with open('/tmp/test_sanitize_password.txt', 'w+') as pwd_file:
            # Add new line separators (like some text editors when a file is saved).
            pwd_file.write('{}{}'.format(password, '\n\r\n\n'))
            pwd_file.seek(0)
            user = self.exec_cmd('ac-user-set-password', username='admin',
                                 inbuf=pwd_file.read(), force_password=True)
            pass_hash = password_hash(password, user['password'])
            self.assertEqual(user['password'], pass_hash)

    def test_set_user_password_nonexistent_user(self):
        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('ac-user-set-password', username='admin',
                          inbuf='newpass', force_password=True)

        self.assertEqual(ctx.exception.retcode, -errno.ENOENT)
        self.assertEqual(str(ctx.exception), "User 'admin' does not exist")

    def test_set_user_password_empty(self):
        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('ac-user-set-password', username='admin', inbuf='\n',
                          force_password=True)

        self.assertEqual(ctx.exception.retcode, -errno.EINVAL)
        self.assertIn(ERROR_MSG_EMPTY_INPUT_FILE, str(ctx.exception))

    def test_set_user_password_hash(self):
        user_orig = self.test_create_user()
        user = self.exec_cmd('ac-user-set-password-hash', username='admin',
                             inbuf='$2b$12$Pt3Vq/rDt2y9glTPSV.VFegiLkQeIpddtkhoFetNApYmIJOY8gau2')
        pass_hash = password_hash('newpass', user['password'])
        self.assertDictEqual(user, {
            'username': 'admin',
            'password': pass_hash,
            'pwdExpirationDate': None,
            'pwdUpdateRequired': False,
            'name': 'admin User',
            'email': 'admin@user.com',
            'lastUpdate': user['lastUpdate'],
            'roles': [],
            'enabled': True
        })
        self.validate_persistent_user('admin', [], pass_hash, 'admin User',
                                      'admin@user.com')
        self.assertGreaterEqual(user['lastUpdate'], user_orig['lastUpdate'])

    def test_set_user_password_hash_nonexistent_user(self):
        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('ac-user-set-password-hash', username='admin',
                          inbuf='$2b$12$Pt3Vq/rDt2y9glTPSV.VFegiLkQeIpddtkhoFetNApYmIJOY8gau2')

        self.assertEqual(ctx.exception.retcode, -errno.ENOENT)
        self.assertEqual(str(ctx.exception), "User 'admin' does not exist")

    def test_set_user_password_hash_broken_hash(self):
        self.test_create_user()
        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('ac-user-set-password-hash', username='admin',
                          inbuf='1')

        self.assertEqual(ctx.exception.retcode, -errno.EINVAL)
        self.assertEqual(str(ctx.exception), 'Invalid password hash')

    def test_set_login_credentials(self):
        self.exec_cmd('set-login-credentials', username='admin',
                      inbuf='admin')
        user = self.exec_cmd('ac-user-show', username='admin')
        pass_hash = password_hash('admin', user['password'])
        self.assertDictEqual(user, {
            'username': 'admin',
            'password': pass_hash,
            'pwdExpirationDate': None,
            'pwdUpdateRequired': False,
            'name': None,
            'email': None,
            'lastUpdate': user['lastUpdate'],
            'roles': ['administrator'],
            'enabled': True,
        })
        self.validate_persistent_user('admin', ['administrator'], pass_hash,
                                      None, None)

    def test_set_login_credentials_for_existing_user(self):
        self.test_add_user_roles('admin', ['read-only'])
        self.exec_cmd('set-login-credentials', username='admin',
                      inbuf='admin2')
        user = self.exec_cmd('ac-user-show', username='admin')
        pass_hash = password_hash('admin2', user['password'])
        self.assertDictEqual(user, {
            'username': 'admin',
            'password': pass_hash,
            'pwdExpirationDate': None,
            'pwdUpdateRequired': False,
            'name': 'admin User',
            'email': 'admin@user.com',
            'lastUpdate': user['lastUpdate'],
            'roles': ['read-only'],
            'enabled': True
        })
        self.validate_persistent_user('admin', ['read-only'], pass_hash,
                                      'admin User', 'admin@user.com')

    def test_load_v1(self):
        self.CONFIG_KEY_DICT['accessdb_v1'] = '''
            {{
                "users": {{
                    "admin": {{
                        "username": "admin",
                        "password":
                "$2b$12$sd0Az7mm3FaJl8kN3b/xwOuztaN0sWUwC1SJqjM4wcDw/s5cmGbLK",
                        "roles": ["block-manager", "test_role"],
                        "name": "admin User",
                        "email": "admin@user.com",
                        "lastUpdate": {}
                    }}
                }},
                "roles": {{
                    "test_role": {{
                        "name": "test_role",
                        "description": "Test Role",
                        "scopes_permissions": {{
                            "{}": ["{}", "{}"],
                            "{}": ["{}"]
                        }}
                    }}
                }},
                "version": 1
            }}
        '''.format(int(round(time.time())), Scope.ISCSI, Permission.READ,
                   Permission.UPDATE, Scope.POOL, Permission.CREATE)

        load_access_control_db()
        role = self.exec_cmd('ac-role-show', rolename="test_role")
        self.assertDictEqual(role, {
            'name': 'test_role',
            'description': "Test Role",
            'scopes_permissions': {
                Scope.ISCSI: [Permission.READ, Permission.UPDATE],
                Scope.POOL: [Permission.CREATE]
            }
        })
        user = self.exec_cmd('ac-user-show', username="admin")
        self.assertDictEqual(user, {
            'username': 'admin',
            'lastUpdate': user['lastUpdate'],
            'password':
                "$2b$12$sd0Az7mm3FaJl8kN3b/xwOuztaN0sWUwC1SJqjM4wcDw/s5cmGbLK",
            'pwdExpirationDate': None,
            'pwdUpdateRequired': False,
            'name': 'admin User',
            'email': 'admin@user.com',
            'roles': ['block-manager', 'test_role'],
            'enabled': True
        })

    def test_load_v2(self):
        self.CONFIG_KEY_DICT['accessdb_v2'] = '''
            {{
                "users": {{
                    "admin": {{
                        "username": "admin",
                        "password":
                "$2b$12$sd0Az7mm3FaJl8kN3b/xwOuztaN0sWUwC1SJqjM4wcDw/s5cmGbLK",
                        "pwdExpirationDate": null,
                        "pwdUpdateRequired": false,
                        "roles": ["block-manager", "test_role"],
                        "name": "admin User",
                        "email": "admin@user.com",
                        "lastUpdate": {},
                        "enabled": true
                    }}
                }},
                "roles": {{
                    "test_role": {{
                        "name": "test_role",
                        "description": "Test Role",
                        "scopes_permissions": {{
                            "{}": ["{}", "{}"],
                            "{}": ["{}"]
                        }}
                    }}
                }},
                "version": 2
            }}
        '''.format(int(round(time.time())), Scope.ISCSI, Permission.READ,
                   Permission.UPDATE, Scope.POOL, Permission.CREATE)

        load_access_control_db()
        role = self.exec_cmd('ac-role-show', rolename="test_role")
        self.assertDictEqual(role, {
            'name': 'test_role',
            'description': "Test Role",
            'scopes_permissions': {
                Scope.ISCSI: [Permission.READ, Permission.UPDATE],
                Scope.POOL: [Permission.CREATE]
            }
        })
        user = self.exec_cmd('ac-user-show', username="admin")
        self.assertDictEqual(user, {
            'username': 'admin',
            'lastUpdate': user['lastUpdate'],
            'password':
                "$2b$12$sd0Az7mm3FaJl8kN3b/xwOuztaN0sWUwC1SJqjM4wcDw/s5cmGbLK",
            'pwdExpirationDate': None,
            'pwdUpdateRequired': False,
            'name': 'admin User',
            'email': 'admin@user.com',
            'roles': ['block-manager', 'test_role'],
            'enabled': True
        })

    def test_password_policy_pw_length(self):
        Settings.PWD_POLICY_CHECK_LENGTH_ENABLED = True
        Settings.PWD_POLICY_MIN_LENGTH = 3
        pw_policy = PasswordPolicy('foo')
        self.assertTrue(pw_policy.check_password_length())

    def test_password_policy_pw_length_fail(self):
        Settings.PWD_POLICY_CHECK_LENGTH_ENABLED = True
        pw_policy = PasswordPolicy('bar')
        self.assertFalse(pw_policy.check_password_length())

    def test_password_policy_credits_too_weak(self):
        Settings.PWD_POLICY_CHECK_COMPLEXITY_ENABLED = True
        pw_policy = PasswordPolicy('foo')
        pw_credits = pw_policy.check_password_complexity()
        self.assertEqual(pw_credits, 3)

    def test_password_policy_credits_weak(self):
        Settings.PWD_POLICY_CHECK_COMPLEXITY_ENABLED = True
        pw_policy = PasswordPolicy('mypassword1')
        pw_credits = pw_policy.check_password_complexity()
        self.assertEqual(pw_credits, 11)

    def test_password_policy_credits_ok(self):
        Settings.PWD_POLICY_CHECK_COMPLEXITY_ENABLED = True
        pw_policy = PasswordPolicy('mypassword1!@')
        pw_credits = pw_policy.check_password_complexity()
        self.assertEqual(pw_credits, 17)

    def test_password_policy_credits_strong(self):
        Settings.PWD_POLICY_CHECK_COMPLEXITY_ENABLED = True
        pw_policy = PasswordPolicy('testpassword0047!@')
        pw_credits = pw_policy.check_password_complexity()
        self.assertEqual(pw_credits, 22)

    def test_password_policy_credits_very_strong(self):
        Settings.PWD_POLICY_CHECK_COMPLEXITY_ENABLED = True
        pw_policy = PasswordPolicy('testpassword#!$!@$')
        pw_credits = pw_policy.check_password_complexity()
        self.assertEqual(pw_credits, 30)

    def test_password_policy_forbidden_words(self):
        Settings.PWD_POLICY_CHECK_EXCLUSION_LIST_ENABLED = True
        pw_policy = PasswordPolicy('!@$testdashboard#!$')
        self.assertTrue(pw_policy.check_if_contains_forbidden_words())

    def test_password_policy_forbidden_words_custom(self):
        Settings.PWD_POLICY_CHECK_EXCLUSION_LIST_ENABLED = True
        Settings.PWD_POLICY_EXCLUSION_LIST = 'foo,bar'
        pw_policy = PasswordPolicy('foo123bar')
        self.assertTrue(pw_policy.check_if_contains_forbidden_words())

    def test_password_policy_sequential_chars(self):
        Settings.PWD_POLICY_CHECK_SEQUENTIAL_CHARS_ENABLED = True
        pw_policy = PasswordPolicy('!@$test123#!$')
        self.assertTrue(pw_policy.check_if_sequential_characters())

    def test_password_policy_repetitive_chars(self):
        Settings.PWD_POLICY_CHECK_REPETITIVE_CHARS_ENABLED = True
        pw_policy = PasswordPolicy('!@$testfooo#!$')
        self.assertTrue(pw_policy.check_if_repetitive_characters())

    def test_password_policy_contain_username(self):
        Settings.PWD_POLICY_CHECK_USERNAME_ENABLED = True
        pw_policy = PasswordPolicy('%admin135)', 'admin')
        self.assertTrue(pw_policy.check_if_contains_username())

    def test_password_policy_is_old_pwd(self):
        Settings.PWD_POLICY_CHECK_OLDPWD_ENABLED = True
        pw_policy = PasswordPolicy('foo', old_password='foo')
        self.assertTrue(pw_policy.check_is_old_password())
