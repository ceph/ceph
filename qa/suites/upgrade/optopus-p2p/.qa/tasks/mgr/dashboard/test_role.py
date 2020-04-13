# -*- coding: utf-8 -*-

from __future__ import absolute_import

from .helper import DashboardTestCase


class RoleTest(DashboardTestCase):
    @classmethod
    def _create_role(cls, name=None, description=None, scopes_permissions=None):
        data = {}
        if name:
            data['name'] = name
        if description:
            data['description'] = description
        if scopes_permissions:
            data['scopes_permissions'] = scopes_permissions
        cls._post('/api/role', data)

    def test_crud_role(self):
        self._create_role(name='role1',
                          description='Description 1',
                          scopes_permissions={'osd': ['read']})
        self.assertStatus(201)
        self.assertJsonBody({
            'name': 'role1',
            'description': 'Description 1',
            'scopes_permissions': {'osd': ['read']},
            'system': False
        })

        self._get('/api/role/role1')
        self.assertStatus(200)
        self.assertJsonBody({
            'name': 'role1',
            'description': 'Description 1',
            'scopes_permissions': {'osd': ['read']},
            'system': False
        })

        self._put('/api/role/role1', {
            'description': 'Description 2',
            'scopes_permissions': {'osd': ['read', 'update']},
        })
        self.assertStatus(200)
        self.assertJsonBody({
            'name': 'role1',
            'description': 'Description 2',
            'scopes_permissions': {'osd': ['read', 'update']},
            'system': False
        })

        self._delete('/api/role/role1')
        self.assertStatus(204)

    def test_list_roles(self):
        roles = self._get('/api/role')
        self.assertStatus(200)

        self.assertGreaterEqual(len(roles), 1)
        for role in roles:
            self.assertIn('name', role)
            self.assertIn('description', role)
            self.assertIn('scopes_permissions', role)
            self.assertIn('system', role)

    def test_get_role_does_not_exist(self):
        self._get('/api/role/role2')
        self.assertStatus(404)

    def test_create_role_already_exists(self):
        self._create_role(name='read-only',
                          description='Description 1',
                          scopes_permissions={'osd': ['read']})
        self.assertStatus(400)
        self.assertError(code='role_already_exists',
                         component='role')

    def test_create_role_no_name(self):
        self._create_role(description='Description 1',
                          scopes_permissions={'osd': ['read']})
        self.assertStatus(400)
        self.assertError(code='name_required',
                         component='role')

    def test_create_role_invalid_scope(self):
        self._create_role(name='role1',
                          description='Description 1',
                          scopes_permissions={'invalid-scope': ['read']})
        self.assertStatus(400)
        self.assertError(code='invalid_scope',
                         component='role')

    def test_create_role_invalid_permission(self):
        self._create_role(name='role1',
                          description='Description 1',
                          scopes_permissions={'osd': ['invalid-permission']})
        self.assertStatus(400)
        self.assertError(code='invalid_permission',
                         component='role')

    def test_delete_role_does_not_exist(self):
        self._delete('/api/role/role2')
        self.assertStatus(404)

    def test_delete_system_role(self):
        self._delete('/api/role/read-only')
        self.assertStatus(400)
        self.assertError(code='cannot_delete_system_role',
                         component='role')

    def test_delete_role_associated_with_user(self):
        self.create_user("user", "user", ['read-only'])
        self._create_role(name='role1',
                          description='Description 1',
                          scopes_permissions={'user': ['create', 'read', 'update', 'delete']})
        self.assertStatus(201)
        self._put('/api/user/user', {'roles': ['role1']})
        self.assertStatus(200)

        self._delete('/api/role/role1')
        self.assertStatus(400)
        self.assertError(code='role_is_associated_with_user',
                         component='role')

        self._put('/api/user/user', {'roles': ['administrator']})
        self.assertStatus(200)
        self._delete('/api/role/role1')
        self.assertStatus(204)
        self.delete_user("user")

    def test_update_role_does_not_exist(self):
        self._put('/api/role/role2', {})
        self.assertStatus(404)

    def test_update_system_role(self):
        self._put('/api/role/read-only', {})
        self.assertStatus(400)
        self.assertError(code='cannot_update_system_role',
                         component='role')
