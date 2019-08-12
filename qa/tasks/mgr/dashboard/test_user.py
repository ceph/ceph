# -*- coding: utf-8 -*-

from __future__ import absolute_import

from .helper import DashboardTestCase


class UserTest(DashboardTestCase):

    @classmethod
    def _create_user(cls, username=None, password=None, name=None, email=None, roles=None):
        data = {}
        if username:
            data['username'] = username
        if password:
            data['password'] = password
        if name:
            data['name'] = name
        if email:
            data['email'] = email
        if roles:
            data['roles'] = roles
        cls._post("/api/user", data)

    def test_crud_user(self):
        self._create_user(username='user1',
                          password='mypassword',
                          name='My Name',
                          email='my@email.com',
                          roles=['administrator'])
        self.assertStatus(201)
        user = self.jsonBody()

        self._get('/api/user/user1')
        self.assertStatus(200)
        self.assertJsonBody({
            'username': 'user1',
            'name': 'My Name',
            'email': 'my@email.com',
            'roles': ['administrator'],
            'lastUpdate': user['lastUpdate']
        })

        self._put('/api/user/user1', {
            'name': 'My New Name',
            'email': 'mynew@email.com',
            'roles': ['block-manager'],
        })
        self.assertStatus(200)
        user = self.jsonBody()
        self.assertJsonBody({
            'username': 'user1',
            'name': 'My New Name',
            'email': 'mynew@email.com',
            'roles': ['block-manager'],
            'lastUpdate': user['lastUpdate']
        })

        self._delete('/api/user/user1')
        self.assertStatus(204)

    def test_crd_disabled_user(self):
        self._create_user(username='klara',
                          password='123456789',
                          name='Klara Musterfrau',
                          email='klara@musterfrau.com',
                          roles=['administrator'],
                          enabled=False)
        self.assertStatus(201)
        user = self.jsonBody()

        # Restart dashboard module.
        self._unload_module('dashboard')
        self._load_module('dashboard')

        self._get('/api/user/klara')
        self.assertStatus(200)
        self.assertJsonBody({
            'username': 'klara',
            'name': 'Klara Musterfrau',
            'email': 'klara@musterfrau.com',
            'roles': ['administrator'],
            'lastUpdate': user['lastUpdate'],
            'enabled': False
        })

        self._delete('/api/user/klara')
        self.assertStatus(204)

    def test_list_users(self):
        self._get('/api/user')
        self.assertStatus(200)
        user = self.jsonBody()
        self.assertEqual(len(user), 1)
        user = user[0]
        self.assertJsonBody([{
            'username': 'admin',
            'name': None,
            'email': None,
            'roles': ['administrator'],
            'lastUpdate': user['lastUpdate']
        }])

    def test_create_user_already_exists(self):
        self._create_user(username='admin',
                          password='mypassword',
                          name='administrator',
                          email='my@email.com',
                          roles=['administrator'])
        self.assertStatus(400)
        self.assertError(code='username_already_exists',
                         component='user')

    def test_create_user_invalid_role(self):
        self._create_user(username='user1',
                          password='mypassword',
                          name='My Name',
                          email='my@email.com',
                          roles=['invalid-role'])
        self.assertStatus(400)
        self.assertError(code='role_does_not_exist',
                         component='user')

    def test_delete_user_does_not_exist(self):
        self._delete('/api/user/user2')
        self.assertStatus(404)

    @DashboardTestCase.RunAs('test', 'test', [{'user': ['create', 'read', 'update', 'delete']}])
    def test_delete_current_user(self):
        self._delete('/api/user/test')
        self.assertStatus(400)
        self.assertError(code='cannot_delete_current_user',
                         component='user')

    def test_update_user_does_not_exist(self):
        self._put('/api/user/user2', {'name': 'My New Name'})
        self.assertStatus(404)

    def test_update_user_invalid_role(self):
        self._put('/api/user/admin', {'roles': ['invalid-role']})
        self.assertStatus(400)
        self.assertError(code='role_does_not_exist',
                         component='user')
