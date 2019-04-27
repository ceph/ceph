# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging
import urllib
import six

from .helper import DashboardTestCase, JObj, JList, JLeaf

logger = logging.getLogger(__name__)


class RgwTestCase(DashboardTestCase):

    maxDiff = None
    create_test_user = False

    AUTH_ROLES = ['rgw-manager']

    @classmethod
    def setUpClass(cls):
        super(RgwTestCase, cls).setUpClass()
        # Create the administrator account.
        cls._radosgw_admin_cmd([
            'user', 'create', '--uid', 'admin', '--display-name', 'admin',
            '--system', '--access-key', 'admin', '--secret', 'admin'
        ])
        # Update the dashboard configuration.
        cls._ceph_cmd(['dashboard', 'set-rgw-api-secret-key', 'admin'])
        cls._ceph_cmd(['dashboard', 'set-rgw-api-access-key', 'admin'])
        # Create a test user?
        if cls.create_test_user:
            cls._radosgw_admin_cmd([
                'user', 'create', '--uid', 'teuth-test-user', '--display-name',
                'teuth-test-user'
            ])
            cls._radosgw_admin_cmd([
                'caps', 'add', '--uid', 'teuth-test-user', '--caps',
                'metadata=write'
            ])
            cls._radosgw_admin_cmd([
                'subuser', 'create', '--uid', 'teuth-test-user', '--subuser',
                'teuth-test-subuser', '--access', 'full', '--key-type', 's3',
                '--access-key', 'xyz123'
            ])
            cls._radosgw_admin_cmd([
                'subuser', 'create', '--uid', 'teuth-test-user', '--subuser',
                'teuth-test-subuser2', '--access', 'full', '--key-type',
                'swift'
            ])

    @classmethod
    def tearDownClass(cls):
        if cls.create_test_user:
            cls._radosgw_admin_cmd(['user', 'rm', '--uid=teuth-test-user'])
        super(RgwTestCase, cls).tearDownClass()

    def setUp(self):
        super(RgwTestCase, self).setUp()

    def get_rgw_user(self, uid):
        return self._get('/api/rgw/user/{}'.format(uid))


class RgwApiCredentialsTest(RgwTestCase):

    AUTH_ROLES = ['rgw-manager']

    def setUp(self):
        # Restart the Dashboard module to ensure that the connection to the
        # RGW Admin Ops API is re-established with the new credentials.
        self.logout()
        self._ceph_cmd(['mgr', 'module', 'disable', 'dashboard'])
        self._ceph_cmd(['mgr', 'module', 'enable', 'dashboard', '--force'])
        # Set the default credentials.
        self._ceph_cmd(['dashboard', 'set-rgw-api-user-id', ''])
        self._ceph_cmd(['dashboard', 'set-rgw-api-secret-key', 'admin'])
        self._ceph_cmd(['dashboard', 'set-rgw-api-access-key', 'admin'])
        super(RgwApiCredentialsTest, self).setUp()

    def test_no_access_secret_key(self):
        self._ceph_cmd(['dashboard', 'set-rgw-api-secret-key', ''])
        self._ceph_cmd(['dashboard', 'set-rgw-api-access-key', ''])
        resp = self._get('/api/rgw/user')
        self.assertStatus(500)
        self.assertIn('detail', resp)
        self.assertIn('component', resp)
        self.assertIn('No RGW credentials found', resp['detail'])
        self.assertEqual(resp['component'], 'rgw')

    def test_success(self):
        data = self._get('/api/rgw/status')
        self.assertStatus(200)
        self.assertIn('available', data)
        self.assertIn('message', data)
        self.assertTrue(data['available'])

    def test_invalid_user_id(self):
        self._ceph_cmd(['dashboard', 'set-rgw-api-user-id', 'xyz'])
        data = self._get('/api/rgw/status')
        self.assertStatus(200)
        self.assertIn('available', data)
        self.assertIn('message', data)
        self.assertFalse(data['available'])
        self.assertIn('The user "xyz" is unknown to the Object Gateway.',
                      data['message'])


class RgwBucketTest(RgwTestCase):

    AUTH_ROLES = ['rgw-manager']

    @classmethod
    def setUpClass(cls):
        cls.create_test_user = True
        super(RgwBucketTest, cls).setUpClass()
        # Create a tenanted user.
        cls._radosgw_admin_cmd([
            'user', 'create', '--tenant', 'testx', '--uid', 'teuth-test-user',
            '--display-name', 'tenanted teuth-test-user'
        ])

    @classmethod
    def tearDownClass(cls):
        cls._radosgw_admin_cmd(
            ['user', 'rm', '--tenant', 'testx', '--uid=teuth-test-user'])
        super(RgwBucketTest, cls).tearDownClass()

    def test_all(self):
        # Create a new bucket.
        self._post(
            '/api/rgw/bucket',
            params={
                'bucket': 'teuth-test-bucket',
                'uid': 'admin'
            })
        self.assertStatus(201)
        data = self.jsonBody()
        self.assertSchema(data, JObj(sub_elems={
            'bucket_info': JObj(sub_elems={
                'bucket': JObj(allow_unknown=True, sub_elems={
                    'name': JLeaf(str),
                    'bucket_id': JLeaf(str),
                    'tenant': JLeaf(str)
                }),
                'quota': JObj(sub_elems={}, allow_unknown=True),
                'creation_time': JLeaf(str)
            }, allow_unknown=True)
        }, allow_unknown=True))
        data = data['bucket_info']['bucket']
        self.assertEqual(data['name'], 'teuth-test-bucket')
        self.assertEqual(data['tenant'], '')

        # List all buckets.
        data = self._get('/api/rgw/bucket')
        self.assertStatus(200)
        self.assertEqual(len(data), 1)
        self.assertIn('teuth-test-bucket', data)

        # Get the bucket.
        data = self._get('/api/rgw/bucket/teuth-test-bucket')
        self.assertStatus(200)
        self.assertSchema(data, JObj(sub_elems={
            'id': JLeaf(str),
            'bid': JLeaf(str),
            'tenant': JLeaf(str),
            'bucket': JLeaf(str),
            'bucket_quota': JObj(sub_elems={}, allow_unknown=True),
            'owner': JLeaf(str)
        }, allow_unknown=True))
        self.assertEqual(data['bucket'], 'teuth-test-bucket')
        self.assertEqual(data['owner'], 'admin')

        # Update the bucket.
        self._put(
            '/api/rgw/bucket/teuth-test-bucket',
            params={
                'bucket_id': data['id'],
                'uid': 'teuth-test-user'
            })
        self.assertStatus(200)
        data = self._get('/api/rgw/bucket/teuth-test-bucket')
        self.assertStatus(200)
        self.assertSchema(data, JObj(sub_elems={
            'owner': JLeaf(str),
            'bid': JLeaf(str),
            'tenant': JLeaf(str)
        }, allow_unknown=True))
        self.assertEqual(data['owner'], 'teuth-test-user')

        # Delete the bucket.
        self._delete('/api/rgw/bucket/teuth-test-bucket')
        self.assertStatus(204)
        data = self._get('/api/rgw/bucket')
        self.assertStatus(200)
        self.assertEqual(len(data), 0)

    def test_create_get_update_delete_w_tenant(self):
        # Create a new bucket. The tenant of the user is used when
        # the bucket is created.
        self._post(
            '/api/rgw/bucket',
            params={
                'bucket': 'teuth-test-bucket',
                'uid': 'testx$teuth-test-user'
            })
        self.assertStatus(201)
        # It's not possible to validate the result because there
        # IS NO result object returned by the RGW Admin OPS API
        # when a tenanted bucket is created.
        data = self.jsonBody()
        self.assertIsNone(data)

        # List all buckets.
        data = self._get('/api/rgw/bucket')
        self.assertStatus(200)
        self.assertEqual(len(data), 1)
        self.assertIn('testx/teuth-test-bucket', data)

        # Get the bucket.
        data = self._get('/api/rgw/bucket/{}'.format(
            urllib.quote_plus('testx/teuth-test-bucket')))
        self.assertStatus(200)
        self.assertSchema(data, JObj(sub_elems={
            'owner': JLeaf(str),
            'bucket': JLeaf(str),
            'tenant': JLeaf(str),
            'bid': JLeaf(str)
        }, allow_unknown=True))
        self.assertEqual(data['owner'], 'testx$teuth-test-user')
        self.assertEqual(data['bucket'], 'teuth-test-bucket')
        self.assertEqual(data['tenant'], 'testx')
        self.assertEqual(data['bid'], 'testx/teuth-test-bucket')

        # Update the bucket.
        self._put(
            '/api/rgw/bucket/{}'.format(
                urllib.quote_plus('testx/teuth-test-bucket')),
            params={
                'bucket_id': data['id'],
                'uid': 'admin'
            })
        self.assertStatus(200)
        data = self._get('/api/rgw/bucket/{}'.format(
            urllib.quote_plus('testx/teuth-test-bucket')))
        self.assertStatus(200)
        self.assertIn('owner', data)
        self.assertEqual(data['owner'], 'admin')

        # Delete the bucket.
        self._delete('/api/rgw/bucket/{}'.format(
            urllib.quote_plus('testx/teuth-test-bucket')))
        self.assertStatus(204)
        data = self._get('/api/rgw/bucket')
        self.assertStatus(200)
        self.assertEqual(len(data), 0)


class RgwDaemonTest(DashboardTestCase):

    AUTH_ROLES = ['rgw-manager']

    @DashboardTestCase.RunAs('test', 'test', [{
        'rgw': ['create', 'update', 'delete']
    }])
    def test_read_access_permissions(self):
        self._get('/api/rgw/daemon')
        self.assertStatus(403)
        self._get('/api/rgw/daemon/id')
        self.assertStatus(403)

    def test_list(self):
        data = self._get('/api/rgw/daemon')
        self.assertStatus(200)
        self.assertEqual(len(data), 1)
        data = data[0]
        self.assertIn('id', data)
        self.assertIn('version', data)
        self.assertIn('server_hostname', data)

    def test_get(self):
        data = self._get('/api/rgw/daemon')
        self.assertStatus(200)

        data = self._get('/api/rgw/daemon/{}'.format(data[0]['id']))
        self.assertStatus(200)
        self.assertIn('rgw_metadata', data)
        self.assertIn('rgw_id', data)
        self.assertIn('rgw_status', data)
        self.assertTrue(data['rgw_metadata'])

    def test_status(self):
        self._radosgw_admin_cmd([
            'user', 'create', '--uid=admin', '--display-name=admin',
            '--system', '--access-key=admin', '--secret=admin'
        ])
        self._ceph_cmd(['dashboard', 'set-rgw-api-user-id', 'admin'])
        self._ceph_cmd(['dashboard', 'set-rgw-api-secret-key', 'admin'])
        self._ceph_cmd(['dashboard', 'set-rgw-api-access-key', 'admin'])

        data = self._get('/api/rgw/status')
        self.assertStatus(200)
        self.assertIn('available', data)
        self.assertIn('message', data)
        self.assertTrue(data['available'])


class RgwUserTest(RgwTestCase):

    AUTH_ROLES = ['rgw-manager']

    @classmethod
    def setUpClass(cls):
        super(RgwUserTest, cls).setUpClass()

    def _assert_user_data(self, data):
        self.assertSchema(data, JObj(sub_elems={
            'caps': JList(JObj(sub_elems={}, allow_unknown=True)),
            'display_name': JLeaf(str),
            'email': JLeaf(str),
            'keys': JList(JObj(sub_elems={}, allow_unknown=True)),
            'max_buckets': JLeaf(int),
            'subusers': JList(JLeaf(str)),
            'suspended': JLeaf(int),
            'swift_keys': JList(JObj(sub_elems={}, allow_unknown=True)),
            'tenant': JLeaf(str),
            'user_id': JLeaf(str),
            'uid': JLeaf(str)
        }, allow_unknown=True))
        self.assertGreaterEqual(len(data['keys']), 1)

    def test_get(self):
        data = self.get_rgw_user('admin')
        self.assertStatus(200)
        self._assert_user_data(data)
        self.assertEqual(data['user_id'], 'admin')

    def test_list(self):
        data = self._get('/api/rgw/user')
        self.assertStatus(200)
        self.assertGreaterEqual(len(data), 1)
        self.assertIn('admin', data)

    def test_create_get_update_delete(self):
        # Create a new user.
        self._post('/api/rgw/user', params={
            'uid': 'teuth-test-user',
            'display_name': 'display name'
        })
        self.assertStatus(201)
        data = self.jsonBody()
        self._assert_user_data(data)
        self.assertEqual(data['user_id'], 'teuth-test-user')
        self.assertEqual(data['display_name'], 'display name')

        # Get the user.
        data = self.get_rgw_user('teuth-test-user')
        self.assertStatus(200)
        self._assert_user_data(data)
        self.assertEqual(data['tenant'], '')
        self.assertEqual(data['user_id'], 'teuth-test-user')
        self.assertEqual(data['uid'], 'teuth-test-user')

        # Update the user.
        self._put(
            '/api/rgw/user/teuth-test-user',
            params={'display_name': 'new name'})
        self.assertStatus(200)
        data = self.jsonBody()
        self._assert_user_data(data)
        self.assertEqual(data['display_name'], 'new name')

        # Delete the user.
        self._delete('/api/rgw/user/teuth-test-user')
        self.assertStatus(204)
        self.get_rgw_user('teuth-test-user')
        self.assertStatus(500)
        resp = self.jsonBody()
        self.assertIn('detail', resp)
        self.assertIn('failed request with status code 404', resp['detail'])
        self.assertIn('"Code":"NoSuchUser"', resp['detail'])
        self.assertIn('"HostId"', resp['detail'])
        self.assertIn('"RequestId"', resp['detail'])

    def test_create_get_update_delete_w_tenant(self):
        # Create a new user.
        self._post(
            '/api/rgw/user',
            params={
                'uid': 'test01$teuth-test-user',
                'display_name': 'display name'
            })
        self.assertStatus(201)
        data = self.jsonBody()
        self._assert_user_data(data)
        self.assertEqual(data['user_id'], 'teuth-test-user')
        self.assertEqual(data['display_name'], 'display name')

        # Get the user.
        data = self.get_rgw_user('test01$teuth-test-user')
        self.assertStatus(200)
        self._assert_user_data(data)
        self.assertEqual(data['tenant'], 'test01')
        self.assertEqual(data['user_id'], 'teuth-test-user')
        self.assertEqual(data['uid'], 'test01$teuth-test-user')

        # Update the user.
        self._put(
            '/api/rgw/user/test01$teuth-test-user',
            params={'display_name': 'new name'})
        self.assertStatus(200)
        data = self.jsonBody()
        self._assert_user_data(data)
        self.assertEqual(data['display_name'], 'new name')

        # Delete the user.
        self._delete('/api/rgw/user/test01$teuth-test-user')
        self.assertStatus(204)
        self.get_rgw_user('test01$teuth-test-user')
        self.assertStatus(500)
        resp = self.jsonBody()
        self.assertIn('detail', resp)
        self.assertIn('failed request with status code 404', resp['detail'])
        self.assertIn('"Code":"NoSuchUser"', resp['detail'])
        self.assertIn('"HostId"', resp['detail'])
        self.assertIn('"RequestId"', resp['detail'])


class RgwUserCapabilityTest(RgwTestCase):

    AUTH_ROLES = ['rgw-manager']

    @classmethod
    def setUpClass(cls):
        cls.create_test_user = True
        super(RgwUserCapabilityTest, cls).setUpClass()

    def test_set(self):
        self._post(
            '/api/rgw/user/teuth-test-user/capability',
            params={
                'type': 'usage',
                'perm': 'read'
            })
        self.assertStatus(201)
        data = self.jsonBody()
        self.assertEqual(len(data), 1)
        data = data[0]
        self.assertEqual(data['type'], 'usage')
        self.assertEqual(data['perm'], 'read')

        # Get the user data to validate the capabilities.
        data = self.get_rgw_user('teuth-test-user')
        self.assertStatus(200)
        self.assertGreaterEqual(len(data['caps']), 1)
        self.assertEqual(data['caps'][0]['type'], 'usage')
        self.assertEqual(data['caps'][0]['perm'], 'read')

    def test_delete(self):
        self._delete(
            '/api/rgw/user/teuth-test-user/capability',
            params={
                'type': 'metadata',
                'perm': 'write'
            })
        self.assertStatus(204)

        # Get the user data to validate the capabilities.
        data = self.get_rgw_user('teuth-test-user')
        self.assertStatus(200)
        self.assertEqual(len(data['caps']), 0)


class RgwUserKeyTest(RgwTestCase):

    AUTH_ROLES = ['rgw-manager']

    @classmethod
    def setUpClass(cls):
        cls.create_test_user = True
        super(RgwUserKeyTest, cls).setUpClass()

    def test_create_s3(self):
        self._post(
            '/api/rgw/user/teuth-test-user/key',
            params={
                'key_type': 's3',
                'generate_key': 'false',
                'access_key': 'abc987',
                'secret_key': 'aaabbbccc'
            })
        data = self.jsonBody()
        self.assertStatus(201)
        self.assertGreaterEqual(len(data), 3)
        key = self.find_object_in_list('access_key', 'abc987', data)
        self.assertIsInstance(key, object)
        self.assertEqual(key['secret_key'], 'aaabbbccc')

    def test_create_swift(self):
        self._post(
            '/api/rgw/user/teuth-test-user/key',
            params={
                'key_type': 'swift',
                'subuser': 'teuth-test-subuser',
                'generate_key': 'false',
                'secret_key': 'xxxyyyzzz'
            })
        data = self.jsonBody()
        self.assertStatus(201)
        self.assertGreaterEqual(len(data), 2)
        key = self.find_object_in_list('secret_key', 'xxxyyyzzz', data)
        self.assertIsInstance(key, object)

    def test_delete_s3(self):
        self._delete(
            '/api/rgw/user/teuth-test-user/key',
            params={
                'key_type': 's3',
                'access_key': 'xyz123'
            })
        self.assertStatus(204)

    def test_delete_swift(self):
        self._delete(
            '/api/rgw/user/teuth-test-user/key',
            params={
                'key_type': 'swift',
                'subuser': 'teuth-test-user:teuth-test-subuser2'
            })
        self.assertStatus(204)


class RgwUserQuotaTest(RgwTestCase):

    AUTH_ROLES = ['rgw-manager']

    @classmethod
    def setUpClass(cls):
        cls.create_test_user = True
        super(RgwUserQuotaTest, cls).setUpClass()

    def _assert_quota(self, data):
        self.assertIn('user_quota', data)
        self.assertIn('max_objects', data['user_quota'])
        self.assertIn('enabled', data['user_quota'])
        self.assertIn('max_size_kb', data['user_quota'])
        self.assertIn('max_size', data['user_quota'])
        self.assertIn('bucket_quota', data)
        self.assertIn('max_objects', data['bucket_quota'])
        self.assertIn('enabled', data['bucket_quota'])
        self.assertIn('max_size_kb', data['bucket_quota'])
        self.assertIn('max_size', data['bucket_quota'])

    def test_get_quota(self):
        data = self._get('/api/rgw/user/teuth-test-user/quota')
        self.assertStatus(200)
        self._assert_quota(data)

    def test_set_user_quota(self):
        self._put(
            '/api/rgw/user/teuth-test-user/quota',
            params={
                'quota_type': 'user',
                'enabled': 'true',
                'max_size_kb': 2048,
                'max_objects': 101
            })
        self.assertStatus(200)

        data = self._get('/api/rgw/user/teuth-test-user/quota')
        self.assertStatus(200)
        self._assert_quota(data)
        self.assertEqual(data['user_quota']['max_objects'], 101)
        self.assertTrue(data['user_quota']['enabled'])
        self.assertEqual(data['user_quota']['max_size_kb'], 2048)

    def test_set_bucket_quota(self):
        self._put(
            '/api/rgw/user/teuth-test-user/quota',
            params={
                'quota_type': 'bucket',
                'enabled': 'false',
                'max_size_kb': 4096,
                'max_objects': 2000
            })
        self.assertStatus(200)

        data = self._get('/api/rgw/user/teuth-test-user/quota')
        self.assertStatus(200)
        self._assert_quota(data)
        self.assertEqual(data['bucket_quota']['max_objects'], 2000)
        self.assertFalse(data['bucket_quota']['enabled'])
        self.assertEqual(data['bucket_quota']['max_size_kb'], 4096)


class RgwUserSubuserTest(RgwTestCase):

    AUTH_ROLES = ['rgw-manager']

    @classmethod
    def setUpClass(cls):
        cls.create_test_user = True
        super(RgwUserSubuserTest, cls).setUpClass()

    def test_create_swift(self):
        self._post(
            '/api/rgw/user/teuth-test-user/subuser',
            params={
                'subuser': 'tux',
                'access': 'readwrite',
                'key_type': 'swift'
            })
        self.assertStatus(201)
        data = self.jsonBody()
        subuser = self.find_object_in_list('id', 'teuth-test-user:tux', data)
        self.assertIsInstance(subuser, object)
        self.assertEqual(subuser['permissions'], 'read-write')

        # Get the user data to validate the keys.
        data = self.get_rgw_user('teuth-test-user')
        self.assertStatus(200)
        key = self.find_object_in_list('user', 'teuth-test-user:tux',
                                       data['swift_keys'])
        self.assertIsInstance(key, object)

    def test_create_s3(self):
        self._post(
            '/api/rgw/user/teuth-test-user/subuser',
            params={
                'subuser': 'hugo',
                'access': 'write',
                'generate_secret': 'false',
                'access_key': 'yyy',
                'secret_key': 'xxx'
            })
        self.assertStatus(201)
        data = self.jsonBody()
        subuser = self.find_object_in_list('id', 'teuth-test-user:hugo', data)
        self.assertIsInstance(subuser, object)
        self.assertEqual(subuser['permissions'], 'write')

        # Get the user data to validate the keys.
        data = self.get_rgw_user('teuth-test-user')
        self.assertStatus(200)
        key = self.find_object_in_list('user', 'teuth-test-user:hugo',
                                       data['keys'])
        self.assertIsInstance(key, object)
        self.assertEqual(key['secret_key'], 'xxx')

    def test_delete_w_purge(self):
        self._delete(
            '/api/rgw/user/teuth-test-user/subuser/teuth-test-subuser2')
        self.assertStatus(204)

        # Get the user data to check that the keys don't exist anymore.
        data = self.get_rgw_user('teuth-test-user')
        self.assertStatus(200)
        key = self.find_object_in_list(
            'user', 'teuth-test-user:teuth-test-subuser2', data['swift_keys'])
        self.assertIsNone(key)

    def test_delete_wo_purge(self):
        self._delete(
            '/api/rgw/user/teuth-test-user/subuser/teuth-test-subuser',
            params={'purge_keys': 'false'})
        self.assertStatus(204)

        # Get the user data to check whether they keys still exist.
        data = self.get_rgw_user('teuth-test-user')
        self.assertStatus(200)
        key = self.find_object_in_list(
            'user', 'teuth-test-user:teuth-test-subuser', data['keys'])
        self.assertIsInstance(key, object)
