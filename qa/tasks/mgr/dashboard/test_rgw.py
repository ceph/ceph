# -*- coding: utf-8 -*-
from __future__ import absolute_import
import urllib
import logging
logger = logging.getLogger(__name__)

from .helper import DashboardTestCase, authenticate


class RgwTestCase(DashboardTestCase):

    maxDiff = None
    create_test_user = False

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
                'user', 'create', '--uid', 'teuth-test-user',
                '--display-name', 'teuth-test-user'
            ])
            cls._radosgw_admin_cmd([
                'caps', 'add', '--uid', 'teuth-test-user',
                '--caps', 'metadata=write'
            ])
            cls._radosgw_admin_cmd([
                'subuser', 'create', '--uid', 'teuth-test-user',
                '--subuser', 'teuth-test-subuser', '--access',
                'full', '--key-type', 's3', '--access-key',
                'xyz123'
            ])
            cls._radosgw_admin_cmd([
                'subuser', 'create', '--uid', 'teuth-test-user',
                '--subuser', 'teuth-test-subuser2', '--access',
                'full', '--key-type', 'swift'
            ])

    @classmethod
    def tearDownClass(cls):
        if cls.create_test_user:
            cls._radosgw_admin_cmd(['user', 'rm', '--uid=teuth-test-user'])
        super(DashboardTestCase, cls).tearDownClass()

    def get_rgw_user(self, uid):
        return self._get('/api/rgw/user/{}'.format(uid))

    def find_in_list(self, key, value, data):
        """
        Helper function to find an object with the specified key/value
        in a list.
        :param key: The name of the key.
        :param value: The value to search for.
        :param data: The list to process.
        :return: Returns the found object or None.
        """
        return next(iter(filter(lambda x: x[key] == value, data)), None)


class RgwApiCredentialsTest(RgwTestCase):

    def setUp(self):
        # Restart the Dashboard module to ensure that the connection to the
        # RGW Admin Ops API is re-established with the new credentials.
        self._ceph_cmd(['mgr', 'module', 'disable', 'dashboard'])
        self._ceph_cmd(['mgr', 'module', 'enable', 'dashboard', '--force'])
        # Set the default credentials.
        self._ceph_cmd(['dashboard', 'set-rgw-api-user-id', ''])
        self._ceph_cmd(['dashboard', 'set-rgw-api-secret-key', 'admin'])
        self._ceph_cmd(['dashboard', 'set-rgw-api-access-key', 'admin'])

    @authenticate
    def test_no_access_secret_key(self):
        self._ceph_cmd(['dashboard', 'set-rgw-api-secret-key', ''])
        self._ceph_cmd(['dashboard', 'set-rgw-api-access-key', ''])
        resp = self._get('/api/rgw/user')
        self.assertStatus(500)
        self.assertIn('detail', resp)
        self.assertIn('component', resp)
        self.assertIn('No RGW credentials found', resp['detail'])
        self.assertEquals(resp['component'], 'rgw')

    @authenticate
    def test_success(self):
        data = self._get('/api/rgw/status')
        self.assertStatus(200)
        self.assertIn('available', data)
        self.assertIn('message', data)
        self.assertTrue(data['available'])

    @authenticate
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

    @classmethod
    def setUpClass(cls):
        cls.create_test_user = True
        super(RgwBucketTest, cls).setUpClass()

    @authenticate
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
        self.assertIn('bucket_info', data)
        data = data['bucket_info']
        self.assertIn('bucket', data)
        self.assertIn('quota', data)
        self.assertIn('creation_time', data)
        self.assertIn('name', data['bucket'])
        self.assertIn('bucket_id', data['bucket'])
        self.assertEquals(data['bucket']['name'], 'teuth-test-bucket')

        # List all buckets.
        data = self._get('/api/rgw/bucket')
        self.assertStatus(200)
        self.assertEqual(len(data), 1)
        self.assertIn('teuth-test-bucket', data)

        # Get the bucket.
        data = self._get('/api/rgw/bucket/teuth-test-bucket')
        self.assertStatus(200)
        self.assertIn('id', data)
        self.assertIn('bucket', data)
        self.assertIn('bucket_quota', data)
        self.assertIn('owner', data)
        self.assertEquals(data['bucket'], 'teuth-test-bucket')
        self.assertEquals(data['owner'], 'admin')

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
        self.assertEquals(data['owner'], 'teuth-test-user')

        # Delete the bucket.
        self._delete('/api/rgw/bucket/teuth-test-bucket')
        self.assertStatus(204)
        data = self._get('/api/rgw/bucket')
        self.assertStatus(200)
        self.assertEqual(len(data), 0)


class RgwDaemonTest(DashboardTestCase):

    @authenticate
    def test_list(self):
        data = self._get('/api/rgw/daemon')
        self.assertStatus(200)
        self.assertEqual(len(data), 1)
        data = data[0]
        self.assertIn('id', data)
        self.assertIn('version', data)
        self.assertIn('server_hostname', data)

    @authenticate
    def test_get(self):
        data = self._get('/api/rgw/daemon')
        self.assertStatus(200)

        data = self._get('/api/rgw/daemon/{}'.format(data[0]['id']))
        self.assertStatus(200)
        self.assertIn('rgw_metadata', data)
        self.assertIn('rgw_id', data)
        self.assertIn('rgw_status', data)
        self.assertTrue(data['rgw_metadata'])

    @authenticate
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

    @classmethod
    def setUpClass(cls):
        super(RgwUserTest, cls).setUpClass()

    def _assert_user_data(self, data):
        self.assertIn('caps', data)
        self.assertIn('display_name', data)
        self.assertIn('email', data)
        self.assertIn('keys', data)
        self.assertGreaterEqual(len(data['keys']), 1)
        self.assertIn('max_buckets', data)
        self.assertIn('subusers', data)
        self.assertIn('suspended', data)
        self.assertIn('swift_keys', data)
        self.assertIn('tenant', data)
        self.assertIn('user_id', data)

    @authenticate
    def test_get(self):
        data = self.get_rgw_user('admin')
        self.assertStatus(200)
        self._assert_user_data(data)
        self.assertEquals(data['user_id'], 'admin')

    @authenticate
    def test_list(self):
        data = self._get('/api/rgw/user')
        self.assertStatus(200)
        self.assertGreaterEqual(len(data), 1)
        self.assertIn('admin', data)

    @authenticate
    def test_create_update_delete(self):
        # Create a new user.
        self._post('/api/rgw/user', params={
            'uid': 'teuth-test-user',
            'display_name': 'display name'
        })
        self.assertStatus(201)
        data = self.jsonBody()
        self._assert_user_data(data)
        self.assertEquals(data['user_id'], 'teuth-test-user')
        self.assertEquals(data['display_name'], 'display name')

        # Get the user.
        data = self.get_rgw_user('teuth-test-user')
        self.assertStatus(200)
        self._assert_user_data(data)
        self.assertEquals(data['user_id'], 'teuth-test-user')

        # Update the user.
        self._put(
            '/api/rgw/user/teuth-test-user',
            params={
                'display_name': 'new name'
            })
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


class RgwUserCapabilityTest(RgwTestCase):

    @classmethod
    def setUpClass(cls):
        cls.create_test_user = True
        super(RgwUserCapabilityTest, cls).setUpClass()

    @authenticate
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

    @authenticate
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

    @classmethod
    def setUpClass(cls):
        cls.create_test_user = True
        super(RgwUserKeyTest, cls).setUpClass()

    @authenticate
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
        key = self.find_in_list('access_key', 'abc987', data)
        self.assertIsInstance(key, object)
        self.assertEqual(key['secret_key'], 'aaabbbccc')

    @authenticate
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
        key = self.find_in_list('secret_key', 'xxxyyyzzz', data)
        self.assertIsInstance(key, object)

    @authenticate
    def test_delete_s3(self):
        self._delete(
            '/api/rgw/user/teuth-test-user/key',
            params={
                'key_type': 's3',
                'access_key': 'xyz123'
            })
        self.assertStatus(204)

    @authenticate
    def test_delete_swift(self):
        self._delete(
            '/api/rgw/user/teuth-test-user/key',
            params={
                'key_type': 'swift',
                'subuser': 'teuth-test-user:teuth-test-subuser2'
            })
        self.assertStatus(204)


class RgwUserQuotaTest(RgwTestCase):

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

    @authenticate
    def test_get_quota(self):
        data = self._get('/api/rgw/user/teuth-test-user/quota')
        self.assertStatus(200)
        self._assert_quota(data)

    @authenticate
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

    @authenticate
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

    @classmethod
    def setUpClass(cls):
        cls.create_test_user = True
        super(RgwUserSubuserTest, cls).setUpClass()

    @authenticate
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
        subuser = self.find_in_list('id', 'teuth-test-user:tux', data)
        self.assertIsInstance(subuser, object)
        self.assertEqual(subuser['permissions'], 'read-write')

        # Get the user data to validate the keys.
        data = self.get_rgw_user('teuth-test-user')
        self.assertStatus(200)
        key = self.find_in_list('user', 'teuth-test-user:tux', data['swift_keys'])
        self.assertIsInstance(key, object)

    @authenticate
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
        subuser = self.find_in_list('id', 'teuth-test-user:hugo', data)
        self.assertIsInstance(subuser, object)
        self.assertEqual(subuser['permissions'], 'write')

        # Get the user data to validate the keys.
        data = self.get_rgw_user('teuth-test-user')
        self.assertStatus(200)
        key = self.find_in_list('user', 'teuth-test-user:hugo', data['keys'])
        self.assertIsInstance(key, object)
        self.assertEqual(key['secret_key'], 'xxx')

    @authenticate
    def test_delete_w_purge(self):
        self._delete(
            '/api/rgw/user/teuth-test-user/subuser/teuth-test-subuser2')
        self.assertStatus(204)

        # Get the user data to check that the keys don't exist anymore.
        data = self.get_rgw_user('teuth-test-user')
        self.assertStatus(200)
        key = self.find_in_list('user', 'teuth-test-user:teuth-test-subuser2',
                                data['swift_keys'])
        self.assertIsNone(key)

    @authenticate
    def test_delete_wo_purge(self):
        self._delete(
            '/api/rgw/user/teuth-test-user/subuser/teuth-test-subuser',
            params={'purge_keys': 'false'})
        self.assertStatus(204)

        # Get the user data to check whether they keys still exist.
        data = self.get_rgw_user('teuth-test-user')
        self.assertStatus(200)
        key = self.find_in_list('user', 'teuth-test-user:teuth-test-subuser',
                                data['keys'])
        self.assertIsInstance(key, object)
