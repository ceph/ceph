# -*- coding: utf-8 -*-
from __future__ import absolute_import
import urllib
import logging
logger = logging.getLogger(__name__)

from .helper import DashboardTestCase, authenticate, JObj, JList, JLeaf


class RgwControllerTest(DashboardTestCase):
    @authenticate
    def test_rgw_daemon_list(self):
        data = self._get('/api/rgw/daemon')
        self.assertStatus(200)

        self.assertEqual(len(data), 1)
        data = data[0]
        self.assertIn('id', data)
        self.assertIn('version', data)
        self.assertIn('server_hostname', data)

    @authenticate
    def test_rgw_daemon_get(self):
        data = self._get('/api/rgw/daemon')
        self.assertStatus(200)
        data = self._get('/api/rgw/daemon/{}'.format(data[0]['id']))
        self.assertStatus(200)

        self.assertIn('rgw_metadata', data)
        self.assertIn('rgw_id', data)
        self.assertIn('rgw_status', data)
        self.assertTrue(data['rgw_metadata'])

    @authenticate
    def test_rgw_status(self):
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


class RgwProxyExceptionsTest(DashboardTestCase):

    @classmethod
    def setUpClass(cls):
        super(RgwProxyExceptionsTest, cls).setUpClass()
        cls._ceph_cmd(['dashboard', 'set-rgw-api-secret-key', ''])
        cls._ceph_cmd(['dashboard', 'set-rgw-api-access-key', ''])

    @authenticate
    def test_no_credentials_exception(self):
        resp = self._get('/api/rgw/proxy/status')
        self.assertStatus(500)
        self.assertIn('detail', resp)


class RgwProxyTest(DashboardTestCase):

    @classmethod
    def setUpClass(cls):
        super(RgwProxyTest, cls).setUpClass()
        cls._radosgw_admin_cmd([
            'user', 'create', '--uid=admin', '--display-name=admin',
            '--system', '--access-key=admin', '--secret=admin'
        ])
        cls._ceph_cmd(['dashboard', 'set-rgw-api-secret-key', 'admin'])
        cls._ceph_cmd(['dashboard', 'set-rgw-api-access-key', 'admin'])

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

    def _test_put(self):
        self._put(
            '/api/rgw/proxy/user',
            params={
                'uid': 'teuth-test-user',
                'display-name': 'display name',
            })
        data = self._resp.json()

        self._assert_user_data(data)
        self.assertStatus(200)

        data = self._get(
            '/api/rgw/proxy/user',
            params={
                'uid': 'teuth-test-user'
            })

        self.assertStatus(200)
        self.assertEqual(data['user_id'], 'teuth-test-user')

    def _test_get(self):
        data = self._get(
            '/api/rgw/proxy/user',
            params={
                'uid': 'teuth-test-user'
            })

        self._assert_user_data(data)
        self.assertStatus(200)
        self.assertEquals(data['user_id'], 'teuth-test-user')

    def _test_post(self):
        """Updates the user"""
        self._post(
            '/api/rgw/proxy/user',
            params={
                'uid': 'teuth-test-user',
                'display-name': 'new name'
            })

        self.assertStatus(200)
        self._assert_user_data(self._resp.json())
        self.assertEqual(self._resp.json()['display_name'], 'new name')

    def _test_delete(self):
        self._delete('/api/rgw/user/teuth-test-user')
        self.assertStatus(204)
        self._delete('/api/rgw/user/teuth-test-user')
        self.assertStatus(500)
        resp = self._resp.json()
        self.assertIn('detail', resp)
        self.assertIn('failed request with status code 404', resp['detail'])
        self.assertIn('"Code":"NoSuchUser"', resp['detail'])
        self.assertIn('"HostId"', resp['detail'])
        self.assertIn('"RequestId"', resp['detail'])

    @authenticate
    def test_user_list(self):
        data = self._get('/api/rgw/proxy/metadata/user')
        self.assertStatus(200)
        self.assertGreaterEqual(len(data), 1)
        self.assertIn('admin', data)

    @authenticate
    def test_rgw_proxy(self):
        """Test basic request types"""
        self.maxDiff = None

        # PUT - Create a user
        self._test_put()

        # GET - Get the user details
        self._test_get()

        # POST - Update the user details
        self._test_post()

        # DELETE - Delete the user
        self._test_delete()


class RgwTestCase(DashboardTestCase):

    maxDiff = None
    create_test_user = False

    @classmethod
    def setUpClass(cls):
        super(RgwTestCase, cls).setUpClass()
        # Create the administrator account.
        cls._radosgw_admin_cmd([
            'user', 'create', '--uid=admin', '--display-name=admin',
            '--system', '--access-key=admin', '--secret=admin'
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


class RgwBucketTest(RgwTestCase):

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
        data = self._get('/api/rgw/proxy/bucket')
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
            '/api/rgw/proxy/bucket',
            params={
                'bucket': 'teuth-test-bucket',
                'bucket-id': data['id'],
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
        self._delete('/api/rgw/proxy/bucket', params={
            'bucket': 'teuth-test-bucket',
            'purge-objects': 'true'
        })
        self.assertStatus(200)
        data = self._get('/api/rgw/proxy/bucket')
        self.assertStatus(200)
        self.assertEqual(len(data), 0)

    @authenticate
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
        data = self._get('/api/rgw/proxy/bucket')
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
        # Currently it is not possible to link such a bucket that is
        # owned by a tenanted user to a 'normal' user.
        # See http://tracker.ceph.com/issues/39991
        #self._put(
        #    '/api/rgw/proxy/bucket',
        #    params={
        #        'bucket': urllib.quote_plus('testx/teuth-test-bucket'),
        #        'bucket-id': data['id'],
        #        'uid': 'admin'
        #    })
        #self.assertStatus(200)
        #data = self._get('/api/rgw/bucket/{}'.format(
        #    urllib.quote_plus('testx/teuth-test-bucket')))
        #self.assertStatus(200)
        #self.assertIn('owner', data)
        #self.assertEqual(data['owner'], 'admin')

        # Delete the bucket.
        self._delete('/api/rgw/proxy/bucket', params={
            'bucket': urllib.quote_plus('testx/teuth-test-bucket'),
            'purge-objects': 'true'
        })
        self.assertStatus(200)
        data = self._get('/api/rgw/proxy/bucket')
        self.assertStatus(200)
        self.assertEqual(len(data), 0)


class RgwUserTest(RgwTestCase):

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
            'user_id': JLeaf(str)
        }, allow_unknown=True))
        self.assertGreaterEqual(len(data['keys']), 1)

    @authenticate
    def test_get(self):
        data = self._get('/api/rgw/user/admin')
        self.assertStatus(200)
        self._assert_user_data(data)
        self.assertIn('uid', data)
        self.assertEqual(data['user_id'], 'admin')

    @authenticate
    def test_create_get_update_delete(self):
        # Create a new user.
        self._put(
            '/api/rgw/proxy/user',
            params={
                'uid': 'teuth-test-user',
                'display-name': 'display name'
            })
        self.assertStatus(200)
        data = self.jsonBody()
        self._assert_user_data(data)
        self.assertEqual(data['user_id'], 'teuth-test-user')
        self.assertEqual(data['display_name'], 'display name')

        # Get the user.
        data = self._get('/api/rgw/user/teuth-test-user')
        self.assertStatus(200)
        self._assert_user_data(data)
        self.assertEqual(data['tenant'], '')
        self.assertEqual(data['user_id'], 'teuth-test-user')
        self.assertEqual(data['uid'], 'teuth-test-user')

        # Update the user.
        self._post(
            '/api/rgw/proxy/user',
            params={
                'uid': 'teuth-test-user',
                'display-name': 'new name',
            })
        self.assertStatus(200)
        data = self.jsonBody()
        self._assert_user_data(data)
        self.assertEqual(data['display_name'], 'new name')

        # Delete the user.
        self._delete('/api/rgw/user/teuth-test-user')
        self.assertStatus(204)
        self._get('/api/rgw/user/teuth-test-user')
        self.assertStatus(500)
        resp = self.jsonBody()
        self.assertIn('detail', resp)
        self.assertIn('failed request with status code 404', resp['detail'])
        self.assertIn('"Code":"NoSuchUser"', resp['detail'])
        self.assertIn('"HostId"', resp['detail'])
        self.assertIn('"RequestId"', resp['detail'])

    @authenticate
    def test_create_get_update_delete_w_tenant(self):
        # Create a new user.
        self._put(
            '/api/rgw/proxy/user',
            params={
                'uid': 'test01$teuth-test-user',
                'display-name': 'display name'
            })
        self.assertStatus(200)
        data = self.jsonBody()
        self._assert_user_data(data)
        self.assertEqual(data['user_id'], 'teuth-test-user')
        self.assertEqual(data['display_name'], 'display name')

        # Get the user.
        data = self._get('/api/rgw/user/test01$teuth-test-user')
        self.assertStatus(200)
        self._assert_user_data(data)
        self.assertEqual(data['tenant'], 'test01')
        self.assertEqual(data['user_id'], 'teuth-test-user')
        self.assertEqual(data['uid'], 'test01$teuth-test-user')

        # Update the user.
        self._post(
            '/api/rgw/proxy/user',
            params={
                'uid': 'test01$teuth-test-user',
                'display-name': 'new name'
            })
        self.assertStatus(200)
        data = self.jsonBody()
        self._assert_user_data(data)
        self.assertEqual(data['display_name'], 'new name')

        # Delete the user.
        self._delete('/api/rgw/user/test01$teuth-test-user')
        self.assertStatus(204)
        self._get('/api/rgw/user/test01$teuth-test-user')
        self.assertStatus(500)
        resp = self.jsonBody()
        self.assertIn('detail', resp)
        self.assertIn('failed request with status code 404', resp['detail'])
        self.assertIn('"Code":"NoSuchUser"', resp['detail'])
        self.assertIn('"HostId"', resp['detail'])
        self.assertIn('"RequestId"', resp['detail'])
