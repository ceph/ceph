# -*- coding: utf-8 -*-
from __future__ import absolute_import

import base64
import logging
import time
from urllib import parse

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.hashes import SHA1
from cryptography.hazmat.primitives.twofactor.totp import TOTP

from .helper import DashboardTestCase, JLeaf, JList, JObj

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
        cls._ceph_cmd_with_secret(['dashboard', 'set-rgw-api-secret-key'], 'admin')
        cls._ceph_cmd_with_secret(['dashboard', 'set-rgw-api-access-key'], 'admin')
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
        # Delete administrator account.
        cls._radosgw_admin_cmd(['user', 'rm', '--uid', 'admin'])
        if cls.create_test_user:
            cls._radosgw_admin_cmd(['user', 'rm', '--uid=teuth-test-user', '--purge-data'])
        super(RgwTestCase, cls).tearDownClass()

    def get_rgw_user(self, uid, stats=True):
        return self._get('/api/rgw/user/{}?stats={}'.format(uid, stats))


class RgwSiteTest(RgwTestCase):

    AUTH_ROLES = ['rgw-manager']

    def test_get_placement_targets(self):
        data = self._get('/api/rgw/site?query=placement-targets')
        self.assertStatus(200)
        self.assertSchema(data, JObj({
            'zonegroup': str,
            'placement_targets': JList(JObj({
                'name': str,
                'data_pool': str
            }))
        }))

    def test_get_realms(self):
        data = self._get('/api/rgw/site?query=realms')
        self.assertStatus(200)
        self.assertSchema(data, JList(str))


class RgwBucketTest(RgwTestCase):

    _mfa_token_serial = '1'
    _mfa_token_seed = '23456723'
    _mfa_token_time_step = 2

    AUTH_ROLES = ['rgw-manager']

    @classmethod
    def setUpClass(cls):
        cls.create_test_user = True
        super(RgwBucketTest, cls).setUpClass()
        # Create an MFA user
        cls._radosgw_admin_cmd([
            'user', 'create', '--uid', 'mfa-test-user', '--display-name', 'mfa-user',
            '--system', '--access-key', 'mfa-access', '--secret', 'mfa-secret'
        ])
        # Create MFA TOTP token for test user.
        cls._radosgw_admin_cmd([
            'mfa', 'create', '--uid', 'mfa-test-user', '--totp-serial', cls._mfa_token_serial,
            '--totp-seed', cls._mfa_token_seed, '--totp-seed-type', 'base32',
            '--totp-seconds', str(cls._mfa_token_time_step), '--totp-window', '1'
        ])
        # Create tenanted users.
        cls._radosgw_admin_cmd([
            'user', 'create', '--tenant', 'testx', '--uid', 'teuth-test-user',
            '--display-name', 'tenanted teuth-test-user'
        ])
        cls._radosgw_admin_cmd([
            'user', 'create', '--tenant', 'testx2', '--uid', 'teuth-test-user2',
            '--display-name', 'tenanted teuth-test-user 2'
        ])

    @classmethod
    def tearDownClass(cls):
        cls._radosgw_admin_cmd(
            ['user', 'rm', '--tenant', 'testx', '--uid=teuth-test-user', '--purge-data'])
        cls._radosgw_admin_cmd(
            ['user', 'rm', '--tenant', 'testx2', '--uid=teuth-test-user2', '--purge-data'])
        super(RgwBucketTest, cls).tearDownClass()

    def _get_mfa_token_pin(self):
        totp_key = base64.b32decode(self._mfa_token_seed)
        totp = TOTP(totp_key, 6, SHA1(), self._mfa_token_time_step, backend=default_backend(),
                    enforce_key_length=False)
        time_value = int(time.time())
        return totp.generate(time_value)

    def test_all(self):
        # Create a new bucket.
        self._post(
            '/api/rgw/bucket',
            params={
                'bucket': 'teuth-test-bucket',
                'uid': 'admin',
                'zonegroup': 'default',
                'placement_target': 'default-placement'
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
        data = self._get('/api/rgw/bucket', version='1.1')
        self.assertStatus(200)
        self.assertEqual(len(data), 1)
        self.assertIn('teuth-test-bucket', data)

        # List all buckets with stats.
        data = self._get('/api/rgw/bucket?stats=true', version='1.1')
        self.assertStatus(200)
        self.assertEqual(len(data), 1)
        self.assertSchema(data[0], JObj(sub_elems={
            'bid': JLeaf(str),
            'bucket': JLeaf(str),
            'bucket_quota': JObj(sub_elems={}, allow_unknown=True),
            'id': JLeaf(str),
            'owner': JLeaf(str),
            'usage': JObj(sub_elems={}, allow_unknown=True),
            'tenant': JLeaf(str),
        }, allow_unknown=True))

        # List all buckets names without stats.
        data = self._get('/api/rgw/bucket?stats=false', version='1.1')
        self.assertStatus(200)
        self.assertEqual(data, ['teuth-test-bucket'])

        # Get the bucket.
        data = self._get('/api/rgw/bucket/teuth-test-bucket')
        self.assertStatus(200)
        self.assertSchema(data, JObj(sub_elems={
            'id': JLeaf(str),
            'bid': JLeaf(str),
            'tenant': JLeaf(str),
            'bucket': JLeaf(str),
            'bucket_quota': JObj(sub_elems={}, allow_unknown=True),
            'owner': JLeaf(str),
            'mfa_delete': JLeaf(str),
            'usage': JObj(sub_elems={}, allow_unknown=True),
            'versioning': JLeaf(str)
        }, allow_unknown=True))
        self.assertEqual(data['bucket'], 'teuth-test-bucket')
        self.assertEqual(data['owner'], 'admin')
        self.assertEqual(data['placement_rule'], 'default-placement')
        self.assertEqual(data['versioning'], 'Suspended')

        # Update bucket: change owner, enable versioning.
        self._put(
            '/api/rgw/bucket/teuth-test-bucket',
            params={
                'bucket_id': data['id'],
                'uid': 'mfa-test-user',
                'versioning_state': 'Enabled'
            })
        self.assertStatus(200)
        data = self._get('/api/rgw/bucket/teuth-test-bucket')
        self.assertStatus(200)
        self.assertSchema(data, JObj(sub_elems={
            'owner': JLeaf(str),
            'bid': JLeaf(str),
            'tenant': JLeaf(str)
        }, allow_unknown=True))
        self.assertEqual(data['owner'], 'mfa-test-user')
        self.assertEqual(data['versioning'], 'Enabled')

        # Update bucket: enable MFA Delete.
        self._put(
            '/api/rgw/bucket/teuth-test-bucket',
            params={
                'bucket_id': data['id'],
                'uid': 'mfa-test-user',
                'versioning_state': 'Enabled',
                'mfa_delete': 'Enabled',
                'mfa_token_serial': self._mfa_token_serial,
                'mfa_token_pin': self._get_mfa_token_pin()
            })
        self.assertStatus(200)
        data = self._get('/api/rgw/bucket/teuth-test-bucket')
        self.assertStatus(200)
        self.assertEqual(data['versioning'], 'Enabled')
        self.assertEqual(data['mfa_delete'], 'Enabled')

        # Update bucket: disable versioning & MFA Delete.
        time.sleep(self._mfa_token_time_step * 3)  # Required to get new TOTP pin.
        self._put(
            '/api/rgw/bucket/teuth-test-bucket',
            params={
                'bucket_id': data['id'],
                'uid': 'mfa-test-user',
                'versioning_state': 'Suspended',
                'mfa_delete': 'Disabled',
                'mfa_token_serial': self._mfa_token_serial,
                'mfa_token_pin': self._get_mfa_token_pin()
            })
        self.assertStatus(200)
        data = self._get('/api/rgw/bucket/teuth-test-bucket')
        self.assertStatus(200)
        self.assertEqual(data['versioning'], 'Suspended')
        self.assertEqual(data['mfa_delete'], 'Disabled')

        # Delete the bucket.
        self._delete('/api/rgw/bucket/teuth-test-bucket')
        self.assertStatus(204)
        data = self._get('/api/rgw/bucket', version='1.1')
        self.assertStatus(200)
        self.assertEqual(len(data), 0)

    def test_crud_w_tenant(self):
        # Create a new bucket. The tenant of the user is used when
        # the bucket is created.
        self._post(
            '/api/rgw/bucket',
            params={
                'bucket': 'teuth-test-bucket',
                'uid': 'testx$teuth-test-user',
                'zonegroup': 'default',
                'placement_target': 'default-placement'
            })
        self.assertStatus(201)
        # It's not possible to validate the result because there
        # IS NO result object returned by the RGW Admin OPS API
        # when a tenanted bucket is created.
        data = self.jsonBody()
        self.assertIsNone(data)

        # List all buckets.
        data = self._get('/api/rgw/bucket', version='1.1')
        self.assertStatus(200)
        self.assertEqual(len(data), 1)
        self.assertIn('testx/teuth-test-bucket', data)

        def _verify_tenant_bucket(bucket, tenant, uid):
            full_bucket_name = '{}/{}'.format(tenant, bucket)
            _data = self._get('/api/rgw/bucket/{}'.format(
                parse.quote_plus(full_bucket_name)))
            self.assertStatus(200)
            self.assertSchema(_data, JObj(sub_elems={
                'owner': JLeaf(str),
                'bucket': JLeaf(str),
                'tenant': JLeaf(str),
                'bid': JLeaf(str)
            }, allow_unknown=True))
            self.assertEqual(_data['owner'], '{}${}'.format(tenant, uid))
            self.assertEqual(_data['bucket'], bucket)
            self.assertEqual(_data['tenant'], tenant)
            self.assertEqual(_data['bid'], full_bucket_name)
            return _data

        # Get the bucket.
        data = _verify_tenant_bucket('teuth-test-bucket', 'testx', 'teuth-test-user')
        self.assertEqual(data['placement_rule'], 'default-placement')
        self.assertEqual(data['versioning'], 'Suspended')

        # Update bucket: different user with different tenant, enable versioning.
        self._put(
            '/api/rgw/bucket/{}'.format(
                parse.quote_plus('testx/teuth-test-bucket')),
            params={
                'bucket_id': data['id'],
                'uid': 'testx2$teuth-test-user2',
                'versioning_state': 'Enabled'
            })
        data = _verify_tenant_bucket('teuth-test-bucket', 'testx2', 'teuth-test-user2')
        self.assertEqual(data['versioning'], 'Enabled')

        # Change owner to a non-tenanted user
        self._put(
            '/api/rgw/bucket/{}'.format(
                parse.quote_plus('testx2/teuth-test-bucket')),
            params={
                'bucket_id': data['id'],
                'uid': 'admin'
            })
        self.assertStatus(200)
        data = self._get('/api/rgw/bucket/teuth-test-bucket')
        self.assertStatus(200)
        self.assertIn('owner', data)
        self.assertEqual(data['owner'], 'admin')
        self.assertEqual(data['tenant'], '')
        self.assertEqual(data['bucket'], 'teuth-test-bucket')
        self.assertEqual(data['bid'], 'teuth-test-bucket')
        self.assertEqual(data['versioning'], 'Enabled')

        # Change owner back to tenanted user, suspend versioning.
        self._put(
            '/api/rgw/bucket/teuth-test-bucket',
            params={
                'bucket_id': data['id'],
                'uid': 'testx$teuth-test-user',
                'versioning_state': 'Suspended'
            })
        self.assertStatus(200)
        data = _verify_tenant_bucket('teuth-test-bucket', 'testx', 'teuth-test-user')
        self.assertEqual(data['versioning'], 'Suspended')

        # Delete the bucket.
        self._delete('/api/rgw/bucket/{}'.format(
            parse.quote_plus('testx/teuth-test-bucket')))
        self.assertStatus(204)
        data = self._get('/api/rgw/bucket', version='1.1')
        self.assertStatus(200)
        self.assertEqual(len(data), 0)

    def test_crud_w_locking(self):
        # Create
        self._post('/api/rgw/bucket',
                   params={
                       'bucket': 'teuth-test-bucket',
                       'uid': 'mfa-test-user',
                       'zonegroup': 'default',
                       'placement_target': 'default-placement',
                       'lock_enabled': 'true',
                       'lock_mode': 'GOVERNANCE',
                       'lock_retention_period_days': '0',
                       'lock_retention_period_years': '1'
                   })
        self.assertStatus(201)
        # Read
        data = self._get('/api/rgw/bucket/teuth-test-bucket')
        self.assertStatus(200)
        self.assertSchema(
            data,
            JObj(sub_elems={
                'lock_enabled': JLeaf(bool),
                'lock_mode': JLeaf(str),
                'lock_retention_period_days': JLeaf(int),
                'lock_retention_period_years': JLeaf(int)
            },
                allow_unknown=True))
        self.assertTrue(data['lock_enabled'])
        self.assertEqual(data['lock_mode'], 'GOVERNANCE')
        self.assertEqual(data['lock_retention_period_days'], 0)
        self.assertEqual(data['lock_retention_period_years'], 1)
        # Update
        self._put('/api/rgw/bucket/teuth-test-bucket',
                  params={
                      'bucket_id': data['id'],
                      'uid': 'mfa-test-user',
                      'lock_mode': 'COMPLIANCE',
                      'lock_retention_period_days': '15',
                      'lock_retention_period_years': '0'
                  })
        self.assertStatus(200)
        data = self._get('/api/rgw/bucket/teuth-test-bucket')
        self.assertTrue(data['lock_enabled'])
        self.assertEqual(data['lock_mode'], 'COMPLIANCE')
        self.assertEqual(data['lock_retention_period_days'], 15)
        self.assertEqual(data['lock_retention_period_years'], 0)
        self.assertStatus(200)

        # Update: Disabling bucket versioning should fail if object locking enabled
        self._put('/api/rgw/bucket/teuth-test-bucket',
                  params={
                      'bucket_id': data['id'],
                      'uid': 'mfa-test-user',
                      'versioning_state': 'Suspended'
                  })
        self.assertStatus(409)

        # Delete
        self._delete('/api/rgw/bucket/teuth-test-bucket')
        self.assertStatus(204)


class RgwDaemonTest(RgwTestCase):

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
        self.assertIn('zonegroup_name', data)
        self.assertIn('zone_name', data)
        self.assertIn('port', data)

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
        data = self._get('/ui-api/rgw/status')
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
        self.assertTrue(data['stats'])
        self.assertIsInstance(data['stats'], dict)
        # Test without stats.
        data = self.get_rgw_user('admin', False)
        self.assertStatus(200)
        self._assert_user_data(data)
        self.assertEqual(data['user_id'], 'admin')

    def test_list(self):
        data = self._get('/api/rgw/user')
        self.assertStatus(200)
        self.assertGreaterEqual(len(data), 1)
        self.assertIn('admin', data)

    def test_get_emails(self):
        data = self._get('/api/rgw/user/get_emails')
        self.assertStatus(200)
        self.assertSchema(data, JList(str))

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
        self.assertStatus(200)
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
        self.assertStatus(200)
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


class RgwApiCredentialsTest(RgwTestCase):

    AUTH_ROLES = ['rgw-manager']

    def test_invalid_credentials(self):
        self._ceph_cmd_with_secret(['dashboard', 'set-rgw-api-secret-key'], 'invalid')
        self._ceph_cmd_with_secret(['dashboard', 'set-rgw-api-access-key'], 'invalid')
        resp = self._get('/api/rgw/user')
        self.assertStatus(404)
        self.assertIn('detail', resp)
        self.assertIn('component', resp)
        self.assertIn('Error connecting to Object Gateway', resp['detail'])
        self.assertEqual(resp['component'], 'rgw')

    def test_success(self):
        # Set the default credentials.
        self._ceph_cmd_with_secret(['dashboard', 'set-rgw-api-secret-key'], 'admin')
        self._ceph_cmd_with_secret(['dashboard', 'set-rgw-api-access-key'], 'admin')
        data = self._get('/ui-api/rgw/status')
        self.assertStatus(200)
        self.assertIn('available', data)
        self.assertIn('message', data)
        self.assertTrue(data['available'])
