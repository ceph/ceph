from unittest import TestCase
from unittest.mock import call, patch

from ..controllers.rgw_iam import RgwRolePolicyController, RgwUserAccountsController
from ..exceptions import DashboardException
from ..services.rgw_iam import RgwAccounts
from ..tests import ControllerTestCase


class TestRgwUserAccountsController(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        cls.setup_controllers([RgwUserAccountsController], '/test')

    @patch('dashboard.controllers.rgw.RgwAccounts.get_accounts')
    @patch('dashboard.controllers.rgw_iam.RgwUserAccountsController.get_account')
    @patch('dashboard.controllers.rgw_iam.RgwUserAccountsController.proxy')
    def test_account_list(self, mock_proxy, mock_get_account, mock_get_accounts):
        mock_get_accounts.return_value = ['RGW67392003738907404']
        mock_proxy.return_value = {
            "id": "RGW67392003738907404",
            "tenant": "",
            "name": "",
            "email": "",
            "quota": {
                "enabled": False,
                "check_on_raw": False,
                "max_size": -1,
                "max_size_kb": 0,
                "max_objects": -1
            },
            "bucket_quota": {
                "enabled": False,
                "check_on_raw": False,
                "max_size": -1,
                "max_size_kb": 0,
                "max_objects": -1
            },
            "max_users": 1000,
            "max_roles": 1000,
            "max_groups": 1000,
            "max_buckets": 1000,
            "max_access_keys": 4
        }

        mock_get_account.side_effect = lambda account_id, daemon_name=None: {
            "id": account_id,
            "tenant": "",
            "name": "",
            "email": "",
            "quota": {
                "enabled": False,
                "check_on_raw": False,
                "max_size": -1,
                "max_size_kb": 0,
                "max_objects": -1
            },
            "bucket_quota": {
                "enabled": False,
                "check_on_raw": False,
                "max_size": -1,
                "max_size_kb": 0,
                "max_objects": -1
            },
            "max_users": 1000,
            "max_roles": 1000,
            "max_groups": 1000,
            "max_buckets": 1000,
            "max_access_keys": 4
        }
        self._get('/test/api/rgw/accounts?daemon_name=dummy-daemon')
        self.assertStatus(200)
        self.assertJsonBody(['RGW67392003738907404'])

        mock_get_accounts.assert_called_once()

        self._get('/test/api/rgw/accounts?daemon_name=dummy-daemon&detailed=true')
        self.assertStatus(200)

        expected_detailed_response = [
            {
                "id": "RGW67392003738907404",
                "tenant": "",
                "name": "",
                "email": "",
                "quota": {
                    "enabled": False,
                    "check_on_raw": False,
                    "max_size": -1,
                    "max_size_kb": 0,
                    "max_objects": -1
                },
                "bucket_quota": {
                    "enabled": False,
                    "check_on_raw": False,
                    "max_size": -1,
                    "max_size_kb": 0,
                    "max_objects": -1
                },
                "max_users": 1000,
                "max_roles": 1000,
                "max_groups": 1000,
                "max_buckets": 1000,
                "max_access_keys": 4
            }
        ]
        self.assertJsonBody(expected_detailed_response)

        mock_get_account.assert_has_calls([
            call('RGW67392003738907404', 'dummy-daemon')
        ])

        self._get('/test/api/rgw/accounts/RGW67392003738907404?daemon_name=dummy-daemon')
        self.assertStatus(200)

    @patch('dashboard.controllers.rgw_iam.RgwUserAccountsController.proxy')
    def test_create_account(self, mock_proxy):
        mock_proxy.return_value = {
            "id": "RGW67392003738907404",
            "tenant": "",
            "name": "jack",
            "email": "",
            "quota": {
                "enabled": False,
                "check_on_raw": False,
                "max_size": -1,
                "max_size_kb": 0,
                "max_objects": -1
            },
            "bucket_quota": {
                "enabled": False,
                "check_on_raw": False,
                "max_size": -1,
                "max_size_kb": 0,
                "max_objects": -1
            },
            "max_users": 1000,
            "max_roles": 1000,
            "max_groups": 1000,
            "max_buckets": 1000,
            "max_access_keys": 4
        }

        self._post('/test/api/rgw/accounts?daemon_name=dummy-daemon', data={
            'account_name': 'jack',
            'max_buckets': '1000',
            'max_users': '1000',
            'max_roles': '1000',
            'max_group': '1000',
            'max_access_keys': '4'
        })

        mock_proxy.assert_called_once_with(
            'dummy-daemon', 'POST', 'account', {
                'name': 'jack',
                'max-buckets': '1000',
                'max-users': '1000',
                'max-roles': '1000',
                'max-groups': '1000',
                'max-access-keys': '4'
            })

    @patch('dashboard.controllers.rgw_iam.RgwUserAccountsController.get_account')
    @patch('dashboard.controllers.rgw_iam.RgwUserAccountsController.proxy')
    def test_get_account(self, mock_get_account, mock_proxy):
        mock_proxy.return_value = {
            "id": "RGW67392003738907404",
            "tenant": "",
            "name": "",
            "email": "",
            "quota": {
                "enabled": False,
                "check_on_raw": False,
                "max_size": -1,
                "max_size_kb": 0,
                "max_objects": -1
            },
            "bucket_quota": {
                "enabled": False,
                "check_on_raw": False,
                "max_size": -1,
                "max_size_kb": 0,
                "max_objects": -1
            },
            "max_users": 1000,
            "max_roles": 1000,
            "max_groups": 1000,
            "max_buckets": 1000,
            "max_access_keys": 4
        }

        mock_get_account.side_effect = lambda account_id, daemon_name=None: {
            "id": account_id,
            "tenant": "",
            "name": "",
            "email": "",
            "quota": {
                "enabled": False,
                "check_on_raw": False,
                "max_size": -1,
                "max_size_kb": 0,
                "max_objects": -1
            },
            "bucket_quota": {
                "enabled": False,
                "check_on_raw": False,
                "max_size": -1,
                "max_size_kb": 0,
                "max_objects": -1
            },
            "max_users": 1000,
            "max_roles": 1000,
            "max_groups": 1000,
            "max_buckets": 1000,
            "max_access_keys": 4
        }
        self._get('/test/api/rgw/accounts/RGW67392003738907404?daemon_name=dummy-daemon')
        self.assertStatus(200)

    @patch('dashboard.controllers.rgw_iam.RgwUserAccountsController.proxy')
    def test_delete_account(self, mock_proxy):
        mock_proxy.return_value = None

        self._delete('/test/api/rgw/accounts/RGW67392003738907404?daemon_name=dummy-daemon')
        self.assertStatus(204)

    @patch('dashboard.controllers.rgw_iam.RgwUserAccountsController.proxy')
    def test_set_account(self, mock_proxy):
        mock_proxy.return_value = {
            "id": "RGW67392003738907404",
            "tenant": "",
            "name": "jack",
            "email": "",
            "quota": {
                "enabled": False,
                "check_on_raw": False,
                "max_size": -1,
                "max_size_kb": 0,
                "max_objects": -1
            },
            "bucket_quota": {
                "enabled": False,
                "check_on_raw": False,
                "max_size": -1,
                "max_size_kb": 0,
                "max_objects": -1
            },
            "max_users": 1000,
            "max_roles": 1000,
            "max_groups": 1000,
            "max_buckets": 1000,
            "max_access_keys": 4
        }

        self._put('/test/api/rgw/accounts/RGW67392003738907404?daemon_name=dummy-daemon', data={
            'account_name': 'jack',
            'max_buckets': '1000',
            'max_users': '1000',
            'max_roles': '1000',
            'max_group': '1000',
            'max_access_keys': '4'
        })

        mock_proxy.assert_called_once_with(
            'dummy-daemon', 'PUT', 'account', {
                'id': 'RGW67392003738907404',
                'name': 'jack',
                'max-buckets': '1000',
                'max-users': '1000',
                'max-roles': '1000',
                'max-groups': '1000',
                'max-access-keys': '4'
            })

    @patch('dashboard.controllers.rgw_iam.RgwUserAccountsController.set_quota')
    def test_set_quota(self, mock_set_quota):
        mock_return_value = {
            "id": "RGW11111111111111111",
            "tenant": "",
            "name": "Account1",
            "email": "account1@ceph.com",
            "quota": {
                "enabled": False,
                "check_on_raw": False,
                "max_size": 10737418240,
                "max_size_kb": 10485760,
                "max_objects": 1000000
            },
            "bucket_quota": {
                "enabled": False,
                "check_on_raw": False,
                "max_size": -1,
                "max_size_kb": 0,
                "max_objects": 1000000
            },
            "max_users": 1000,
            "max_roles": 1000,
            "max_groups": 1000,
            "max_buckets": 1000,
            "max_access_keys": 4
        }

        mock_set_quota.return_value = mock_return_value

        controller = RgwUserAccountsController()
        result = controller.set_quota('account', 'RGW11111111111111111', '10GB', '1000', True)

        mock_set_quota.assert_called_with('account', 'RGW11111111111111111', '10GB', '1000', True)

        self.assertEqual(result, mock_return_value)

    @patch('dashboard.controllers.rgw_iam.RgwUserAccountsController.set_quota_status')
    def test_set_quota_status(self, mock_set_quota_status):
        mock_return_value = {
            "id": "RGW11111111111111111",
            "tenant": "",
            "name": "Account1",
            "email": "account1@ceph.com",
            "quota": {
                "enabled": True,
                "check_on_raw": False,
                "max_size": 10737418240,
                "max_size_kb": 10485760,
                "max_objects": 1000000
            },
            "bucket_quota": {
                "enabled": False,
                "check_on_raw": False,
                "max_size": -1,
                "max_size_kb": 0,
                "max_objects": 1000000
            },
            "max_users": 1000,
            "max_roles": 1000,
            "max_groups": 1000,
            "max_buckets": 1000,
            "max_access_keys": 4
        }

        mock_set_quota_status.return_value = mock_return_value

        controller = RgwUserAccountsController()
        result = controller.set_quota_status('account', 'RGW11111111111111111', 'enabled')

        mock_set_quota_status.assert_called_with('account', 'RGW11111111111111111', 'enabled')

        self.assertEqual(result, mock_return_value)


class TestRgwAccountsRolePolicies(TestCase):
    ACCOUNT_ID = 'RGW12345678901234567'
    ROLE_NAME = 'test-role'
    POLICY_NAME = 'S3ReadPolicy'
    POLICY_DOC = '{"Version":"2012-10-17","Statement":[]}'

    @patch('dashboard.services.rgw_iam.mgr.send_rgwadmin_command')
    def test_list_role_policies(self, mock_cmd):
        mock_cmd.return_value = (0, ['S3ReadPolicy', 'S3WritePolicy'], '')

        result = RgwAccounts.list_role_policies(self.ROLE_NAME, self.ACCOUNT_ID)

        self.assertEqual(result, ['S3ReadPolicy', 'S3WritePolicy'])
        cmd = mock_cmd.call_args[0][0]
        self.assertEqual(cmd[:3], ['role', 'policy', 'list'])
        self.assertIn('--role-name', cmd)
        self.assertIn(self.ROLE_NAME, cmd)
        self.assertIn('--account-id', cmd)
        self.assertIn(self.ACCOUNT_ID, cmd)

    @patch('dashboard.services.rgw_iam.mgr.send_rgwadmin_command')
    def test_list_role_policies_from_dict(self, mock_cmd):
        mock_cmd.return_value = (0, {'PolicyNames': ['OnlyPolicy']}, '')

        result = RgwAccounts.list_role_policies(self.ROLE_NAME, self.ACCOUNT_ID)

        self.assertEqual(result, ['OnlyPolicy'])

    @patch('dashboard.services.rgw_iam.mgr.send_rgwadmin_command')
    def test_list_role_policies_error(self, mock_cmd):
        mock_cmd.return_value = (1, None, 'list failed')

        with self.assertRaises(DashboardException) as ctx:
            RgwAccounts.list_role_policies(self.ROLE_NAME, self.ACCOUNT_ID)

        self.assertIn('Error listing role policies', str(ctx.exception))

    @patch('dashboard.services.rgw_iam.mgr.send_rgwadmin_command')
    def test_get_role_policy(self, mock_cmd):
        policy = {'PolicyName': self.POLICY_NAME, 'PolicyDocument': self.POLICY_DOC}
        mock_cmd.return_value = (0, policy, '')

        result = RgwAccounts.get_role_policy(
            self.ROLE_NAME, self.POLICY_NAME, self.ACCOUNT_ID)

        self.assertEqual(result, policy)
        cmd = mock_cmd.call_args[0][0]
        self.assertEqual(cmd[:3], ['role', 'policy', 'get'])
        self.assertIn('--policy-name', cmd)
        self.assertIn(self.POLICY_NAME, cmd)
        self.assertIn('--account-id', cmd)
        self.assertIn(self.ACCOUNT_ID, cmd)

    @patch('dashboard.services.rgw_iam.mgr.send_rgwadmin_command')
    def test_get_role_policy_error(self, mock_cmd):
        mock_cmd.return_value = (1, None, 'not found')

        with self.assertRaises(DashboardException) as ctx:
            RgwAccounts.get_role_policy(
                self.ROLE_NAME, self.POLICY_NAME, self.ACCOUNT_ID)

        self.assertIn('Error getting role policy', str(ctx.exception))

    @patch('dashboard.services.rgw_iam.mgr.send_rgwadmin_command')
    def test_put_role_policy(self, mock_cmd):
        mock_cmd.return_value = (0, '', '')

        RgwAccounts.put_role_policy(
            self.ROLE_NAME, self.POLICY_NAME, self.POLICY_DOC, self.ACCOUNT_ID)

        cmd = mock_cmd.call_args[0][0]
        self.assertEqual(cmd[:3], ['role', 'policy', 'put'])
        self.assertIn('--policy-name', cmd)
        self.assertIn(self.POLICY_NAME, cmd)
        self.assertIn('--policy-doc', cmd)
        self.assertIn(self.POLICY_DOC, cmd)
        self.assertIn('--account-id', cmd)
        self.assertFalse(mock_cmd.call_args[1].get('stdout_as_json', True))

    def test_put_role_policy_invalid_json(self):
        with self.assertRaises(DashboardException) as ctx:
            RgwAccounts.put_role_policy(
                self.ROLE_NAME, self.POLICY_NAME, 'not-json', self.ACCOUNT_ID)

        self.assertIn('not a valid json', str(ctx.exception))

    @patch('dashboard.services.rgw_iam.mgr.send_rgwadmin_command')
    def test_put_role_policy_error(self, mock_cmd):
        mock_cmd.return_value = (1, None, 'put failed')

        with self.assertRaises(DashboardException) as ctx:
            RgwAccounts.put_role_policy(
                self.ROLE_NAME, self.POLICY_NAME, self.POLICY_DOC, self.ACCOUNT_ID)

        self.assertIn('Error putting role policy', str(ctx.exception))

    @patch('dashboard.services.rgw_iam.mgr.send_rgwadmin_command')
    def test_delete_role_policy(self, mock_cmd):
        mock_cmd.return_value = (0, '', '')

        RgwAccounts.delete_role_policy(
            self.ROLE_NAME, self.POLICY_NAME, self.ACCOUNT_ID)

        cmd = mock_cmd.call_args[0][0]
        self.assertEqual(cmd[:3], ['role', 'policy', 'delete'])
        self.assertIn('--policy-name', cmd)
        self.assertIn(self.POLICY_NAME, cmd)
        self.assertIn('--account-id', cmd)
        self.assertFalse(mock_cmd.call_args[1].get('stdout_as_json', True))

    @patch('dashboard.services.rgw_iam.mgr.send_rgwadmin_command')
    def test_delete_role_policy_error(self, mock_cmd):
        mock_cmd.return_value = (1, None, 'delete failed')

        with self.assertRaises(DashboardException) as ctx:
            RgwAccounts.delete_role_policy(
                self.ROLE_NAME, self.POLICY_NAME, self.ACCOUNT_ID)

        self.assertIn('Error deleting role policy', str(ctx.exception))

    @patch('dashboard.services.rgw_iam.mgr.send_rgwadmin_command')
    def test_send_rgw_cmd_custom_error_message(self, mock_cmd):
        mock_cmd.return_value = (7, None, 'boom')

        with self.assertRaises(DashboardException) as ctx:
            RgwAccounts.send_rgw_cmd(
                ['role', 'policy', 'get'],
                error_msg='Error getting role policy with code {code}: {err}')

        self.assertEqual(str(ctx.exception), 'Error getting role policy with code 7: boom')


class TestRgwRolePolicyController(ControllerTestCase):
    ACCOUNT_ID = 'RGW12345678901234567'
    ROLE_NAME = 'test-role'
    POLICY_NAME = 'S3ReadPolicy'
    POLICY_DOC = '{"Version":"2012-10-17","Statement":[]}'

    @classmethod
    def setup_server(cls):
        cls.setup_controllers([RgwRolePolicyController], '/test')

    @patch('dashboard.controllers.rgw_iam.RgwAccounts.list_role_policies')
    def test_list(self, mock_list):
        mock_list.return_value = [self.POLICY_NAME]

        self._get(
            f'/test/api/rgw/accounts/{self.ACCOUNT_ID}/roles/{self.ROLE_NAME}/policy')
        self.assertStatus(200)
        self.assertJsonBody([self.POLICY_NAME])
        mock_list.assert_called_once_with(self.ROLE_NAME, self.ACCOUNT_ID)

    @patch('dashboard.controllers.rgw_iam.RgwAccounts.get_role_policy')
    def test_get(self, mock_get):
        policy = {'PolicyName': self.POLICY_NAME, 'PolicyDocument': self.POLICY_DOC}
        mock_get.return_value = policy

        self._get(
            f'/test/api/rgw/accounts/{self.ACCOUNT_ID}/roles/'
            f'{self.ROLE_NAME}/policy/{self.POLICY_NAME}')
        self.assertStatus(200)
        self.assertJsonBody(policy)
        mock_get.assert_called_once_with(
            self.ROLE_NAME, self.POLICY_NAME, self.ACCOUNT_ID)

    @patch('dashboard.controllers.rgw_iam.RgwAccounts.put_role_policy')
    @patch('dashboard.controllers._base_controller.cherrypy.request')
    def test_create(self, mock_request, mock_put):
        mock_request.headers = {
            'Accept': 'application/vnd.ceph.api.v1.0+json'
        }
        controller = RgwRolePolicyController()
        result = controller.create(
            self.ROLE_NAME, self.POLICY_NAME, self.POLICY_DOC, self.ACCOUNT_ID)

        mock_put.assert_called_once_with(
            self.ROLE_NAME, self.POLICY_NAME, self.POLICY_DOC, self.ACCOUNT_ID)
        if isinstance(result, bytes):
            result = result.decode('utf-8')
        self.assertIn(
            f'Policy {self.POLICY_NAME} attached to role {self.ROLE_NAME}',
            result)

    @patch('dashboard.controllers.rgw_iam.RgwAccounts.delete_role_policy')
    def test_delete(self, mock_delete):
        self._delete(
            f'/test/api/rgw/accounts/{self.ACCOUNT_ID}/roles/'
            f'{self.ROLE_NAME}/policy/{self.POLICY_NAME}')
        self.assertStatus(204)
        mock_delete.assert_called_once_with(
            self.ROLE_NAME, self.POLICY_NAME, self.ACCOUNT_ID)
