from unittest.mock import call, patch

from ..controllers.rgw_iam import RgwUserAccountsController
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
                'max-group': '1000',
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
                'max-group': '1000',
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
