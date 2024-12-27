from unittest import TestCase
from unittest.mock import patch

from ..controllers.rgw_iam import RgwUserAccountsController
from ..services.rgw_iam import RgwAccounts


class TestRgwUserAccountsController(TestCase):

    @patch.object(RgwAccounts, 'create_account')
    def test_create_account(self, mock_create_account):
        mockReturnVal = {
            "id": "RGW18661471562806836",
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

        # Mock the return value of the create_account method
        mock_create_account.return_value = mockReturnVal

        controller = RgwUserAccountsController()
        result = controller.create(account_name='test_account', account_id='RGW18661471562806836',
                                   email='test@example.com')

        # Check if the account creation method was called with the correct parameters
        mock_create_account.assert_called_with('test_account', 'RGW18661471562806836',
                                               'test@example.com')
        # Check the returned result
        self.assertEqual(result, mockReturnVal)

    @patch.object(RgwAccounts, 'get_accounts')
    def test_list_accounts(self, mock_get_accounts):
        mock_return_value = [
            "RGW22222222222222222",
            "RGW59378973811515857",
            "RGW11111111111111111"
        ]

        mock_get_accounts.return_value = mock_return_value

        controller = RgwUserAccountsController()
        result = controller.list(detailed=False)

        mock_get_accounts.assert_called_with(False)

        self.assertEqual(result, mock_return_value)

    @patch.object(RgwAccounts, 'get_accounts')
    def test_list_accounts_with_details(self, mock_get_accounts):
        mock_return_value = [
            {
                "id": "RGW22222222222222222",
                "tenant": "",
                "name": "Account2",
                "email": "account2@ceph.com",
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
            },
            {
                "id": "RGW11111111111111111",
                "tenant": "",
                "name": "Account1",
                "email": "account1@ceph.com",
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

        mock_get_accounts.return_value = mock_return_value

        controller = RgwUserAccountsController()
        result = controller.list(detailed=True)

        mock_get_accounts.assert_called_with(True)

        self.assertEqual(result, mock_return_value)

    @patch.object(RgwAccounts, 'get_account')
    def test_get_account(self, mock_get_account):
        mock_return_value = {
            "id": "RGW22222222222222222",
            "tenant": "",
            "name": "Account2",
            "email": "account2@ceph.com",
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
        mock_get_account.return_value = mock_return_value

        controller = RgwUserAccountsController()
        result = controller.get(account_id='RGW22222222222222222')

        mock_get_account.assert_called_with('RGW22222222222222222')

        self.assertEqual(result, mock_return_value)

    @patch.object(RgwAccounts, 'delete_account')
    def test_delete_account(self, mock_delete_account):
        mock_delete_account.return_value = None

        controller = RgwUserAccountsController()
        result = controller.delete(account_id='RGW59378973811515857')

        mock_delete_account.assert_called_with('RGW59378973811515857')

        self.assertEqual(result, None)

    @patch.object(RgwAccounts, 'modify_account')
    def test_set_account_name(self, mock_modify_account):
        mock_return_value = mock_return_value = {
            "id": "RGW59378973811515857",
            "tenant": "",
            "name": "new_account_name",
            "email": "new_email@example.com",
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
        mock_modify_account.return_value = mock_return_value

        controller = RgwUserAccountsController()
        result = controller.set(account_id='RGW59378973811515857', account_name='new_account_name',
                                email='new_email@example.com')

        mock_modify_account.assert_called_with('RGW59378973811515857', 'new_account_name',
                                               'new_email@example.com')

        self.assertEqual(result, mock_return_value)

    @patch.object(RgwAccounts, 'set_quota')
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
        result = controller.set_quota(quota_type='account', account_id='RGW11111111111111111',
                                      max_size='10GB', max_objects='1000')

        mock_set_quota.assert_called_with('account', 'RGW11111111111111111', '10GB', '1000')

        self.assertEqual(result, mock_return_value)

    @patch.object(RgwAccounts, 'set_quota_status')
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
        result = controller.set_quota_status(quota_type='account',
                                             account_id='RGW11111111111111111',
                                             quota_status='enabled')

        mock_set_quota_status.assert_called_with('account', 'RGW11111111111111111', 'enabled')

        self.assertEqual(result, mock_return_value)
