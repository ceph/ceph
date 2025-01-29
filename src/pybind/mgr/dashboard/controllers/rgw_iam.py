from typing import Optional

from ..security import Scope
from ..services.rgw_iam import RgwAccounts
from ..tools import str_to_bool
from . import APIDoc, APIRouter, EndpointDoc, RESTController, allow_empty_body


@APIRouter('rgw/accounts', Scope.RGW)
@APIDoc("RGW User Accounts API", "RgwUserAccounts")
class RgwUserAccountsController(RESTController):
    @EndpointDoc("Update RGW account info",
                 parameters={'account_name': (str, 'Account name'),
                             'email': (str, 'Email'),
                             'tenant': (str, 'Tenant'),
                             'max_buckets': (int, 'Max buckets'),
                             'max_users': (int, 'Max users'),
                             'max_roles': (int, 'Max roles'),
                             'max_group': (int, 'Max groups'),
                             'max_access_keys': (int, 'Max access keys')})
    @allow_empty_body
    def create(self, account_name: str, tenant: Optional[str] = None,
               email: Optional[str] = None, max_buckets: Optional[int] = None,
               max_users: Optional[int] = None, max_roles: Optional[int] = None,
               max_group: Optional[int] = None,
               max_access_keys: Optional[int] = None):
        """
        Create an account

        :param account_name: Account name
        :return: Returns account resource.
        :rtype: Dict[str, Any]
        """
        return RgwAccounts.create_account(account_name, tenant, email,
                                          max_buckets, max_users, max_roles,
                                          max_group, max_access_keys)

    def list(self, detailed: bool = False):
        """
        List all account ids or all detailed account info based on the 'detailed' query parameter.

        - If detailed=True, returns detailed account info.
        - If detailed=False, returns only account ids.
        """
        detailed = str_to_bool(detailed)
        return RgwAccounts.get_accounts(detailed)

    @EndpointDoc("Get RGW Account by id",
                 parameters={'account_id': (str, 'Account id')})
    def get(self, account_id: str):
        """
        Get an account by account id
        """
        return RgwAccounts.get_account(account_id)

    @EndpointDoc("Delete RGW Account",
                 parameters={'account_id': (str, 'Account id')})
    def delete(self, account_id):
        """
        Removes an account

        :param account_id: account identifier
        :return: None.
        """
        return RgwAccounts.delete_account(account_id)

    @EndpointDoc("Update RGW account info",
                 parameters={'account_id': (str, 'Account id'),
                             'account_name': (str, 'Account name'),
                             'email': (str, 'Email'),
                             'tenant': (str, 'Tenant'),
                             'max_buckets': (int, 'Max buckets'),
                             'max_users': (int, 'Max users'),
                             'max_roles': (int, 'Max roles'),
                             'max_group': (int, 'Max groups'),
                             'max_access_keys': (int, 'Max access keys')})
    @allow_empty_body
    def set(self, account_id: str, account_name: str,
            email: Optional[str] = None, tenant: Optional[str] = None,
            max_buckets: Optional[int] = None, max_users: Optional[int] = None,
            max_roles: Optional[int] = None, max_group: Optional[int] = None,
            max_access_keys: Optional[int] = None):
        """
        Modifies an account

        :param account_id: Account identifier
        :return: Returns modified account resource.
        :rtype: Dict[str, Any]
        """
        return RgwAccounts.modify_account(account_id, account_name, email, tenant,
                                          max_buckets, max_users, max_roles,
                                          max_group, max_access_keys)

    @EndpointDoc("Set RGW Account/Bucket quota",
                 parameters={'account_id': (str, 'Account id'),
                             'quota_type': (str, 'Quota type'),
                             'max_size': (str, 'Max size'),
                             'max_objects': (str, 'Max objects')})
    @RESTController.Resource(method='PUT', path='/quota')
    @allow_empty_body
    def set_quota(self, quota_type: str, account_id: str, max_size: str, max_objects: str,
                  enabled: bool):
        """
        Modifies quota

        :param account_id: Account identifier
        :param quota_type: 'account' or 'bucket'
        :return: Returns modified quota.
        :rtype: Dict[str, Any]
        """
        return RgwAccounts.set_quota(quota_type, account_id, max_size, max_objects, enabled)

    @EndpointDoc("Enable/Disable RGW Account/Bucket quota",
                 parameters={'account_id': (str, 'Account id'),
                             'quota_type': (str, 'Quota type'),
                             'quota_status': (str, 'Quota status')})
    @RESTController.Resource(method='PUT', path='/quota/status')
    @allow_empty_body
    def set_quota_status(self, quota_type: str, account_id: str, quota_status: str):
        """
        Enable/Disable quota

        :param account_id: Account identifier
        :param quota_type: 'account' or 'bucket'
        :param quota_status: 'enable' or 'disable'
        :return: Returns modified quota.
        :rtype: Dict[str, Any]
        """
        return RgwAccounts.set_quota_status(quota_type, account_id, quota_status)
