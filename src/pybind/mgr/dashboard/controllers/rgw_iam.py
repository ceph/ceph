from typing import Optional

from ..controllers.rgw import RgwRESTController
from ..security import Scope
from ..services.rgw_iam import RgwAccounts
from ..tools import str_to_bool
from . import APIDoc, APIRouter, EndpointDoc, RESTController, allow_empty_body


@APIRouter('rgw/accounts', Scope.RGW)
@APIDoc("RGW User Accounts API", "RgwUserAccounts")
class RgwUserAccountsController(RgwRESTController):
    @EndpointDoc("Update RGW account info",
                 parameters={'account_name': (str, 'Account name'),
                             'email': (str, 'Email'),
                             'tenant': (str, 'Tenant'),
                             'max_buckets': (int, 'Max buckets'),
                             'max_users': (int, 'Max users'),
                             'max_roles': (int, 'Max roles'),
                             'max_group': (int, 'Max groups'),
                             'max_access_keys': (int, 'Max access keys'),
                             'daemon_name': (str, 'Name of the daemon')})
    @allow_empty_body
    def create(self, account_name: str, tenant: Optional[str] = None,
               email: Optional[str] = None, max_buckets: Optional[int] = None,
               max_users: Optional[int] = None, max_roles: Optional[int] = None,
               max_group: Optional[int] = None, daemon_name=None,
               max_access_keys: Optional[int] = None):
        """
        Create an account

        :param account_name: Account name
        :return: Returns account resource.
        :rtype: Dict[str, Any]
        """
        params = {'name': account_name}
        if tenant:
            params['tenant'] = tenant
        if email:
            params['email'] = email
        if max_buckets:
            params['max-buckets'] = str(max_buckets)
        if max_users:
            params['max-users'] = str(max_users)
        if max_roles:
            params['max-roles'] = str(max_roles)
        if max_group:
            params['max-group'] = str(max_group)
        if max_access_keys:
            params['max-access-keys'] = str(max_access_keys)

        result = self.proxy(daemon_name, 'POST', 'account', params)
        return result

    def list(self, daemon_name=None, detailed: bool = False):
        """
        List all account ids or all detailed account info based on the 'detailed' query parameter.

        - If detailed=True, returns detailed account info.
        - If detailed=False, returns only account ids.
        """
        detailed = str_to_bool(detailed)
        account_list = RgwAccounts.get_accounts()
        detailed_account_list = []
        if detailed:
            for account in account_list:
                detailed_account_list.append(self.get_account(account, daemon_name))
            return detailed_account_list
        return account_list

    @EndpointDoc("Get RGW Account by id",
                 parameters={'account_id': (str, 'Account id'),
                             'daemon_name': (str, 'Name of the daemon')})
    def get(self, account_id: str, daemon_name=None):
        """
        Get an account by account id
        """
        return self.get_account(account_id, daemon_name)

    def get_account(self, account_id, daemon_name=None) -> dict:
        return self.proxy(daemon_name, 'GET', 'account', {'id': account_id})

    @EndpointDoc("Delete RGW Account",
                 parameters={'account_id': (str, 'Account id'),
                             'daemon_name': (str, 'Name of the daemon')})
    def delete(self, account_id, daemon_name=None):
        """
        Removes an account

        :param account_id: account identifier
        :return: None.
        """
        return self.proxy(daemon_name, 'DELETE', 'account', {'id': account_id}, json_response=False)

    @EndpointDoc("Update RGW account info",
                 parameters={'account_id': (str, 'Account id'),
                             'account_name': (str, 'Account name'),
                             'email': (str, 'Email'),
                             'tenant': (str, 'Tenant'),
                             'max_buckets': (int, 'Max buckets'),
                             'max_users': (int, 'Max users'),
                             'max_roles': (int, 'Max roles'),
                             'max_group': (int, 'Max groups'),
                             'max_access_keys': (int, 'Max access keys'),
                             'daemon_name': (str, 'Name of the daemon')})
    @allow_empty_body
    def set(self, account_id: str, account_name: str,
            email: Optional[str] = None, tenant: Optional[str] = None,
            max_buckets: Optional[int] = None, max_users: Optional[int] = None,
            max_roles: Optional[int] = None, max_group: Optional[int] = None,
            max_access_keys: Optional[int] = None, daemon_name=None):
        """
        Modifies an account

        :param account_id: Account identifier
        :return: Returns modified account resource.
        :rtype: Dict[str, Any]
        """

        params = {'id': account_id}
        if account_name:
            params['name'] = account_name
        if tenant:
            params['tenant'] = tenant
        if email:
            params['email'] = email
        if max_buckets:
            params['max-buckets'] = str(max_buckets)
        if max_users:
            params['max-users'] = str(max_users)
        if max_roles:
            params['max-roles'] = str(max_roles)
        if max_group:
            params['max-group'] = str(max_group)
        if max_access_keys:
            params['max-access-keys'] = str(max_access_keys)

        return self.proxy(daemon_name, 'PUT', 'account', params)

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
