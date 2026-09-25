from typing import Dict, List, Optional

from ..controllers.rgw import RgwRESTController
from ..exceptions import DashboardException
from ..rest_client import RequestException
from ..security import Scope
from ..services.rgw_iam import RgwAccounts
from ..services.rgw_iam_policy import RgwIamPolicies
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
            params['max-groups'] = str(max_group)
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

    @EndpointDoc("Check if account name exists",
                 parameters={'account_name': (str, 'Account name'),
                             'daemon_name': (str, 'Name of the daemon')})
    @RESTController.Collection(method='GET', path='/exists')
    def exists(self, account_name: str, daemon_name=None):
        """
        Check if an account with the given name exists
        Returns True if account exists, False otherwise
        """
        try:
            self.proxy(daemon_name, 'GET', 'account', {'name': account_name})
            # If we get a result without error, the account exists
            return True
        except (DashboardException, RequestException):
            # If we get an error (e.g., account not found), it doesn't exist
            return False

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
            params['max-groups'] = str(max_group)
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


@APIRouter('/rgw/accounts/{account_id}/policies', Scope.RGW)
@APIDoc("RGW IAM Managed Policies API", "RgwIamPolicy")
class RgwAccountPoliciesController(RESTController):
    RESOURCE_ID = 'policy_name'
    def list(self, account_id: str, daemon_name=None):
        """
        List managed policies for an account.
        """
        return RgwIamPolicies.list_policies(account_id)

    @EndpointDoc("Create managed policy",
                 parameters={'account_id': (str, 'Account ID'),
                             'policy_name': (str, 'Policy name'),
                             'policy_doc': (str, 'Policy document JSON'),
                             'path': (str, 'Policy path'),
                             'description': (str, 'Policy description')})
    @allow_empty_body
    def create(self, account_id: str, policy_name: str, policy_doc: str,
               path: str = '/', description: str = '', daemon_name=None):
        """
        Create a customer managed policy.
        """
        return RgwIamPolicies.create_policy(
            policy_name, policy_doc, account_id=account_id,
            path=path, description=description)

    @EndpointDoc("Get managed policy by ARN",
                 parameters={'account_id': (str, 'Account ID'),
                             'policy_arn': (str, 'Policy ARN')})
    @RESTController.Collection(method='GET', path='/get')
    def get_policy(self, account_id: str, policy_arn: str, daemon_name=None):
        """
        Get policy details and document.
        """
        return RgwIamPolicies.get_policy(policy_arn)

    @EndpointDoc("Delete managed policy",
                 parameters={'account_id': (str, 'Account ID'),
                             'policy_name': (str, 'Policy name')})
    def delete(self, account_id: str, policy_name: str, daemon_name=None):
        """
        Delete a managed policy.
        """
        return RgwIamPolicies.delete_policy_by_name(account_id, policy_name)

    @EndpointDoc("List policy versions",
                 parameters={'account_id': (str, 'Account ID'),
                             'policy_arn': (str, 'Policy ARN')})
    @RESTController.Collection(method='GET', path='/versions')
    def list_versions(self, account_id: str, policy_arn: str, daemon_name=None):
        return RgwIamPolicies.list_policy_versions(policy_arn)

    @EndpointDoc("Get policy version",
                 parameters={'account_id': (str, 'Account ID'),
                             'policy_arn': (str, 'Policy ARN'),
                             'version_id': (str, 'Version ID')})
    @RESTController.Collection(method='GET', path='/versions/get')
    def get_version(self, account_id: str, policy_arn: str, version_id: str,
                    daemon_name=None):
        return RgwIamPolicies.get_policy_version(policy_arn, version_id)

    @EndpointDoc("Create policy version",
                 parameters={'account_id': (str, 'Account ID'),
                             'policy_arn': (str, 'Policy ARN'),
                             'policy_doc': (str, 'Policy document JSON'),
                             'set_as_default': (bool, 'Set as default version')})
    @RESTController.Collection(method='POST', path='/versions')
    @allow_empty_body
    def create_version(self, account_id: str, policy_arn: str, policy_doc: str,
                       set_as_default: bool = False, daemon_name=None):
        return RgwIamPolicies.create_policy_version(
            policy_arn, policy_doc, set_as_default)

    @EndpointDoc("Delete policy version",
                 parameters={'account_id': (str, 'Account ID'),
                             'policy_arn': (str, 'Policy ARN'),
                             'version_id': (str, 'Version ID')})
    @RESTController.Collection(method='DELETE', path='/versions')
    def remove_version(self, account_id: str, policy_arn: str, version_id: str,
                       daemon_name=None):
        return RgwIamPolicies.delete_policy_version(policy_arn, version_id)

    @EndpointDoc("Set default policy version",
                 parameters={'account_id': (str, 'Account ID'),
                             'policy_arn': (str, 'Policy ARN'),
                             'version_id': (str, 'Version ID')})
    @RESTController.Collection(method='PUT', path='/versions/default')
    @allow_empty_body
    def set_default_version(self, account_id: str, policy_arn: str, version_id: str,
                            daemon_name=None):
        return RgwIamPolicies.set_default_policy_version(policy_arn, version_id)

    @EndpointDoc("List policy tags",
                 parameters={'account_id': (str, 'Account ID'),
                             'policy_arn': (str, 'Policy ARN')})
    @RESTController.Collection(method='GET', path='/tags')
    def list_tags(self, account_id: str, policy_arn: str, daemon_name=None):
        return RgwIamPolicies.list_policy_tags(policy_arn)

    @EndpointDoc("Tag policy",
                 parameters={'account_id': (str, 'Account ID'),
                             'policy_arn': (str, 'Policy ARN'),
                             'tags': (list, 'Policy tags')})
    @RESTController.Collection(method='POST', path='/tags')
    @allow_empty_body
    def add_tags(self, account_id: str, policy_arn: str,
                 tags: List[Dict[str, str]], daemon_name=None):
        return RgwIamPolicies.tag_policy(policy_arn, tags)

    @EndpointDoc("Untag policy",
                 parameters={'account_id': (str, 'Account ID'),
                             'policy_arn': (str, 'Policy ARN'),
                             'tag_keys': (str, 'Comma-separated tag keys')})
    @RESTController.Collection(method='DELETE', path='/tags')
    def remove_tags(self, account_id: str, policy_arn: str, tag_keys: str,
                    daemon_name=None):
        keys = [key.strip() for key in tag_keys.split(',') if key.strip()]
        return RgwIamPolicies.untag_policy(policy_arn, keys)
