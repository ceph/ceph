from typing import Optional

from ..security import Scope
from ..services.rgw_iam import RgwAccounts
from ..tools import str_to_bool
from . import APIDoc, APIRouter, EndpointDoc, RESTController, allow_empty_body


@APIRouter('rgw/accounts', Scope.RGW)
@APIDoc("RGW User Accounts API", "RgwUserAccounts")
class RgwUserAccountsController(RESTController):

    @allow_empty_body
    def create(self, account_name: Optional[str] = None,
               account_id: Optional[str] = None, email: Optional[str] = None):
        return RgwAccounts.create_account(account_name, account_id, email)

    def list(self, detailed: bool = False):
        detailed = str_to_bool(detailed)
        return RgwAccounts.get_accounts(detailed)

    @EndpointDoc("Get RGW Account by id",
                 parameters={'account_id': (str, 'Account id')})
    def get(self, account_id: str):
        return RgwAccounts.get_account(account_id)

    @EndpointDoc("Delete RGW Account",
                 parameters={'account_id': (str, 'Account id')})
    def delete(self, account_id):
        return RgwAccounts.delete_account(account_id)

    @EndpointDoc("Update RGW account info",
                 parameters={'account_id': (str, 'Account id')})
    @allow_empty_body
    def set(self, account_id: str, account_name: Optional[str] = None,
            email: Optional[str] = None):
        return RgwAccounts.modify_account(account_id, account_name, email)

    @EndpointDoc("Set RGW Account/Bucket quota",
                 parameters={'account_id': (str, 'Account id'),
                             'max_size': (str, 'Max size')})
    @RESTController.Resource(method='PUT', path='/quota')
    @allow_empty_body
    def set_quota(self, quota_type: str, account_id: str, max_size: str, max_objects: str):
        return RgwAccounts.set_quota(quota_type, account_id, max_size, max_objects)

    @EndpointDoc("Enable/Disable RGW Account/Bucket quota",
                 parameters={'account_id': (str, 'Account id')})
    @RESTController.Resource(method='PUT', path='/quota/status')
    @allow_empty_body
    def set_quota_status(self, quota_type: str, account_id: str, quota_status: str):
        return RgwAccounts.set_quota_status(quota_type, account_id, quota_status)
