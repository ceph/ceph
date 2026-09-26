import json
from subprocess import SubprocessError
from typing import Any, Dict, List, Optional

from .. import mgr
from ..exceptions import DashboardException


class RgwAccounts:
    @classmethod
    def send_rgw_cmd(cls, command: List[str], error_msg: Optional[str] = None,
                     stdout_as_json: bool = True):
        try:
            exit_code, out, err = mgr.send_rgwadmin_command(
                command, stdout_as_json=stdout_as_json)

            if exit_code != 0:
                msg = (error_msg.format(code=exit_code, err=err)
                       if error_msg else err)
                raise DashboardException(msg=msg,
                                         http_status_code=500,
                                         component='rgw')
            return out

        except SubprocessError as e:
            raise DashboardException(e, component='rgw')

    @classmethod
    def get_accounts(cls):
        get_accounts_cmd = ['account', 'list']
        return cls.send_rgw_cmd(get_accounts_cmd)

    @classmethod
    def get_account_by_name(cls, account_name: str):
        """
        Get account info by account name
        Returns account info if found, raises exception otherwise
        """
        get_account_cmd = ['account', 'info', '--account-name', account_name]
        return cls.send_rgw_cmd(get_account_cmd)

    @classmethod
    def set_quota(cls, quota_type: str, account_id: str, max_size: str, max_objects: str,
                  enabled: bool):
        set_quota_cmd = ['quota', 'set', '--quota-scope', quota_type, '--account-id', account_id,
                         '--max-size', max_size, '--max-objects', max_objects]
        if enabled:
            cls.set_quota_status(quota_type, account_id, 'enable')
        else:
            cls.set_quota_status(quota_type, account_id, 'disable')
        return cls.send_rgw_cmd(set_quota_cmd)

    @classmethod
    def set_quota_status(cls, quota_type: str, account_id: str, quota_status: str):
        set_quota_status_cmd = ['quota', quota_status, '--quota-scope', quota_type,
                                '--account-id', account_id]

        return cls.send_rgw_cmd(set_quota_status_cmd)

    @classmethod
    def attach_managed_policy(cls, userId, policy_arn):
        radosgw_attach_managed_policies = ['user', 'policy', 'attach',
                                           '--uid', userId, '--policy-arn', policy_arn]
        try:
            exit_code, _, err = mgr.send_rgwadmin_command(radosgw_attach_managed_policies,
                                                          stdout_as_json=False)
            if exit_code > 0:
                raise DashboardException(e=err, msg='Unable to attach managed policies',
                                         http_status_code=500, component='rgw')
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')

    @classmethod
    def detach_managed_policy(cls, userId, policy_arn):
        radosgw_detach_managed_policy = ['user', 'policy', 'detach',
                                         '--uid', userId, '--policy-arn', policy_arn]
        try:
            exit_code, _, err = mgr.send_rgwadmin_command(radosgw_detach_managed_policy,
                                                          stdout_as_json=False)
            if exit_code > 0:
                raise DashboardException(e=err, msg='Unable to detach managed policies',
                                         http_status_code=500, component='rgw')

        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')

    @classmethod
    def list_managed_policy(cls, userId):
        radosgw_list_managed_policies = ['user', 'policy', 'list', 'attached',
                                         '--uid', userId]
        try:
            exit_code, out, err = mgr.send_rgwadmin_command(radosgw_list_managed_policies)
            if exit_code > 0:
                raise DashboardException(e=err, msg='Unable to get managed policies',
                                         http_status_code=500, component='rgw')
            return out
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')

    @classmethod
    def get_account_user_count(cls, account_id: str) -> int:
        """Return the number of users currently in the given account."""
        out = cls.send_rgw_cmd(['user', 'list', '--account-id', account_id])
        return len(out) if isinstance(out, list) else 0

    @classmethod
    def _append_account_flag(cls, cmd: list, account_id: Optional[str]):
        if account_id:
            cmd.extend(['--account-id', account_id])

    @classmethod
    def list_role_policies(cls, role_name: str,
                           account_id: Optional[str] = None) -> List[str]:
        cmd = ['role', 'policy', 'list', '--role-name', role_name]
        cls._append_account_flag(cmd, account_id)
        res = cls.send_rgw_cmd(
            cmd,
            error_msg='Error listing role policies with code {code}: {err}')
        if isinstance(res, list):
            return res
        if isinstance(res, dict) and 'PolicyNames' in res:
            return res['PolicyNames']
        return []

    @classmethod
    def get_role_policy(cls, role_name: str, policy_name: str,
                        account_id: Optional[str] = None) -> Dict[str, Any]:
        cmd = ['role', 'policy', 'get', '--role-name', role_name,
               '--policy-name', policy_name]
        cls._append_account_flag(cmd, account_id)
        return cls.send_rgw_cmd(
            cmd,
            error_msg='Error getting role policy with code {code}: {err}')

    @classmethod
    def put_role_policy(cls, role_name: str, policy_name: str, policy_doc: str,
                        account_id: Optional[str] = None) -> None:
        try:
            json.loads(policy_doc)
        except json.JSONDecodeError:
            raise DashboardException('Policy document is not a valid json')

        cmd = ['role', 'policy', 'put', '--role-name', role_name,
               '--policy-name', policy_name, '--policy-doc', f'{policy_doc}']
        cls._append_account_flag(cmd, account_id)
        cls.send_rgw_cmd(
            cmd,
            error_msg='Error putting role policy with code {code}: {err}',
            stdout_as_json=False)

    @classmethod
    def delete_role_policy(cls, role_name: str, policy_name: str,
                           account_id: Optional[str] = None) -> None:
        cmd = ['role', 'policy', 'delete', '--role-name', role_name,
               '--policy-name', policy_name]
        cls._append_account_flag(cmd, account_id)
        cls.send_rgw_cmd(
            cmd,
            error_msg='Error deleting role policy with code {code}: {err}',
            stdout_as_json=False)
